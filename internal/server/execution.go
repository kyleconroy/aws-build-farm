package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	lrpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// defaultActionTimeout bounds command execution when an Action does not set one.
const defaultActionTimeout = 15 * time.Minute

// Execute runs an action and streams Operation updates until it completes.
func (s *Server) Execute(req *repb.ExecuteRequest, stream grpc.ServerStreamingServer[lrpb.Operation]) error {
	if s.exec == nil {
		return status.Error(codes.Unimplemented, "remote execution is not enabled on this server")
	}
	actionDigest := req.GetActionDigest()
	if err := digest.Validate(actionDigest); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	action, err := s.fetchAction(stream.Context(), actionDigest)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	name, entry := s.ops.New(cancel)
	go func() {
		defer cancel()
		s.runAction(ctx, entry, name, req, action)
	}()
	return s.ops.stream(stream, entry)
}

// WaitExecution reconnects to an in-flight operation.
func (s *Server) WaitExecution(req *repb.WaitExecutionRequest, stream grpc.ServerStreamingServer[lrpb.Operation]) error {
	entry, ok := s.ops.lookup(req.GetName())
	if !ok {
		return status.Errorf(codes.NotFound, "operation %q not found", req.GetName())
	}
	return s.ops.stream(stream, entry)
}

func (s *Server) fetchAction(ctx context.Context, actionDigest *repb.Digest) (*repb.Action, error) {
	data, err := s.store.Get(ctx, actionDigest)
	if errors.Is(err, cas.ErrNotFound) {
		return nil, status.Errorf(codes.FailedPrecondition, "action %s not found in CAS", digest.Key(actionDigest))
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "fetch action: %v", err)
	}
	action := &repb.Action{}
	if err := proto.Unmarshal(data, action); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshal action: %v", err)
	}
	return action, nil
}

// runAction performs the cache check and, on a miss, the MicroVM execution,
// publishing Operation updates as it progresses.
func (s *Server) runAction(ctx context.Context, entry *opEntry, name string, req *repb.ExecuteRequest, action *repb.Action) {
	actionDigest := req.GetActionDigest()
	entry.publish(s.operation(name, actionDigest, repb.ExecutionStage_CACHE_CHECK, nil))

	// Cache check.
	if !req.GetSkipCacheLookup() && !action.GetDoNotCache() {
		if ar, err := s.store.GetActionResult(ctx, actionDigest); err == nil {
			entry.publish(s.completed(name, actionDigest, &repb.ExecuteResponse{
				Result:       ar,
				CachedResult: true,
				Status:       statusProto(codes.OK, ""),
			}))
			return
		}
	}

	// Fetch the Command.
	cmdData, err := s.store.Get(ctx, action.GetCommandDigest())
	if err != nil {
		entry.publish(s.failed(name, actionDigest, codes.FailedPrecondition,
			fmt.Sprintf("command %s not found: %v", digest.Key(action.GetCommandDigest()), err)))
		return
	}
	command := &repb.Command{}
	if err := proto.Unmarshal(cmdData, command); err != nil {
		entry.publish(s.failed(name, actionDigest, codes.Internal, fmt.Sprintf("unmarshal command: %v", err)))
		return
	}

	entry.publish(s.operation(name, actionDigest, repb.ExecutionStage_EXECUTING, nil))

	timeout := defaultActionTimeout
	if t := action.GetTimeout(); t != nil {
		timeout = t.AsDuration()
	}

	ar, err := s.exec.Run(ctx, command, action.GetInputRootDigest(), timeout)
	if err != nil {
		code := codes.Internal
		if errors.Is(err, context.Canceled) {
			code = codes.Canceled
		}
		entry.publish(s.failed(name, actionDigest, code, err.Error()))
		return
	}

	// Cache successful results unless the action opted out.
	if !action.GetDoNotCache() && ar.GetExitCode() == 0 {
		if perr := s.store.PutActionResult(ctx, actionDigest, ar); perr != nil {
			// Caching is best-effort; the result is still returned.
			ar.ExecutionMetadata = ar.GetExecutionMetadata()
		}
	}

	entry.publish(s.completed(name, actionDigest, &repb.ExecuteResponse{
		Result:       ar,
		CachedResult: false,
		Status:       statusProto(codes.OK, ""),
	}))
}

func (s *Server) operation(name string, actionDigest *repb.Digest, stage repb.ExecutionStage_Value, resp *repb.ExecuteResponse) *lrpb.Operation {
	meta, _ := anypb.New(&repb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: actionDigest,
	})
	op := &lrpb.Operation{Name: name, Metadata: meta}
	if resp != nil {
		respAny, _ := anypb.New(resp)
		op.Done = true
		op.Result = &lrpb.Operation_Response{Response: respAny}
	}
	return op
}

func (s *Server) completed(name string, actionDigest *repb.Digest, resp *repb.ExecuteResponse) *lrpb.Operation {
	return s.operation(name, actionDigest, repb.ExecutionStage_COMPLETED, resp)
}

func (s *Server) failed(name string, actionDigest *repb.Digest, code codes.Code, msg string) *lrpb.Operation {
	return s.completed(name, actionDigest, &repb.ExecuteResponse{
		Status: statusProto(code, msg),
	})
}
