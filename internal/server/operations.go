package server

import (
	"context"
	"sync"

	lrpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// opEntry tracks a single long-running operation and lets multiple gRPC streams
// (Execute and WaitExecution) observe its progress. Waiters block on the
// current "changed" channel, which is closed and replaced on every update, so
// every waiter is guaranteed to observe the terminal state.
type opEntry struct {
	mu      sync.Mutex
	latest  *lrpb.Operation
	done    bool
	cancel  context.CancelFunc
	changed chan struct{}
}

func (e *opEntry) snapshot() (*lrpb.Operation, bool, <-chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.latest, e.done, e.changed
}

func (e *opEntry) publish(op *lrpb.Operation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return
	}
	e.latest = op
	e.done = op.GetDone()
	close(e.changed)
	e.changed = make(chan struct{})
}

// OperationManager stores in-flight and completed operations.
type OperationManager struct {
	mu  sync.Mutex
	ops map[string]*opEntry
}

// NewOperationManager returns an empty manager.
func NewOperationManager() *OperationManager {
	return &OperationManager{ops: map[string]*opEntry{}}
}

// New registers a fresh operation and returns its name and entry. The cancel
// function is invoked if the operation is cancelled via the Operations API.
func (m *OperationManager) New(cancel context.CancelFunc) (string, *opEntry) {
	name := uuid.NewString()
	e := &opEntry{cancel: cancel, changed: make(chan struct{})}
	m.mu.Lock()
	m.ops[name] = e
	m.mu.Unlock()
	return name, e
}

func (m *OperationManager) lookup(name string) (*opEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.ops[name]
	return e, ok
}

// stream sends operation updates to a gRPC server stream until the operation is
// done or the client disconnects.
func (m *OperationManager) stream(stream grpc.ServerStreamingServer[lrpb.Operation], e *opEntry) error {
	var lastSent *lrpb.Operation
	for {
		op, done, changed := e.snapshot()
		if op != nil && op != lastSent {
			if err := stream.Send(op); err != nil {
				return err
			}
			lastSent = op
			if done {
				return nil
			}
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-changed:
		}
	}
}

// --- google.longrunning.Operations service ---

func (s *Server) GetOperation(_ context.Context, req *lrpb.GetOperationRequest) (*lrpb.Operation, error) {
	e, ok := s.ops.lookup(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.GetName())
	}
	op, _, _ := e.snapshot()
	if op == nil {
		return nil, status.Errorf(codes.NotFound, "operation %q has no state", req.GetName())
	}
	return op, nil
}

func (s *Server) ListOperations(_ context.Context, _ *lrpb.ListOperationsRequest) (*lrpb.ListOperationsResponse, error) {
	s.ops.mu.Lock()
	defer s.ops.mu.Unlock()
	resp := &lrpb.ListOperationsResponse{}
	for _, e := range s.ops.ops {
		if op, _, _ := e.snapshot(); op != nil {
			resp.Operations = append(resp.Operations, op)
		}
	}
	return resp, nil
}

func (s *Server) DeleteOperation(_ context.Context, req *lrpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	s.ops.mu.Lock()
	defer s.ops.mu.Unlock()
	if _, ok := s.ops.ops[req.GetName()]; !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.GetName())
	}
	delete(s.ops.ops, req.GetName())
	return &emptypb.Empty{}, nil
}

func (s *Server) CancelOperation(_ context.Context, req *lrpb.CancelOperationRequest) (*emptypb.Empty, error) {
	e, ok := s.ops.lookup(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.GetName())
	}
	if e.cancel != nil {
		e.cancel()
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) WaitOperation(ctx context.Context, req *lrpb.WaitOperationRequest) (*lrpb.Operation, error) {
	e, ok := s.ops.lookup(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.GetName())
	}
	for {
		op, done, changed := e.snapshot()
		if done {
			return op, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
