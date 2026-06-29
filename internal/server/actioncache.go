package server

import (
	"context"
	"errors"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// GetActionResult returns a cached ActionResult for the given action digest.
func (s *Server) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	if err := digest.Validate(req.GetActionDigest()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	ar, err := s.store.GetActionResult(ctx, req.GetActionDigest())
	if errors.Is(err, cas.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "no cached result for %s", digest.Key(req.GetActionDigest()))
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return ar, nil
}

// UpdateActionResult stores an ActionResult for the given action digest.
func (s *Server) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	if err := digest.Validate(req.GetActionDigest()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetActionResult() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "action_result is required")
	}
	if err := s.store.PutActionResult(ctx, req.GetActionDigest(), req.GetActionResult()); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return req.GetActionResult(), nil
}
