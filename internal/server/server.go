// Package server implements the REAPI v2 gRPC services (Capabilities, Content
// Addressable Storage, ByteStream, Action Cache, Execution) plus the
// google.longrunning.Operations service, backed by an S3 CAS and a Lambda
// MicroVM executor.
package server

import (
	"context"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	lrpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
)

// maxBatchSize bounds BatchUpdateBlobs/BatchReadBlobs payloads (4 MiB), the
// conventional REAPI gRPC message limit.
const maxBatchSize = 4 * 1024 * 1024

// Runner executes a single action's command and returns its ActionResult. It is
// implemented by *microvm.Executor; tests provide a fake.
type Runner interface {
	Run(ctx context.Context, command *repb.Command, inputRoot *repb.Digest, timeout time.Duration) (*repb.ActionResult, error)
}

// Server implements every REAPI v2 service. Execution is optional: when exec is
// nil the server still serves the CAS and Action Cache, and advertises
// exec_enabled=false.
type Server struct {
	repb.UnimplementedCapabilitiesServer
	repb.UnimplementedContentAddressableStorageServer
	repb.UnimplementedActionCacheServer
	repb.UnimplementedExecutionServer
	bspb.UnimplementedByteStreamServer
	lrpb.UnimplementedOperationsServer

	store        cas.Store
	exec         Runner
	ops          *OperationManager
	instanceName string
}

// New returns a Server. exec may be nil to disable remote execution.
func New(store cas.Store, exec Runner, instanceName string) *Server {
	return &Server{
		store:        store,
		exec:         exec,
		ops:          NewOperationManager(),
		instanceName: instanceName,
	}
}

// Register wires every service onto the gRPC server.
func (s *Server) Register(g *grpc.Server) {
	repb.RegisterCapabilitiesServer(g, s)
	repb.RegisterContentAddressableStorageServer(g, s)
	repb.RegisterActionCacheServer(g, s)
	repb.RegisterExecutionServer(g, s)
	bspb.RegisterByteStreamServer(g, s)
	lrpb.RegisterOperationsServer(g, s)
}

// ExecutionEnabled reports whether remote execution is configured.
func (s *Server) ExecutionEnabled() bool { return s.exec != nil }

// GetCapabilities advertises the server's supported features.
func (s *Server) GetCapabilities(_ context.Context, _ *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	caps := &repb.ServerCapabilities{
		CacheCapabilities: &repb.CacheCapabilities{
			DigestFunctions: []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes:      maxBatchSize,
			SymlinkAbsolutePathStrategy: repb.SymlinkAbsolutePathStrategy_ALLOWED,
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 3},
	}
	if s.exec != nil {
		caps.ExecutionCapabilities = &repb.ExecutionCapabilities{
			DigestFunction:  repb.DigestFunction_SHA256,
			ExecEnabled:     true,
			DigestFunctions: []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
		}
	}
	return caps, nil
}
