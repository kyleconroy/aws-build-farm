package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// readChunkSize is the payload size for ByteStream Read responses.
const readChunkSize = 1024 * 1024

// --- ContentAddressableStorage ---

// FindMissingBlobs returns the subset of requested digests not present in the CAS.
func (s *Server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	digests := req.GetBlobDigests()
	missing := make([]*repb.Digest, len(digests))

	const concurrency = 32
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for i, d := range digests {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, d *repb.Digest) {
			defer wg.Done()
			defer func() { <-sem }()
			ok, err := s.store.Contains(ctx, d)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			if !ok {
				missing[i] = d
			}
		}(i, d)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, status.Errorf(codes.Internal, "find missing blobs: %v", firstErr)
	}

	resp := &repb.FindMissingBlobsResponse{}
	for _, d := range missing {
		if d != nil {
			resp.MissingBlobDigests = append(resp.MissingBlobDigests, d)
		}
	}
	return resp, nil
}

// BatchUpdateBlobs stores a batch of small blobs.
func (s *Server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	resp := &repb.BatchUpdateBlobsResponse{}
	for _, r := range req.GetRequests() {
		st := s.putOne(ctx, r.GetDigest(), r.GetData())
		resp.Responses = append(resp.Responses, &repb.BatchUpdateBlobsResponse_Response{
			Digest: r.GetDigest(),
			Status: st,
		})
	}
	return resp, nil
}

func (s *Server) putOne(ctx context.Context, d *repb.Digest, data []byte) *rpcstatus.Status {
	if err := digest.Validate(d); err != nil {
		return statusProto(codes.InvalidArgument, err.Error())
	}
	if int64(len(data)) != d.GetSizeBytes() {
		return statusProto(codes.InvalidArgument, fmt.Sprintf("size mismatch: digest says %d, got %d bytes", d.GetSizeBytes(), len(data)))
	}
	if got := digest.FromBytes(data); got.Hash != d.GetHash() {
		return statusProto(codes.InvalidArgument, fmt.Sprintf("hash mismatch: digest says %s, computed %s", d.GetHash(), got.Hash))
	}
	if err := s.store.Put(ctx, d, data); err != nil {
		return statusProto(codes.Internal, err.Error())
	}
	return statusProto(codes.OK, "")
}

// BatchReadBlobs returns a batch of small blobs.
func (s *Server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	resp := &repb.BatchReadBlobsResponse{}
	for _, d := range req.GetDigests() {
		r := &repb.BatchReadBlobsResponse_Response{Digest: d}
		data, err := s.store.Get(ctx, d)
		switch {
		case errors.Is(err, cas.ErrNotFound):
			r.Status = statusProto(codes.NotFound, "blob not found")
		case err != nil:
			r.Status = statusProto(codes.Internal, err.Error())
		default:
			r.Data = data
			r.Status = statusProto(codes.OK, "")
		}
		resp.Responses = append(resp.Responses, r)
	}
	return resp, nil
}

// GetTree streams the directory tree rooted at the requested digest.
func (s *Server) GetTree(req *repb.GetTreeRequest, stream grpc.ServerStreamingServer[repb.GetTreeResponse]) error {
	ctx := stream.Context()
	queue := []*repb.Digest{req.GetRootDigest()}
	const pageSize = 100
	page := &repb.GetTreeResponse{}

	for len(queue) > 0 {
		d := queue[0]
		queue = queue[1:]

		data, err := s.store.Get(ctx, d)
		if errors.Is(err, cas.ErrNotFound) {
			return status.Errorf(codes.NotFound, "directory %s not found", digest.Key(d))
		} else if err != nil {
			return status.Errorf(codes.Internal, "read directory %s: %v", digest.Key(d), err)
		}
		dir := &repb.Directory{}
		if err := proto.Unmarshal(data, dir); err != nil {
			return status.Errorf(codes.Internal, "unmarshal directory %s: %v", digest.Key(d), err)
		}
		page.Directories = append(page.Directories, dir)
		for _, sub := range dir.GetDirectories() {
			queue = append(queue, sub.GetDigest())
		}
		if len(page.Directories) >= pageSize {
			if err := stream.Send(page); err != nil {
				return err
			}
			page = &repb.GetTreeResponse{}
		}
	}
	if len(page.Directories) > 0 {
		if err := stream.Send(page); err != nil {
			return err
		}
	}
	return nil
}

// --- ByteStream ---

// Read streams a blob to the client.
func (s *Server) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	d, err := parseReadResource(req.GetResourceName())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	rc, err := s.store.Reader(stream.Context(), d, req.GetReadOffset(), req.GetReadLimit())
	if errors.Is(err, cas.ErrNotFound) {
		return status.Errorf(codes.NotFound, "blob %s not found", digest.Key(d))
	} else if err != nil {
		return status.Errorf(codes.Internal, "read blob %s: %v", digest.Key(d), err)
	}
	defer rc.Close()

	buf := make([]byte, readChunkSize)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if serr := stream.Send(&bspb.ReadResponse{Data: buf[:n]}); serr != nil {
				return serr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read blob %s: %v", digest.Key(d), err)
		}
	}
}

// Write receives a blob from the client and stores it in the CAS. The blob is
// streamed to a temporary file first so arbitrarily large uploads do not need
// to fit in memory.
func (s *Server) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	d, err := parseWriteResource(first.GetResourceName())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	tmp, err := os.CreateTemp("", "buildfarm-upload-*")
	if err != nil {
		return status.Errorf(codes.Internal, "create temp: %v", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	var written int64
	req := first
	for {
		if int64(len(req.GetData())) > 0 {
			if req.GetWriteOffset() != written {
				return status.Errorf(codes.InvalidArgument, "non-contiguous write: offset %d, expected %d", req.GetWriteOffset(), written)
			}
			n, werr := tmp.Write(req.GetData())
			if werr != nil {
				return status.Errorf(codes.Internal, "buffer upload: %v", werr)
			}
			written += int64(n)
		}
		if req.GetFinishWrite() {
			break
		}
		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if written != d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "wrote %d bytes, digest declares %d", written, d.GetSizeBytes())
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return status.Errorf(codes.Internal, "rewind upload: %v", err)
	}
	if err := s.store.PutStream(ctx, d, written, tmp); err != nil {
		return status.Errorf(codes.Internal, "store blob %s: %v", digest.Key(d), err)
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: written})
}

// QueryWriteStatus reports whether a blob upload has completed.
func (s *Server) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	d, err := parseWriteResource(req.GetResourceName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	ok, err := s.store.Contains(ctx, d)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if ok {
		return &bspb.QueryWriteStatusResponse{CommittedSize: d.GetSizeBytes(), Complete: true}, nil
	}
	return &bspb.QueryWriteStatusResponse{CommittedSize: 0, Complete: false}, nil
}

// parseReadResource parses "{instance}/blobs/{hash}/{size}".
func parseReadResource(name string) (*repb.Digest, error) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		if p == "compressed-blobs" {
			return nil, fmt.Errorf("compressed blobs are not supported")
		}
		if p == "blobs" && i+2 < len(parts) {
			return digestFrom(parts[i+1], parts[i+2])
		}
	}
	return nil, fmt.Errorf("invalid read resource name %q", name)
}

// parseWriteResource parses "{instance}/uploads/{uuid}/blobs/{hash}/{size}/...".
func parseWriteResource(name string) (*repb.Digest, error) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		if p == "uploads" && i+4 < len(parts) && parts[i+2] == "blobs" {
			return digestFrom(parts[i+3], parts[i+4])
		}
	}
	return nil, fmt.Errorf("invalid write resource name %q", name)
}

func digestFrom(hash, size string) (*repb.Digest, error) {
	n, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid size %q: %w", size, err)
	}
	d := &repb.Digest{Hash: hash, SizeBytes: n}
	if err := digest.Validate(d); err != nil {
		return nil, err
	}
	return d, nil
}

func statusProto(code codes.Code, msg string) *rpcstatus.Status {
	return &rpcstatus.Status{Code: int32(code), Message: msg}
}
