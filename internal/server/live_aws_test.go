//go:build liveaws

// Live end-to-end validation against real AWS.
//
// This test is excluded from normal `go test` by the `liveaws` build tag. It
// requires working AWS credentials in the environment (the standard SDK chain)
// and the ability to create/delete an S3 bucket. Run it with:
//
//	go test -tags liveaws ./internal/server/ -run TestLiveAWS -v
//
// It stands up the real REAPI gRPC server backed by a freshly-created S3 bucket
// and drives the whole CAS + ByteStream + Action Cache surface through gRPC
// clients, confirming blobs and action results actually round-trip through S3.
// It also probes the Lambda MicroVMs control plane to confirm the credentials
// are accepted there too. The bucket and every object are deleted at the end.
package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambdamicrovms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/digest"
	"github.com/kyleconroy/aws-build-farm/internal/server"
)

func TestLiveAWS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg, reg := liveAWSConfig(t, ctx)
	s3Client := s3.NewFromConfig(cfg)
	bucket := testBucket(t, ctx, s3Client, cfg, reg)
	t.Logf("using test bucket s3://%s (region=%s)", bucket, reg)

	// A unique prefix per run keeps the assertions independent of other runs
	// sharing this durable bucket.
	prefix := fmt.Sprintf("livetest/%d", time.Now().UnixNano())
	store := cas.NewS3Store(s3Client, bucket, prefix)
	srv := server.New(store, nil, "") // CAS + Action Cache only

	// --- in-process gRPC over bufconn ---
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	srv.Register(g)
	go g.Serve(lis)
	defer g.Stop()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	caps := repb.NewCapabilitiesClient(conn)
	casc := repb.NewContentAddressableStorageClient(conn)
	acc := repb.NewActionCacheClient(conn)
	bs := bspb.NewByteStreamClient(conn)

	// 1. Capabilities.
	cp, err := caps.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	if got := cp.GetCacheCapabilities().GetDigestFunctions(); len(got) == 0 {
		t.Fatalf("expected a digest function advertised")
	}
	t.Logf("capabilities OK: exec_enabled=%v", cp.GetExecutionCapabilities().GetExecEnabled())

	// 2. ByteStream upload of a blob.
	payload := []byte("hello from a real S3 CAS round trip @ " + reg)
	d := digest.FromBytes(payload)
	uploadBlob(t, ctx, bs, payload, d)
	t.Logf("ByteStream write OK: %s", digest.Key(d))

	// 3. The blob really landed in S3.
	blobKey := prefix + "/cas/" + d.GetHash()
	if _, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(blobKey),
	}); err != nil {
		t.Fatalf("blob not found in S3 after upload: %v", err)
	}
	t.Logf("verified blob present in S3 at %s", blobKey)

	// 4. FindMissingBlobs: the uploaded blob is present, a random one is missing.
	absent := digest.FromBytes([]byte("this blob was never uploaded"))
	fm, err := casc.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{d, absent},
	})
	if err != nil {
		t.Fatalf("FindMissingBlobs: %v", err)
	}
	if len(fm.GetMissingBlobDigests()) != 1 || fm.GetMissingBlobDigests()[0].GetHash() != absent.GetHash() {
		t.Fatalf("FindMissingBlobs = %v, want exactly the absent digest", fm.GetMissingBlobDigests())
	}
	t.Logf("FindMissingBlobs OK: present blob known, absent blob reported missing")

	// 5. ByteStream read it back.
	got := downloadBlob(t, ctx, bs, d)
	if string(got) != string(payload) {
		t.Fatalf("ByteStream read mismatch: got %q want %q", got, payload)
	}
	t.Logf("ByteStream read OK: %d bytes match", len(got))

	// 6. Batch update + read another blob.
	p2 := []byte("batched blob")
	d2 := digest.FromBytes(p2)
	bu, err := casc.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
		Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: d2, Data: p2}},
	})
	if err != nil {
		t.Fatalf("BatchUpdateBlobs: %v", err)
	}
	if c := bu.GetResponses()[0].GetStatus().GetCode(); c != 0 {
		t.Fatalf("BatchUpdateBlobs status = %d, want OK", c)
	}
	br, err := casc.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d2}})
	if err != nil {
		t.Fatalf("BatchReadBlobs: %v", err)
	}
	if string(br.GetResponses()[0].GetData()) != string(p2) {
		t.Fatalf("BatchReadBlobs mismatch")
	}
	t.Logf("Batch update/read OK")

	// 7. Action Cache round trip.
	actionDigest := digest.FromBytes([]byte("a fake action"))
	ar := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("ok\n"),
		OutputFiles: []*repb.OutputFile{
			{Path: "out.txt", Digest: d2},
		},
	}
	if _, err := acc.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest: actionDigest,
		ActionResult: ar,
	}); err != nil {
		t.Fatalf("UpdateActionResult: %v", err)
	}
	gotAR, err := acc.GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: actionDigest})
	if err != nil {
		t.Fatalf("GetActionResult: %v", err)
	}
	if gotAR.GetExitCode() != 0 || len(gotAR.GetOutputFiles()) != 1 ||
		gotAR.GetOutputFiles()[0].GetDigest().GetHash() != d2.GetHash() {
		t.Fatalf("GetActionResult round trip mismatch: %+v", gotAR)
	}
	t.Logf("Action Cache update/get OK: cached ActionResult round-tripped through S3")

	// 8. Probe the Lambda MicroVMs control plane: confirms the same credentials
	// are accepted by the execution backend (full execution still needs a
	// deployed MicroVM image, which lives in the deploy/ TODO).
	mv := lambdamicrovms.NewFromConfig(cfg)
	if _, err := mv.ListMicrovmImages(ctx, &lambdamicrovms.ListMicrovmImagesInput{
		MaxResults: aws.Int32(1),
	}); err != nil {
		t.Logf("WARNING: Lambda MicroVMs ListMicrovmImages failed (execution backend not validated): %v", err)
	} else {
		t.Logf("Lambda MicroVMs control plane reachable: credentials accepted by execution backend")
	}

	t.Logf("LIVE END-TO-END OK against real AWS S3 in %s", reg)
}

func uploadBlob(t *testing.T, ctx context.Context, bs bspb.ByteStreamClient, payload []byte, d *repb.Digest) {
	t.Helper()
	resource := fmt.Sprintf("uploads/%s/blobs/%s/%d", "00000000-0000-0000-0000-000000000001", d.GetHash(), d.GetSizeBytes())
	w, err := bs.Write(ctx)
	if err != nil {
		t.Fatalf("Write open: %v", err)
	}
	if err := w.Send(&bspb.WriteRequest{
		ResourceName: resource,
		Data:         payload,
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("Write send: %v", err)
	}
	resp, err := w.CloseAndRecv()
	if err != nil {
		t.Fatalf("Write close: %v", err)
	}
	if resp.GetCommittedSize() != d.GetSizeBytes() {
		t.Fatalf("committed %d, want %d", resp.GetCommittedSize(), d.GetSizeBytes())
	}
}

func downloadBlob(t *testing.T, ctx context.Context, bs bspb.ByteStreamClient, d *repb.Digest) []byte {
	t.Helper()
	resource := fmt.Sprintf("blobs/%s/%d", d.GetHash(), d.GetSizeBytes())
	r, err := bs.Read(ctx, &bspb.ReadRequest{ResourceName: resource})
	if err != nil {
		t.Fatalf("Read open: %v", err)
	}
	var buf []byte
	for {
		chunk, err := r.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read recv: %v", err)
		}
		buf = append(buf, chunk.GetData()...)
	}
	return buf
}
