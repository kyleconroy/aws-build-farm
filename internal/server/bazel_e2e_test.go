//go:build bazele2e

// Full end-to-end validation with a real Bazel 9 client.
//
// Excluded from normal `go test` by the `bazele2e` build tag. It requires:
//   - working AWS credentials in the environment (standard SDK chain), with
//     permission to create/delete an S3 bucket;
//   - `bazel` (or `bazelisk`) on PATH — the repo pins Bazel 9 via .bazelversion.
//
// Run from the repo root:
//
//	go test -tags bazele2e ./internal/server/ -run TestBazelRemoteCache -v -timeout 30m
//
// What it does, against the project's own real source code:
//  1. creates a throwaway S3 bucket and starts the REAPI gRPC server backed by
//     it (CAS + Action Cache);
//  2. runs `bazel build` of this repo pointed at the server as `--remote_cache`,
//     populating the cache in S3;
//  3. runs `bazel clean`, then rebuilds — and asserts the actions are served as
//     remote cache hits, i.e. the compiled outputs come back out of S3.
//
// The bucket is deleted at the end.
package server_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/server"
)

// buildTargets is intentionally limited to this project's own packages; their
// external dependencies are pulled in (and cached) transitively.
var buildTargets = []string{"//cmd/...", "//internal/..."}

var remoteHitRE = regexp.MustCompile(`(\d+) remote cache hit`)

func TestBazelRemoteCache(t *testing.T) {
	bazelBin, err := exec.LookPath("bazel")
	if err != nil {
		if bazelBin, err = exec.LookPath("bazelisk"); err != nil {
			t.Skip("bazel/bazelisk not on PATH; skipping Bazel end-to-end test")
		}
	}
	repoRoot := findRepoRoot(t)

	// A dedicated output base keeps this run hermetic: it shares the global
	// repository cache (so external deps are not re-downloaded) but has its own
	// action cache and output tree, isolated from any developer's Bazel server.
	// The `bazel clean` between the two builds therefore guarantees the rebuild
	// can only succeed by hitting the remote cache.
	outputBase := t.TempDir()
	startup := []string{"--output_base=" + outputBase}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		exec.Command(bazelBin, "--output_base="+outputBase, "shutdown").Run()
	})

	cfg, reg := liveAWSConfig(t, ctx)
	s3Client := s3.NewFromConfig(cfg)
	bucket := testBucket(t, ctx, s3Client, cfg, reg)
	t.Logf("using cache bucket s3://%s (region=%s)", bucket, reg)

	store := cas.NewS3Store(s3Client, bucket, "bazel-cache")
	addr := startGRPCServer(t, store)
	t.Logf("REAPI server listening on %s, backed by s3://%s", addr, bucket)

	remoteCache := "grpc://" + addr
	bz := func(args ...string) string {
		return runBazel(t, ctx, bazelBin, repoRoot, append(append([]string{}, startup...), args...)...)
	}
	buildArgs := append([]string{"build", "--remote_cache=" + remoteCache, "--remote_timeout=120"}, buildTargets...)

	// 1. Populate the remote cache.
	t.Log("bazel build #1 (populating remote cache)...")
	out := bz(buildArgs...)
	if !regexp.MustCompile(`Build completed successfully`).MatchString(out) {
		t.Fatalf("build #1 did not complete successfully:\n%s", tail(out))
	}

	// 2. Drop all local outputs so the rebuild cannot reuse them.
	t.Log("bazel clean...")
	bz("clean")

	// 3. Cold rebuild: every reusable action must now come from the remote cache.
	t.Log("bazel build #2 (cold rebuild, expecting remote cache hits)...")
	out = bz(buildArgs...)
	if !regexp.MustCompile(`Build completed successfully`).MatchString(out) {
		t.Fatalf("build #2 did not complete successfully:\n%s", tail(out))
	}

	m := remoteHitRE.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("rebuild reported no remote cache hits; remote cache not exercised:\n%s", tail(out))
	}
	hits, _ := strconv.Atoi(m[1])
	if hits == 0 {
		t.Fatalf("rebuild reported 0 remote cache hits")
	}
	t.Logf("END-TO-END OK: Bazel %s built this repo and served %d actions from the S3-backed REAPI cache", bazelVersion(repoRoot), hits)
}

// startGRPCServer stands up the REAPI server (CAS + Action Cache only) on a
// loopback TCP port and returns its address.
func startGRPCServer(t *testing.T, store cas.Store) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	server.New(store, nil, "").Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

func runBazel(t *testing.T, ctx context.Context, bazelBin, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, bazelBin, args...)
	cmd.Dir = dir
	b, err := cmd.CombinedOutput()
	out := string(b)
	if err != nil {
		t.Fatalf("bazel %v failed: %v\n%s", args, err, tail(out))
	}
	return out
}

func bazelVersion(repoRoot string) string {
	if b, err := os.ReadFile(filepath.Join(repoRoot, ".bazelversion")); err == nil {
		return string(regexp.MustCompile(`\s+`).ReplaceAll(b, nil))
	}
	return "9"
}

// findRepoRoot walks up from the working directory to the directory containing
// MODULE.bazel.
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "MODULE.bazel")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find MODULE.bazel above %s", dir)
		}
		dir = parent
	}
}

// tail returns the last ~2 KiB of s for error messages.
func tail(s string) string {
	const n = 2048
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
