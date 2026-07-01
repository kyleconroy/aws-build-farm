// Command cas-bake walks a REAPI input tree in the S3-backed CAS and writes
// every blob it references (Directory protos and file contents alike) into a
// flat, content-addressed directory: one file per blob, named by its
// lowercase-hex SHA-256. That directory is baked into the MicroVM image and
// consulted by the executor-agent's read-through cache, so large stable inputs
// like the Go SDK are served locally instead of re-fetched from S3 per action.
//
//	cas-bake -bucket B -region R -prefix P -input-root <hash>/<size> -out DIR
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/awsenv"
	"github.com/kyleconroy/aws-build-farm/internal/cas"
)

const concurrency = 32

func main() {
	bucket := flag.String("bucket", "", "S3 bucket holding the CAS")
	region := flag.String("region", "us-east-1", "AWS region")
	prefix := flag.String("prefix", "remoteexec-go", "CAS key prefix")
	inputRoot := flag.String("input-root", "", "input root digest as <hash>/<size>")
	out := flag.String("out", "", "output directory for baked blobs")
	flag.Parse()
	if *bucket == "" || *inputRoot == "" || *out == "" {
		log.Fatal("usage: cas-bake -bucket B -input-root <hash>/<size> -out DIR [-region R] [-prefix P]")
	}

	root, err := parseDigest(*inputRoot)
	if err != nil {
		log.Fatalf("parse input root: %v", err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}

	awsenv.Sanitize()
	ctx := context.Background()
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxConnsPerHost = 128
		tr.MaxIdleConns = 128
		tr.MaxIdleConnsPerHost = 128
	})
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*region), awsconfig.WithHTTPClient(httpClient))
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	store := cas.NewS3Store(s3.NewFromConfig(cfg), *bucket, *prefix)

	b := &baker{store: store, out: *out, sem: make(chan struct{}, concurrency), seen: map[string]bool{}}
	b.walkDir(ctx, root)
	b.wg.Wait()
	if b.err != nil {
		log.Fatalf("bake failed: %v", b.err)
	}
	log.Printf("baked %d blobs (%d bytes) into %s", b.count, b.bytes, *out)
}

type baker struct {
	store *cas.S3Store
	out   string
	sem   chan struct{}
	wg    sync.WaitGroup

	mu    sync.Mutex
	seen  map[string]bool
	err   error
	count int64
	bytes int64
}

// claim records a digest as being handled and reports whether the caller is the
// first to do so (so each blob is fetched and written exactly once).
func (b *baker) claim(hash string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.seen[hash] {
		return false
	}
	b.seen[hash] = true
	return true
}

func (b *baker) fail(err error) {
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.mu.Unlock()
}

// write fetches d and writes it to out/<hash>, returning the raw bytes so a
// Directory blob can also be traversed. Returns nil if already written/failed.
func (b *baker) write(ctx context.Context, d *repb.Digest) []byte {
	if b.err != nil || !b.claim(d.GetHash()) {
		return nil
	}
	b.sem <- struct{}{}
	defer func() { <-b.sem }()

	data, err := b.store.Get(ctx, d)
	if err != nil {
		b.fail(fmt.Errorf("get %s: %w", d.GetHash(), err))
		return nil
	}
	if err := os.WriteFile(filepath.Join(b.out, d.GetHash()), data, 0o644); err != nil {
		b.fail(err)
		return nil
	}
	b.mu.Lock()
	b.count++
	b.bytes += int64(len(data))
	b.mu.Unlock()
	return data
}

// walkDir schedules the directory blob d and recurses into its entries.
func (b *baker) walkDir(ctx context.Context, d *repb.Digest) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		data := b.write(ctx, d)
		if data == nil {
			return
		}
		dir := &repb.Directory{}
		if err := proto.Unmarshal(data, dir); err != nil {
			b.fail(fmt.Errorf("unmarshal directory %s: %w", d.GetHash(), err))
			return
		}
		for _, f := range dir.GetFiles() {
			f := f
			b.wg.Add(1)
			go func() { defer b.wg.Done(); b.write(ctx, f.GetDigest()) }()
		}
		for _, sub := range dir.GetDirectories() {
			b.walkDir(ctx, sub.GetDigest())
		}
	}()
}

func parseDigest(s string) (*repb.Digest, error) {
	h, sz, ok := strings.Cut(s, "/")
	if !ok {
		return nil, fmt.Errorf("want <hash>/<size>, got %q", s)
	}
	n, err := strconv.ParseInt(sz, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad size %q: %w", sz, err)
	}
	return &repb.Digest{Hash: h, SizeBytes: n}, nil
}
