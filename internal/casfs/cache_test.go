package casfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// countingGetter records how many times the wrapped CAS is consulted.
type countingGetter struct {
	data  map[string][]byte
	calls int
}

func (g *countingGetter) Get(_ context.Context, d *repb.Digest) ([]byte, error) {
	g.calls++
	b, ok := g.data[d.GetHash()]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

func TestDiskCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cached := []byte("baked blob contents")
	cd := digest.FromBytes(cached)
	if err := os.WriteFile(filepath.Join(dir, cd.GetHash()), cached, 0o644); err != nil {
		t.Fatal(err)
	}

	remoteOnly := []byte("only in the remote CAS")
	rd := digest.FromBytes(remoteOnly)

	inner := &countingGetter{data: map[string][]byte{
		cd.GetHash(): cached,
		rd.GetHash(): remoteOnly,
	}}
	c := NewDiskCache(dir, inner)

	// Hit: served from disk without touching the inner store.
	got, err := c.Get(ctx, cd)
	if err != nil || string(got) != string(cached) {
		t.Fatalf("cache hit: got %q, err %v", got, err)
	}
	if inner.calls != 0 {
		t.Fatalf("cache hit consulted inner %d times, want 0", inner.calls)
	}

	// Miss: not on disk, falls through to inner.
	got, err = c.Get(ctx, rd)
	if err != nil || string(got) != string(remoteOnly) {
		t.Fatalf("cache miss: got %q, err %v", got, err)
	}
	if inner.calls != 1 {
		t.Fatalf("cache miss consulted inner %d times, want 1", inner.calls)
	}

	// Size mismatch: a baked file whose size disagrees with the requested digest
	// is ignored (guards against truncation/corruption) and the inner store is
	// used instead.
	wrong := digest.FromBytes(cached)
	wrong.SizeBytes = int64(len(cached)) + 99
	inner.data[wrong.GetHash()] = cached
	if _, err := c.Get(ctx, wrong); err != nil {
		t.Fatalf("size mismatch fallback: %v", err)
	}
	if inner.calls != 2 {
		t.Fatalf("size mismatch consulted inner %d times, want 2", inner.calls)
	}
}
