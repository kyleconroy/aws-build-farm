package casfs

import (
	"context"
	"os"
	"path/filepath"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// DiskCache is a read-through blob cache backed by a local directory of
// content-addressed files (one file per blob, named by its lowercase-hex
// SHA-256 hash). It serves the executor-agent's input materialization: large,
// stable inputs such as the Go SDK can be baked into the MicroVM image so they
// never have to be fetched from the remote CAS.
//
// Because blobs are content-addressed a name match is a content match; as a
// guard against a truncated or corrupt baked file the cached blob's size is
// checked against the requested digest, and any mismatch, missing file, or read
// error falls back to the wrapped Getter. A missing cache directory therefore
// just means every read is a miss.
type DiskCache struct {
	dir   string
	inner Getter
}

// NewDiskCache returns a Getter that serves blobs from dir when present and
// otherwise delegates to inner.
func NewDiskCache(dir string, inner Getter) *DiskCache {
	return &DiskCache{dir: dir, inner: inner}
}

// Get returns the blob for d, preferring the local cache.
func (c *DiskCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	if data, err := os.ReadFile(filepath.Join(c.dir, d.GetHash())); err == nil && int64(len(data)) == d.GetSizeBytes() {
		return data, nil
	}
	return c.inner.Get(ctx, d)
}
