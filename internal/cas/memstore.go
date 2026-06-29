package cas

import (
	"bytes"
	"context"
	"io"
	"sync"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// MemStore is an in-memory Store implementation. It is primarily intended for
// tests and local experimentation; production deployments use S3Store.
type MemStore struct {
	mu     sync.RWMutex
	blobs  map[string][]byte
	action map[string][]byte
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		blobs:  map[string][]byte{},
		action: map[string][]byte{},
	}
}

func (m *MemStore) Contains(_ context.Context, d *repb.Digest) (bool, error) {
	if digest.IsEmpty(d) {
		return true, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.blobs[d.GetHash()]
	return ok, nil
}

func (m *MemStore) Get(_ context.Context, d *repb.Digest) ([]byte, error) {
	if digest.IsEmpty(d) {
		return []byte{}, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.blobs[d.GetHash()]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (m *MemStore) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	b, err := m.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	if offset > int64(len(b)) {
		offset = int64(len(b))
	}
	b = b[offset:]
	if limit > 0 && limit < int64(len(b)) {
		b = b[:limit]
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *MemStore) Put(_ context.Context, d *repb.Digest, data []byte) error {
	if digest.IsEmpty(d) {
		return nil
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[d.GetHash()] = stored
	return nil
}

func (m *MemStore) PutStream(ctx context.Context, d *repb.Digest, _ int64, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return m.Put(ctx, d, data)
}

func (m *MemStore) GetActionResult(_ context.Context, actionDigest *repb.Digest) (*repb.ActionResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.action[actionDigest.GetHash()]
	if !ok {
		return nil, ErrNotFound
	}
	ar := &repb.ActionResult{}
	if err := proto.Unmarshal(data, ar); err != nil {
		return nil, err
	}
	return ar, nil
}

func (m *MemStore) PutActionResult(_ context.Context, actionDigest *repb.Digest, ar *repb.ActionResult) error {
	data, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.action[actionDigest.GetHash()] = data
	return nil
}
