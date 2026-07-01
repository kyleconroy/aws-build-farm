// Package casfs converts between REAPI directory trees stored in the CAS and a
// real filesystem. It is used by the executor-agent to materialize an action's
// input root before execution and to capture the action's outputs afterwards.
package casfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// materializeConcurrency bounds the number of concurrent CAS reads issued while
// materializing an input tree. Go build actions ship the entire Go SDK as
// inputs (thousands of small blobs), so fetching them one at a time dominates
// action latency; fetching them in parallel is the single biggest speedup.
const materializeConcurrency = 64

// Getter reads blobs from the CAS.
type Getter interface {
	Get(ctx context.Context, d *repb.Digest) ([]byte, error)
}

// Putter writes blobs to the CAS.
type Putter interface {
	Put(ctx context.Context, d *repb.Digest, data []byte) error
}

// Materialize writes the directory tree rooted at root into dir, fetching every
// Directory and File blob from the CAS. Blobs are fetched concurrently (bounded
// by materializeConcurrency); on the first error it stops and returns it.
func Materialize(ctx context.Context, store Getter, root *repb.Digest, dir string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := &materializer{
		store:  store,
		ctx:    ctx,
		cancel: cancel,
		sem:    make(chan struct{}, materializeConcurrency),
	}
	m.scheduleDir(root, dir)
	m.wg.Wait()
	return m.firstErr()
}

// materializer fans out CAS reads across a bounded pool of goroutines. Each
// directory and file blob is fetched in its own goroutine that blocks on sem
// before issuing the read, so at most materializeConcurrency reads are in
// flight. Goroutines never wait on one another (only the top-level Wait does),
// which keeps the bounded pool free of hold-and-wait deadlocks.
type materializer struct {
	store  Getter
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup

	mu  sync.Mutex
	err error
}

func (m *materializer) fail(err error) {
	m.mu.Lock()
	if m.err == nil {
		m.err = err
		m.cancel()
	}
	m.mu.Unlock()
}

func (m *materializer) firstErr() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

// get issues a single CAS read while holding one of the concurrency slots.
func (m *materializer) get(d *repb.Digest) ([]byte, error) {
	select {
	case m.sem <- struct{}{}:
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
	defer func() { <-m.sem }()
	return m.store.Get(m.ctx, d)
}

func (m *materializer) scheduleDir(d *repb.Digest, path string) {
	m.wg.Add(1)
	go m.processDir(d, path)
}

func (m *materializer) processDir(d *repb.Digest, path string) {
	defer m.wg.Done()
	if m.firstErr() != nil {
		return
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		m.fail(err)
		return
	}
	data, err := m.get(d)
	if err != nil {
		m.fail(fmt.Errorf("fetch directory %s: %w", digest.Key(d), err))
		return
	}
	dir := &repb.Directory{}
	if err := proto.Unmarshal(data, dir); err != nil {
		m.fail(fmt.Errorf("unmarshal directory %s: %w", digest.Key(d), err))
		return
	}

	// Symlinks are cheap and local; create them inline. Files and subdirectories
	// each become their own scheduled unit. The directory itself already exists
	// (created above) before any child work is scheduled into it.
	for _, sl := range dir.GetSymlinks() {
		if err := checkName(sl.GetName()); err != nil {
			m.fail(err)
			return
		}
		if err := os.Symlink(sl.GetTarget(), filepath.Join(path, sl.GetName())); err != nil {
			m.fail(err)
			return
		}
	}
	for _, f := range dir.GetFiles() {
		if err := checkName(f.GetName()); err != nil {
			m.fail(err)
			return
		}
		m.scheduleFile(f, filepath.Join(path, f.GetName()))
	}
	for _, sub := range dir.GetDirectories() {
		if err := checkName(sub.GetName()); err != nil {
			m.fail(err)
			return
		}
		m.scheduleDir(sub.GetDigest(), filepath.Join(path, sub.GetName()))
	}
}

func (m *materializer) scheduleFile(f *repb.FileNode, path string) {
	m.wg.Add(1)
	go m.processFile(f, path)
}

func (m *materializer) processFile(f *repb.FileNode, path string) {
	defer m.wg.Done()
	if m.firstErr() != nil {
		return
	}
	content, err := m.get(f.GetDigest())
	if err != nil {
		m.fail(fmt.Errorf("fetch file %q (%s): %w", f.GetName(), digest.Key(f.GetDigest()), err))
		return
	}
	mode := os.FileMode(0o644)
	if f.GetIsExecutable() {
		mode = 0o755
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		m.fail(err)
		return
	}
}

// CaptureFile uploads the file at path and returns an OutputFile referencing it.
func CaptureFile(ctx context.Context, store Putter, outputPath, fsPath string, executable bool) (*repb.OutputFile, error) {
	content, err := os.ReadFile(fsPath)
	if err != nil {
		return nil, err
	}
	d := digest.FromBytes(content)
	if err := store.Put(ctx, d, content); err != nil {
		return nil, err
	}
	return &repb.OutputFile{
		Path:         outputPath,
		Digest:       d,
		IsExecutable: executable,
	}, nil
}

// CaptureDirectory walks the directory at fsPath, uploads every file and
// Directory blob plus a single Tree blob, and returns an OutputDirectory whose
// tree_digest references that Tree.
func CaptureDirectory(ctx context.Context, store Putter, outputPath, fsPath string) (*repb.OutputDirectory, error) {
	root, rootDigest, descendants, err := buildDirectory(ctx, store, fsPath)
	if err != nil {
		return nil, err
	}
	tree := &repb.Tree{Root: root, Children: descendants}
	treeBytes, err := proto.Marshal(tree)
	if err != nil {
		return nil, err
	}
	treeDigest := digest.FromBytes(treeBytes)
	if err := store.Put(ctx, treeDigest, treeBytes); err != nil {
		return nil, err
	}
	return &repb.OutputDirectory{
		Path:                outputPath,
		TreeDigest:          treeDigest,
		RootDirectoryDigest: rootDigest,
	}, nil
}

// buildDirectory constructs the Directory message for fsPath, uploading each
// file blob and each nested Directory blob. It returns the Directory, its
// digest, and a flattened slice of all descendant directories (for the Tree).
func buildDirectory(ctx context.Context, store Putter, fsPath string) (*repb.Directory, *repb.Digest, []*repb.Directory, error) {
	entries, err := os.ReadDir(fsPath)
	if err != nil {
		return nil, nil, nil, err
	}
	dir := &repb.Directory{}
	var descendants []*repb.Directory

	for _, e := range entries {
		name := e.Name()
		full := filepath.Join(fsPath, name)
		info, err := e.Info()
		if err != nil {
			return nil, nil, nil, err
		}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(full)
			if err != nil {
				return nil, nil, nil, err
			}
			dir.Symlinks = append(dir.Symlinks, &repb.SymlinkNode{Name: name, Target: target})
		case e.IsDir():
			child, childDigest, childDesc, err := buildDirectory(ctx, store, full)
			if err != nil {
				return nil, nil, nil, err
			}
			dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: childDigest})
			descendants = append(descendants, child)
			descendants = append(descendants, childDesc...)
		default:
			content, err := os.ReadFile(full)
			if err != nil {
				return nil, nil, nil, err
			}
			d := digest.FromBytes(content)
			if err := store.Put(ctx, d, content); err != nil {
				return nil, nil, nil, err
			}
			dir.Files = append(dir.Files, &repb.FileNode{
				Name:         name,
				Digest:       d,
				IsExecutable: info.Mode()&0o111 != 0,
			})
		}
	}

	// REAPI requires the children of a Directory to be sorted by name.
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })
	sort.Slice(dir.Symlinks, func(i, j int) bool { return dir.Symlinks[i].Name < dir.Symlinks[j].Name })

	dirBytes, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	dirDigest := digest.FromBytes(dirBytes)
	if err := store.Put(ctx, dirDigest, dirBytes); err != nil {
		return nil, nil, nil, err
	}
	return dir, dirDigest, descendants, nil
}

func checkName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.ContainsRune(name, os.PathSeparator) {
		return fmt.Errorf("invalid tree entry name %q", name)
	}
	return nil
}
