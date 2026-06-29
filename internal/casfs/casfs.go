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

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// Getter reads blobs from the CAS.
type Getter interface {
	Get(ctx context.Context, d *repb.Digest) ([]byte, error)
}

// Putter writes blobs to the CAS.
type Putter interface {
	Put(ctx context.Context, d *repb.Digest, data []byte) error
}

// Materialize writes the directory tree rooted at root into dir, fetching every
// Directory and File blob from the CAS.
func Materialize(ctx context.Context, store Getter, root *repb.Digest, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := store.Get(ctx, root)
	if err != nil {
		return fmt.Errorf("fetch directory %s: %w", digest.Key(root), err)
	}
	d := &repb.Directory{}
	if err := proto.Unmarshal(data, d); err != nil {
		return fmt.Errorf("unmarshal directory %s: %w", digest.Key(root), err)
	}

	for _, f := range d.GetFiles() {
		if err := checkName(f.GetName()); err != nil {
			return err
		}
		content, err := store.Get(ctx, f.GetDigest())
		if err != nil {
			return fmt.Errorf("fetch file %q (%s): %w", f.GetName(), digest.Key(f.GetDigest()), err)
		}
		mode := os.FileMode(0o644)
		if f.GetIsExecutable() {
			mode = 0o755
		}
		if err := os.WriteFile(filepath.Join(dir, f.GetName()), content, mode); err != nil {
			return err
		}
	}

	for _, sl := range d.GetSymlinks() {
		if err := checkName(sl.GetName()); err != nil {
			return err
		}
		if err := os.Symlink(sl.GetTarget(), filepath.Join(dir, sl.GetName())); err != nil {
			return err
		}
	}

	for _, sub := range d.GetDirectories() {
		if err := checkName(sub.GetName()); err != nil {
			return err
		}
		if err := Materialize(ctx, store, sub.GetDigest(), filepath.Join(dir, sub.GetName())); err != nil {
			return err
		}
	}
	return nil
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
