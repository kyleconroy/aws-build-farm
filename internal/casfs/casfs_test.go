package casfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
)

// TestCaptureMaterializeRoundTrip writes a directory tree to disk, captures it
// into an in-memory CAS, then materializes it back out and checks that the
// files, contents, executable bits, and nested structure survive the round trip.
func TestCaptureMaterializeRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := cas.NewMemStore()

	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "top.txt"), "top-level", 0o644)
	mustWrite(t, filepath.Join(src, "run.sh"), "#!/bin/sh\necho hi\n", 0o755)
	if err := os.MkdirAll(filepath.Join(src, "sub", "deep"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(src, "sub", "a.txt"), "alpha", 0o644)
	mustWrite(t, filepath.Join(src, "sub", "deep", "b.txt"), "beta", 0o644)

	od, err := CaptureDirectory(ctx, store, "out", src)
	if err != nil {
		t.Fatalf("CaptureDirectory: %v", err)
	}
	if od.GetRootDirectoryDigest() == nil || od.GetTreeDigest() == nil {
		t.Fatal("OutputDirectory missing digests")
	}

	dst := t.TempDir()
	if err := Materialize(ctx, store, od.GetRootDirectoryDigest(), dst); err != nil {
		t.Fatalf("Materialize: %v", err)
	}

	checkFile(t, filepath.Join(dst, "top.txt"), "top-level", false)
	checkFile(t, filepath.Join(dst, "run.sh"), "#!/bin/sh\necho hi\n", true)
	checkFile(t, filepath.Join(dst, "sub", "a.txt"), "alpha", false)
	checkFile(t, filepath.Join(dst, "sub", "deep", "b.txt"), "beta", false)
}

func mustWrite(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func checkFile(t *testing.T, path, want string, wantExec bool) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Errorf("%s: content = %q, want %q", path, got, want)
	}
	if exec := info.Mode()&0o111 != 0; exec != wantExec {
		t.Errorf("%s: executable = %v, want %v", path, exec, wantExec)
	}
}
