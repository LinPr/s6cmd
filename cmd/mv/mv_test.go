package mv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinPr/s6cmd/storage"
)

// TestMoveLocalToLocalDirIntoDir verifies POSIX mv semantics: moving a
// directory into an EXISTING directory creates dst/Base(src) instead of
// merging src's contents into dst (the previous behaviour).
func TestMoveLocalToLocalDirIntoDir(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "srcdir")
	dst := filepath.Join(work, "existing")
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	mustWrite(t, filepath.Join(src, "sub", "b.txt"), "b")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}

	o := &Options{}
	if err := o.moveLocalToLocal(src, dst); err != nil {
		t.Fatalf("moveLocalToLocal: %v", err)
	}

	if got := mustRead(t, filepath.Join(dst, "srcdir", "a.txt")); got != "a" {
		t.Errorf("dst/srcdir/a.txt = %q, want %q", got, "a")
	}
	if got := mustRead(t, filepath.Join(dst, "srcdir", "sub", "b.txt")); got != "b" {
		t.Errorf("dst/srcdir/sub/b.txt = %q, want %q", got, "b")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("source dir should be gone, stat err=%v", err)
	}
	// The contents must NOT have been merged directly into dst.
	if _, err := os.Stat(filepath.Join(dst, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("dst/a.txt should not exist (contents merged instead of moved), stat err=%v", err)
	}
}

// TestMoveLocalToLocalFileIntoDir verifies that moving a file into an
// existing directory lands at dst/Base(src).
func TestMoveLocalToLocalFileIntoDir(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "f.txt")
	dst := filepath.Join(work, "dir")
	mustWrite(t, src, "content")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}

	o := &Options{}
	if err := o.moveLocalToLocal(src, dst); err != nil {
		t.Fatalf("moveLocalToLocal: %v", err)
	}
	if got := mustRead(t, filepath.Join(dst, "f.txt")); got != "content" {
		t.Errorf("dst/f.txt = %q, want %q", got, "content")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("source file should be gone, stat err=%v", err)
	}
}

// TestMoveLocalToLocalRename verifies the plain rename path (dst does not
// exist).
func TestMoveLocalToLocalRename(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "old.txt")
	dst := filepath.Join(work, "new.txt")
	mustWrite(t, src, "x")

	o := &Options{}
	if err := o.moveLocalToLocal(src, dst); err != nil {
		t.Fatalf("moveLocalToLocal: %v", err)
	}
	if got := mustRead(t, dst); got != "x" {
		t.Errorf("dst = %q, want %q", got, "x")
	}
}

// TestMoveLocalToLocalDryRun verifies that --dry-run leaves both sides
// untouched.
func TestMoveLocalToLocalDryRun(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "old.txt")
	dst := filepath.Join(work, "new.txt")
	mustWrite(t, src, "x")

	o := &Options{Flags: Flags{DryRun: true}}
	if err := o.moveLocalToLocal(src, dst); err != nil {
		t.Fatalf("moveLocalToLocal dry-run: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Errorf("dry-run must keep the source, stat err=%v", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Errorf("dry-run must not create the destination, stat err=%v", err)
	}
}

// TestPruneEmptyDirs verifies that pruning removes empty directory chains
// but keeps any directory that still holds a file — the guard that makes
// the per-file delete phase safe against files created after the source
// walk (the old os.RemoveAll destroyed them).
func TestPruneEmptyDirs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	empty := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	kept := filepath.Join(root, "keep")
	mustWrite(t, filepath.Join(kept, "late.txt"), "created after the walk")

	pruneEmptyDirs(root)

	if _, err := os.Stat(filepath.Join(root, "a")); !os.IsNotExist(err) {
		t.Errorf("empty chain a/b/c should be pruned, stat err=%v", err)
	}
	if got := mustRead(t, filepath.Join(kept, "late.txt")); got != "created after the walk" {
		t.Errorf("late.txt = %q, want it untouched", got)
	}
	// root itself is non-empty (keep/), so it must survive.
	if _, err := os.Stat(root); err != nil {
		t.Errorf("non-empty root should survive pruning, stat err=%v", err)
	}
}

// TestDeleteMovedSourcesLocal verifies that only the listed (successfully
// transferred) files are deleted and that dry-run deletes nothing.
func TestDeleteMovedSourcesLocal(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "src")
	moved := filepath.Join(src, "moved.txt")
	skipped := filepath.Join(src, "failed-upload.txt")
	mustWrite(t, moved, "1")
	mustWrite(t, skipped, "2")

	srcURL, err := storage.NewStorageURL(src)
	if err != nil {
		t.Fatal(err)
	}
	movedURL, err := storage.NewStorageURL(moved)
	if err != nil {
		t.Fatal(err)
	}

	// Dry-run: nothing is deleted even for "moved" sources.
	dry := &Options{Flags: Flags{DryRun: true}}
	if err := dry.deleteMovedSources(t.Context(), nil, srcURL, true, []*storage.StorageURL{movedURL}); err != nil {
		t.Fatalf("deleteMovedSources dry-run: %v", err)
	}
	if _, err := os.Stat(moved); err != nil {
		t.Fatalf("dry-run must keep every source, stat err=%v", err)
	}

	// Real run: exactly the moved file goes; the failed one stays, so its
	// parent directory must survive pruning too.
	o := &Options{}
	if err := o.deleteMovedSources(t.Context(), nil, srcURL, true, []*storage.StorageURL{movedURL}); err != nil {
		t.Fatalf("deleteMovedSources: %v", err)
	}
	if _, err := os.Stat(moved); !os.IsNotExist(err) {
		t.Errorf("moved source should be deleted, stat err=%v", err)
	}
	if got := mustRead(t, skipped); got != "2" {
		t.Errorf("failed-upload.txt = %q, want it untouched", got)
	}
	if _, err := os.Stat(src); err != nil {
		t.Errorf("src dir still holds a file and must survive, stat err=%v", err)
	}
}

// TestCrossDeviceFallbackDeletesOnlyCopiedFiles verifies the copy+delete
// fallback used when os.Rename fails: only the files that were actually
// copied are removed from the source. A file created AFTER the copy walk
// (simulating a write racing the mv) must survive together with its parent
// directory — the previous blanket os.RemoveAll(src) destroyed it even
// though it was never transferred anywhere.
func TestCrossDeviceFallbackDeletesOnlyCopiedFiles(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "src")
	dst := filepath.Join(work, "dst")
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	mustWrite(t, filepath.Join(src, "sub", "b.txt"), "b")

	// First copy the tree, then plant a "late" file to simulate a file
	// created between the copy walk and the delete phase, then run the
	// delete logic the fallback applies. crossDeviceFallback would copy
	// and delete in one call, so drive its two halves the same way but
	// with the racing file injected in between.
	copied, err := copyLocalTree(src, dst)
	if err != nil {
		t.Fatalf("copyLocalTree: %v", err)
	}
	if len(copied) != 2 {
		t.Fatalf("copied = %q, want 2 entries", copied)
	}
	late := filepath.Join(src, "sub", "late.txt")
	mustWrite(t, late, "created during the move")
	for _, p := range copied {
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove %q: %v", p, err)
		}
	}
	pruneEmptyDirs(src)

	// The copied files are gone from the source and present at the
	// destination.
	if _, err := os.Stat(filepath.Join(src, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("copied a.txt should be deleted from src, stat err=%v", err)
	}
	if got := mustRead(t, filepath.Join(dst, "a.txt")); got != "a" {
		t.Errorf("dst a.txt = %q, want %q", got, "a")
	}
	if got := mustRead(t, filepath.Join(dst, "sub", "b.txt")); got != "b" {
		t.Errorf("dst sub/b.txt = %q, want %q", got, "b")
	}
	// The racing file (and its parent dir) survives.
	if got := mustRead(t, late); got != "created during the move" {
		t.Errorf("late.txt = %q, want it untouched", got)
	}
}

// TestCrossDeviceFallbackWholeTree pins the happy path: with no racing
// writes the fallback moves the whole tree and the (now empty) source
// directory chain is pruned.
func TestCrossDeviceFallbackWholeTree(t *testing.T) {
	t.Parallel()
	work := t.TempDir()
	src := filepath.Join(work, "src")
	dst := filepath.Join(work, "dst")
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	mustWrite(t, filepath.Join(src, "sub", "b.txt"), "b")

	if err := crossDeviceFallback(src, dst, true); err != nil {
		t.Fatalf("crossDeviceFallback: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("empty source tree should be pruned, stat err=%v", err)
	}
	if got := mustRead(t, filepath.Join(dst, "sub", "b.txt")); got != "b" {
		t.Errorf("dst sub/b.txt = %q, want %q", got, "b")
	}

	// Single-file variant.
	srcFile := filepath.Join(work, "f.txt")
	dstFile := filepath.Join(work, "g.txt")
	mustWrite(t, srcFile, "f")
	if err := crossDeviceFallback(srcFile, dstFile, false); err != nil {
		t.Fatalf("crossDeviceFallback(file): %v", err)
	}
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Errorf("source file should be deleted, stat err=%v", err)
	}
	if got := mustRead(t, dstFile); got != "f" {
		t.Errorf("dst file = %q, want %q", got, "f")
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return string(b)
}
