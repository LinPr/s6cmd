package fsstore

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/LinPr/s6cmd/storage"
)

func mustURL(t *testing.T, s string) *storage.StorageURL {
	t.Helper()
	u, err := storage.NewStorageURL(s)
	if err != nil {
		t.Fatalf("NewStorageURL(%q): %v", s, err)
	}
	return u
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}

// collectList drains the List channel into sorted base names, failing the
// test on any per-object error.
func collectList(t *testing.T, f *FileStore, src *storage.StorageURL, followSymlinks bool) []string {
	t.Helper()
	var names []string
	for obj := range f.List(context.Background(), src, followSymlinks) {
		if obj.Err != nil {
			t.Fatalf("List returned error object: %v", obj.Err)
		}
		names = append(names, filepath.Base(obj.StorageURL.Absolute()))
	}
	sort.Strings(names)
	return names
}

// TestCopyBasic verifies Copy lands the source content at the destination
// (creating parent directories), overwrites an existing file, and leaves
// no temporary file behind.
func TestCopyBasic(t *testing.T) {
	t.Parallel()
	f := NewFileStore(context.Background(), LocalOption{})
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.txt")
	writeFile(t, srcPath, "payload")

	dstPath := filepath.Join(dir, "sub", "dst.txt")
	if err := f.Copy(context.Background(), mustURL(t, srcPath), mustURL(t, dstPath), storage.Metadata{}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("dst content = %q, want %q", got, "payload")
	}

	// Overwrite with new content.
	writeFile(t, srcPath, "updated")
	if err := f.Copy(context.Background(), mustURL(t, srcPath), mustURL(t, dstPath), storage.Metadata{}); err != nil {
		t.Fatalf("Copy (overwrite): %v", err)
	}
	got, err = os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "updated" {
		t.Errorf("dst content after overwrite = %q, want %q", got, "updated")
	}

	entries, err := os.ReadDir(filepath.Dir(dstPath))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "dst.txt" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("destination dir entries = %v, want only [dst.txt] (no temp leftovers)", names)
	}
}

// TestCopyMissingSourceLeavesDestination verifies that a failed copy (source
// does not exist) does not truncate an existing destination file.
func TestCopyMissingSourceLeavesDestination(t *testing.T) {
	t.Parallel()
	f := NewFileStore(context.Background(), LocalOption{})
	dir := t.TempDir()

	dstPath := filepath.Join(dir, "dst.txt")
	writeFile(t, dstPath, "keep me")

	err := f.Copy(context.Background(), mustURL(t, filepath.Join(dir, "missing.txt")), mustURL(t, dstPath), storage.Metadata{})
	if err == nil {
		t.Fatal("Copy succeeded, want error for missing source")
	}
	got, readErr := os.ReadFile(dstPath)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}
	if string(got) != "keep me" {
		t.Errorf("dst content = %q, want untouched %q", got, "keep me")
	}
}

// TestExpandGlobDanglingSymlink verifies that a dangling symlink matched by
// a glob does not abort the whole listing: the healthy files are still
// emitted and the broken entry is skipped.
func TestExpandGlobDanglingSymlink(t *testing.T) {
	t.Parallel()
	f := NewFileStore(context.Background(), LocalOption{})
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "a.txt"), "a")
	writeFile(t, filepath.Join(dir, "b.txt"), "b")
	if err := os.Symlink(filepath.Join(dir, "gone.txt"), filepath.Join(dir, "broken.txt")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}

	src := mustURL(t, filepath.Join(dir, "*.txt"))
	for _, followSymlinks := range []bool{true, false} {
		got := collectList(t, f, src, followSymlinks)
		want := []string{"a.txt", "b.txt"}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("followSymlinks=%v: List = %v, want %v", followSymlinks, got, want)
		}
	}
}

// TestExpandGlobSymlinkedFile verifies that a glob-matched symlink to a
// regular file is skipped when followSymlinks is false and followed when
// it is true.
func TestExpandGlobSymlinkedFile(t *testing.T) {
	t.Parallel()
	f := NewFileStore(context.Background(), LocalOption{})
	dir := t.TempDir()
	targetDir := t.TempDir()

	writeFile(t, filepath.Join(dir, "real.txt"), "real")
	targetPath := filepath.Join(targetDir, "target.txt")
	writeFile(t, targetPath, "target")
	if err := os.Symlink(targetPath, filepath.Join(dir, "link.txt")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}

	src := mustURL(t, filepath.Join(dir, "*.txt"))

	got := collectList(t, f, src, false)
	if len(got) != 1 || got[0] != "real.txt" {
		t.Errorf("followSymlinks=false: List = %v, want [real.txt]", got)
	}

	got = collectList(t, f, src, true)
	if len(got) != 2 || got[0] != "link.txt" || got[1] != "real.txt" {
		t.Errorf("followSymlinks=true: List = %v, want [link.txt real.txt]", got)
	}
}
