package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeRemote implements just enough of Store and S3Extension for
// DownloadFile. The embedded interfaces satisfy the method sets; any
// method that is not overridden panics with a nil-pointer dereference,
// which is fine because DownloadFile only calls Get.
type fakeRemote struct {
	Store
	S3Extension

	data    []byte
	failGet bool
}

func (f *fakeRemote) Get(_ context.Context, _ *StorageURL, to io.WriterAt, _ int, _ int64) (int64, error) {
	if f.failGet {
		// Write a partial body before failing, mimicking a transfer that
		// dies mid-flight.
		half := f.data[:len(f.data)/2]
		n, _ := to.WriteAt(half, 0)
		return int64(n), errors.New("simulated transfer failure")
	}
	n, err := to.WriteAt(f.data, 0)
	return int64(n), err
}

// listDir returns the sorted names of the entries in dir.
func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestDownloadFileSuccess verifies that a successful download lands the
// full content at the destination path, creates missing parent
// directories, and leaves no temporary file behind.
func TestDownloadFileSuccess(t *testing.T) {
	t.Parallel()
	content := []byte("hello, atomic world")
	store := NewStorage(&fakeRemote{data: content}, nil)

	dir := t.TempDir()
	dst := filepath.Join(dir, "nested", "out.txt")
	if err := store.DownloadFile(context.Background(), "bucket", "key.txt", dst, 0, 0); err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("content = %q, want %q", got, content)
	}
	if names := listDir(t, filepath.Dir(dst)); len(names) != 1 || names[0] != "out.txt" {
		t.Errorf("destination dir entries = %v, want only [out.txt] (no temp leftovers)", names)
	}
}

// TestDownloadFileFailurePreservesExisting verifies that a mid-transfer
// failure neither truncates nor replaces an existing destination file,
// and that the temporary file is cleaned up.
func TestDownloadFileFailurePreservesExisting(t *testing.T) {
	t.Parallel()
	store := NewStorage(&fakeRemote{data: []byte("new partial content"), failGet: true}, nil)

	dir := t.TempDir()
	dst := filepath.Join(dir, "out.txt")
	existing := []byte("existing good content")
	if err := os.WriteFile(dst, existing, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err := store.DownloadFile(context.Background(), "bucket", "key.txt", dst, 0, 0)
	if err == nil {
		t.Fatal("DownloadFile succeeded, want simulated transfer failure")
	}
	if !strings.Contains(err.Error(), "simulated transfer failure") {
		t.Errorf("err = %v, want simulated transfer failure", err)
	}

	got, readErr := os.ReadFile(dst)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}
	if string(got) != string(existing) {
		t.Errorf("destination content = %q, want untouched %q", got, existing)
	}
	if names := listDir(t, dir); len(names) != 1 || names[0] != "out.txt" {
		t.Errorf("destination dir entries = %v, want only [out.txt] (temp file must be removed)", names)
	}
}
