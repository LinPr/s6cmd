package cliutil

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinPr/s6cmd/internal/progressbar"
	"github.com/LinPr/s6cmd/storage"
)

// fakeRemote is a minimal remote backend for TransferSpec.Download tests.
// The embedded nil interfaces satisfy storage.Store and storage.S3Extension
// (so storage.NewStorage detects the S3 extension); only the methods a
// download actually touches are implemented. Calling anything else panics,
// which is exactly what a test should do if the code under test suddenly
// starts calling new methods.
type fakeRemote struct {
	storage.Store
	storage.S3Extension
	// content, when non-empty, is written to the destination at offset 0.
	content []byte
}

func (f *fakeRemote) Get(ctx context.Context, from *storage.StorageURL, to io.WriterAt, concurrency int, partSize int64) (int64, error) {
	if len(f.content) == 0 {
		return 0, nil
	}
	n, err := to.WriteAt(f.content, 0)
	return int64(n), err
}

// closeFailLocal is a local backend whose CreateTemp hands out a file whose
// descriptor is already closed: every subsequent Close returns an error,
// simulating a close-time write-back failure (NFS commit, ENOSPC at flush).
type closeFailLocal struct {
	storage.Store
	tempPath string
	renamed  bool
}

func (f *closeFailLocal) MkdirAll(path string) error { return os.MkdirAll(path, 0o755) }

func (f *closeFailLocal) CreateTemp(dir, pattern string) (*os.File, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	f.tempPath = file.Name()
	// Close the descriptor now: the transfer's own deferred Close must
	// observe the failure (os.ErrClosed) and abort the rename.
	if err := file.Close(); err != nil {
		return nil, err
	}
	return file, nil
}

func (f *closeFailLocal) Rename(oldpath, newpath string) error {
	f.renamed = true
	return os.Rename(oldpath, newpath)
}

func (f *closeFailLocal) Open(path string) (*os.File, error) { return os.Open(path) }

// TestTransferSpecDownload_CloseErrorFailsTransfer is the regression test
// for the promoted-corrupt-file bug: Download used to discard file.Close()'s
// error and rename the temp file into place, so a close-time write-back
// failure silently replaced the destination with a corrupt file and
// reported success. The close error must fail the transfer, skip the
// rename and clean up the temp file.
func TestTransferSpecDownload_CloseErrorFailsTransfer(t *testing.T) {
	dstDir := t.TempDir()
	local := &closeFailLocal{}
	store := storage.NewStorage(&fakeRemote{}, local)

	srcURL, err := storage.NewStorageURL("s3://bucket/key.txt")
	if err != nil {
		t.Fatalf("NewStorageURL(src): %v", err)
	}
	dstPath := filepath.Join(dstDir, "out.txt")
	dstURL, err := storage.NewStorageURL(dstPath)
	if err != nil {
		t.Fatalf("NewStorageURL(dst): %v", err)
	}

	spec := &TransferSpec{Op: "cp", Shared: NewSharedFlags()}
	err = spec.Download(context.Background(), store, srcURL, dstURL, &progressbar.NoOp{})
	if err == nil {
		t.Fatal("Download must fail when the temp file's Close reports an error")
	}
	if !errors.Is(err, os.ErrClosed) {
		t.Errorf("Download error should carry the close error, got %v", err)
	}
	if local.renamed {
		t.Error("a temp file with a failed Close must not be renamed into place")
	}
	if _, statErr := os.Stat(dstPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("destination must not exist after a failed download, stat err: %v", statErr)
	}
	if local.tempPath != "" {
		if _, statErr := os.Stat(local.tempPath); !errors.Is(statErr, os.ErrNotExist) {
			t.Errorf("temp file %q was not cleaned up, stat err: %v", local.tempPath, statErr)
		}
	}
}

// okLocal is closeFailLocal's healthy sibling: CreateTemp returns a normal
// writable temp file, so the success path (write → close → rename) can be
// asserted as a control.
type okLocal struct {
	storage.Store
}

func (f *okLocal) MkdirAll(path string) error { return os.MkdirAll(path, 0o755) }
func (f *okLocal) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}
func (f *okLocal) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }
func (f *okLocal) Open(path string) (*os.File, error)   { return os.Open(path) }

// TestTransferSpecDownload_Success is the control for the close-error test:
// with a healthy temp file the download must rename the temp file into
// place with the fetched content.
func TestTransferSpecDownload_Success(t *testing.T) {
	dstDir := t.TempDir()
	content := []byte("hello download")
	store := storage.NewStorage(&fakeRemote{content: content}, &okLocal{})

	srcURL, err := storage.NewStorageURL("s3://bucket/key.txt")
	if err != nil {
		t.Fatalf("NewStorageURL(src): %v", err)
	}
	dstPath := filepath.Join(dstDir, "out.txt")
	dstURL, err := storage.NewStorageURL(dstPath)
	if err != nil {
		t.Fatalf("NewStorageURL(dst): %v", err)
	}

	spec := &TransferSpec{Op: "cp", Shared: NewSharedFlags()}
	if err := spec.Download(context.Background(), store, srcURL, dstURL, &progressbar.NoOp{}); err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("ReadFile(dst): %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("downloaded content = %q, want %q", got, content)
	}
}
