package fsstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/storage"
)

// FileStore is the storage.Storage implementation for the local filesystem.
type FileStore struct {
	dryRun bool
}

// NewFileStore returns a FileStore. The ctx parameter is accepted for
// symmetry with the S3 constructor; it is not currently used.
func NewFileStore(ctx context.Context, option LocalOption) *FileStore {
	_ = ctx
	return &FileStore{dryRun: option.DryRun}
}

// Stat returns the Object describing the file at url.Absolute(). If the
// path does not exist it returns errorpkg.ErrGivenObjectNotFound.
func (f *FileStore) Stat(ctx context.Context, url *storage.StorageURL) (*storage.Object, error) {
	_ = ctx
	st, err := os.Stat(url.Absolute())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errorpkg.ErrGivenObjectNotFound
		}
		return nil, err
	}
	mod := st.ModTime()
	return &storage.Object{
		StorageURL: url,
		Type:       storage.NewObjectType(st.Mode()),
		Size:       st.Size(),
		ModTime:    &mod,
	}, nil
}

// List returns a channel of Objects matching src. Wildcards use
// filepath.Glob, directories use filepath.WalkDir, single files are
// emitted directly.
func (f *FileStore) List(ctx context.Context, src *storage.StorageURL, followSymlinks bool) <-chan *storage.Object {
	if src.IsWildcard() {
		return f.expandGlob(ctx, src, followSymlinks)
	}
	obj, err := f.Stat(ctx, src)
	isDir := err == nil && obj.Type.IsDir()
	if isDir {
		return f.walkDir(ctx, src, followSymlinks)
	}
	return f.listSingleObject(ctx, src)
}

func (f *FileStore) listSingleObject(ctx context.Context, src *storage.StorageURL) <-chan *storage.Object {
	ch := make(chan *storage.Object, 1)
	defer close(ch)
	obj, err := f.Stat(ctx, src)
	if err != nil {
		obj = &storage.Object{Err: err}
	}
	ch <- obj
	return ch
}

func (f *FileStore) expandGlob(ctx context.Context, src *storage.StorageURL, followSymlinks bool) <-chan *storage.Object {
	ch := make(chan *storage.Object)

	go func() {
		defer close(ch)

		matched, err := filepath.Glob(src.Absolute())
		if err != nil {
			sendError(ctx, err, ch)
			return
		}
		if len(matched) == 0 {
			sendError(ctx, fmt.Errorf("no match found for %q", src), ch)
			return
		}

		for _, filename := range matched {
			fileURL, err := storage.NewStorageURL(filename)
			if err != nil {
				sendError(ctx, err, ch)
				return
			}
			fileURL.SetRelative(src)

			obj, err := f.Stat(ctx, fileURL)
			if err != nil {
				sendError(ctx, err, ch)
				return
			}
			if !obj.Type.IsDir() {
				sendObject(ctx, obj, ch)
				continue
			}
			walkDir(ctx, f, fileURL, followSymlinks, func(o *storage.Object) {
				sendObject(ctx, o, ch)
			})
		}
	}()
	return ch
}

// walkDir walks the directory rooted at src and calls fn for every file
// (symlinks are skipped when followSymlinks is false). It uses
// filepath.WalkDir to avoid pulling in github.com/karrick/godirwalk.
func walkDir(ctx context.Context, f *FileStore, src *storage.StorageURL, followSymlinks bool, fn func(*storage.Object)) {
	if !ShouldProcessURL(src, followSymlinks) {
		return
	}
	err := filepath.WalkDir(src.Absolute(), func(pathname string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		fileURL, err := storage.NewStorageURL(pathname)
		if err != nil {
			return err
		}
		fileURL.SetRelative(src)
		if !ShouldProcessURL(fileURL, followSymlinks) {
			return nil
		}
		obj, err := f.Stat(ctx, fileURL)
		if err != nil {
			return err
		}
		fn(obj)
		return nil
	})
	if err != nil {
		fn(&storage.Object{Err: err})
	}
}

func (f *FileStore) walkDir(ctx context.Context, src *storage.StorageURL, followSymlinks bool) <-chan *storage.Object {
	ch := make(chan *storage.Object)
	go func() {
		defer close(ch)
		walkDir(ctx, f, src, followSymlinks, func(obj *storage.Object) {
			sendObject(ctx, obj, ch)
		})
	}()
	return ch
}

// Copy copies src.Absolute() to dst.Absolute(), creating parent directories
// as needed. The Metadata argument is ignored on the local backend.
func (f *FileStore) Copy(ctx context.Context, src, dst *storage.StorageURL, _ storage.Metadata) error {
	_ = ctx
	if f.dryRun {
		return nil
	}
	if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
		return err
	}
	in, err := os.Open(src.Absolute())
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst.Absolute())
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// Delete removes the file at url.Absolute().
func (f *FileStore) Delete(ctx context.Context, url *storage.StorageURL) error {
	_ = ctx
	if f.dryRun {
		return nil
	}
	return os.Remove(url.Absolute())
}

// MultiDelete deletes every URL read from urlch, emitting an Object per
// URL carrying the per-URL error (nil on success).
func (f *FileStore) MultiDelete(ctx context.Context, urlch <-chan *storage.StorageURL) <-chan *storage.Object {
	resultch := make(chan *storage.Object)
	go func() {
		defer close(resultch)
		for u := range urlch {
			err := f.Delete(ctx, u)
			resultch <- &storage.Object{
				StorageURL: u,
				Err:        err,
			}
		}
	}()
	return resultch
}

// --- Filesystem-specific helpers ---

// MkdirAll creates the directory and any missing parents.
func (f *FileStore) MkdirAll(path string) error {
	if f.dryRun {
		return nil
	}
	return os.MkdirAll(path, os.ModePerm)
}

// Create creates or truncates the named file.
func (f *FileStore) Create(path string) (*os.File, error) {
	if f.dryRun {
		return nil, errors.New("dry-run: cannot create file")
	}
	return os.Create(path)
}

// Open opens the named file for reading.
func (f *FileStore) Open(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY, 0o644)
}

// CreateTemp creates a new temporary file in dir with the given prefix.
func (f *FileStore) CreateTemp(dir, pattern string) (*os.File, error) {
	if f.dryRun {
		return nil, errors.New("dry-run: cannot create temp file")
	}
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// Rename renames oldpath to newpath.
func (f *FileStore) Rename(oldpath, newpath string) error {
	if f.dryRun {
		return nil
	}
	return os.Rename(oldpath, newpath)
}

// ShouldProcessURL reports whether the URL should be processed given the
// follow-symlinks flag. Remote URLs are always processed. Local symlinks
// are skipped when followSymlinks is false.
func ShouldProcessURL(url *storage.StorageURL, followSymlinks bool) bool {
	if followSymlinks {
		return true
	}
	if url.IsRemote() {
		return true
	}
	fi, err := os.Lstat(url.Absolute())
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSymlink == 0
}

func sendObject(ctx context.Context, obj *storage.Object, ch chan<- *storage.Object) {
	select {
	case <-ctx.Done():
	case ch <- obj:
	}
}

func sendError(ctx context.Context, err error, ch chan<- *storage.Object) {
	sendObject(ctx, &storage.Object{Err: err}, ch)
}
