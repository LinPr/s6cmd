// copyops.go holds the object-transfer plumbing shared by cp and mv: source
// expansion, destination resolution and the three transfer primitives
// (server-side copy, download, upload). The code used to live in cmd/cp;
// mv routes its transfers through the same helpers so --metadata /
// --storage-class / --concurrency / --part-size / --exclude / ... behave
// identically across the two commands instead of silently doing nothing on
// mv.
package cliutil

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/progressbar"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
)

// TransferSpec bundles the flag-driven knobs a single cp/mv invocation
// applies to every transferred object. Op is used for log/error
// attribution ("cp"/"mv"); the override flags gate ShouldOverride; DryRun
// suppresses the local-filesystem side effects that the dry-run stores
// cannot intercept (temp-file creation on download).
type TransferSpec struct {
	Op            string
	Flatten       bool
	NoClobber     bool
	IfSizeDiffer  bool
	IfSourceNewer bool
	DryRun        bool
	Shared        *SharedFlags
}

// Metadata assembles a storage.Metadata from the SharedFlags. It is the
// single source of truth for Copy/Upload so the metadata fields never
// drift between the two paths.
func (t *TransferSpec) Metadata() storage.Metadata {
	return storage.Metadata{
		UserDefined:        t.Shared.MetadataMap(),
		ACL:                t.Shared.ACL,
		CacheControl:       t.Shared.CacheControl,
		Expires:            t.Shared.Expires,
		StorageClass:       t.Shared.StorageClass,
		ContentType:        t.Shared.ContentType,
		ContentEncoding:    t.Shared.ContentEncoding,
		ContentDisposition: t.Shared.ContentDisposition,
		EncryptionMethod:   t.Shared.SSE,
		EncryptionKeyID:    t.Shared.SSEKMSKeyID,
	}
}

// ShouldOverride returns nil when the destination should be overwritten,
// and an errorpkg.ErrObject* sentinel (which is a warning, not a failure)
// when it should not. The flags --no-clobber / --if-size-differ /
// --if-source-newer gate the behaviour.
//
// The sentinel errors are recognized by errorpkg.IsWarning so callers can
// short-circuit without surfacing a command failure.
func (t *TransferSpec) ShouldOverride(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL) error {
	if !t.NoClobber && !t.IfSizeDiffer && !t.IfSourceNewer {
		return nil
	}

	srcObj, err := store.Stat(ctx, srcURL)
	if err != nil {
		return err
	}

	dstObj, err := store.Stat(ctx, dstURL)
	if err != nil {
		if errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
			return nil
		}
		return err
	}
	if dstObj == nil {
		return nil
	}

	var stickyErr error
	if t.NoClobber {
		stickyErr = errorpkg.ErrObjectExists
	}
	if t.IfSizeDiffer {
		if srcObj.Size == dstObj.Size {
			stickyErr = errorpkg.ErrObjectSizesMatch
		} else {
			stickyErr = nil
		}
	}
	if t.IfSourceNewer {
		srcMod := srcObj.ModTime
		dstMod := dstObj.ModTime
		if srcMod == nil || dstMod == nil || !srcMod.After(*dstMod) {
			stickyErr = errorpkg.ErrObjectIsNewer
		} else {
			stickyErr = nil
		}
	}
	return stickyErr
}

// Copy performs a server-side CopyObject. Metadata is assembled from the
// shared flags and the metadata-directive default follows the rule:
// REPLACE for S3->S3 when the user did not pass a directive explicitly.
//
// A skip decided by ShouldOverride is returned as the warning sentinel so
// the caller can tell "copied" from "skipped" (mv must not delete the
// source of a skipped copy); errorpkg.IsWarning recognizes it.
func (t *TransferSpec) Copy(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL) error {
	directive := t.Shared.MetadataDirective
	if directive == "" {
		directive = MetadataDirectiveReplace
	}
	md := t.Metadata()
	md.Directive = directive

	if err := t.ShouldOverride(ctx, store, srcURL, dstURL); err != nil {
		return err
	}

	if err := store.Copy(ctx, srcURL, dstURL, md); err != nil {
		return err
	}

	log.Info(log.InfoMessage{Operation: t.Op, Source: srcURL.String(), Destination: dstURL.String()})
	return nil
}

// Download downloads a remote object to a local file via the multipart
// downloader. It writes to a temp file in the destination directory and
// renames on success so a partial download never replaces a complete file.
// Under DryRun the operation is logged and no local file is created or
// truncated.
func (t *TransferSpec) Download(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, pb progressbar.ProgressBar) error {
	if err := t.ShouldOverride(ctx, store, srcURL, dstURL); err != nil {
		return err
	}

	if t.DryRun {
		log.Info(log.InfoMessage{Operation: t.Op, Source: srcURL.String(), Destination: dstURL.Absolute()})
		pb.IncrementCompletedObjects()
		return nil
	}

	local := localTempStore(store, dstURL)
	if local == nil {
		// Fall back to the legacy DownloadFile wrapper when the local
		// backend does not expose CreateTemp/Rename (it always does in
		// practice, but we do not want a panic if a mock is plugged in).
		return store.DownloadFile(ctx, srcURL.Bucket, srcURL.Path, dstURL.Absolute(), t.Shared.Concurrency, t.Shared.PartSizeBytes())
	}

	dstPath := dstURL.Dir()
	if err := local.MkdirAll(dstPath); err != nil {
		return err
	}
	file, err := local.CreateTemp(dstPath, "s6cmd-")
	if err != nil {
		return err
	}
	tempPath := file.Name()

	writer := NewCountingReaderWriter(file, pb)
	_, err = store.Get(ctx, srcURL, writer, t.Shared.Concurrency, t.Shared.PartSizeBytes())
	// A close-time write-back error (NFS, ENOSPC) means the temp file may
	// be corrupt; it must fail the transfer instead of being renamed over
	// the destination.
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := local.Rename(tempPath, dstURL.Absolute()); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	log.Info(log.InfoMessage{Operation: t.Op, Source: srcURL.String(), Destination: dstURL.Absolute()})
	pb.IncrementCompletedObjects()
	return nil
}

// Upload uploads a local file to S3 via the multipart uploader. The
// content type is guessed from the extension (and the first 512 bytes)
// when --content-type is not set explicitly.
func (t *TransferSpec) Upload(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, pb progressbar.ProgressBar) error {
	local := localTempStore(store, srcURL)
	if local == nil {
		_, err := store.UploadFile(ctx, srcURL.Absolute(), dstURL.Bucket, dstURL.Path, t.Shared.Concurrency, t.Shared.PartSizeBytes())
		if err == nil {
			log.Info(log.InfoMessage{Operation: t.Op, Source: srcURL.Absolute(), Destination: dstURL.String()})
		}
		return err
	}

	file, err := local.Open(srcURL.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	if err := t.ShouldOverride(ctx, store, srcURL, dstURL); err != nil {
		return err
	}

	md := t.Metadata()
	if md.ContentType == "" {
		md.ContentType = GuessContentType(file)
	}

	reader := NewCountingReaderWriter(file, pb)
	if err := store.Put(ctx, reader, dstURL, md, t.Shared.Concurrency, t.Shared.PartSizeBytes()); err != nil {
		return err
	}

	log.Info(log.InfoMessage{Operation: t.Op, Source: srcURL.Absolute(), Destination: dstURL.String()})
	pb.IncrementCompletedObjects()
	return nil
}

// ExpandSource materializes the list of source objects. For a single
// non-wildcard, non-directory source it returns a one-element slice;
// otherwise it drains the channel returned by storage.List.
//
// For remote bucket/prefix sources the listing URL's delimiter is cleared
// so S3 returns a flat recursive listing of every key under the prefix,
// and each object's relative path is reset to the full key with the
// listing prefix stripped. Without the reset, StorageURL.Match (via
// parseNonBatch) only surfaces the first path segment, so nested keys
// would be flattened or hidden.
//
// The function deliberately returns a slice rather than a channel so the
// caller can iterate it without spawning a second consumer goroutine,
// which would race with the waiter goroutine on the error slice.
func ExpandSource(ctx context.Context, store *storage.Storage, src *storage.StorageURL, followSymlinks bool) ([]*storage.Object, error) {
	// Single object: stat first so we can detect "source is a directory"
	// (which triggers a walk on the local backend).
	if !src.IsWildcard() {
		if !src.IsRemote() {
			obj, err := store.Stat(ctx, src)
			if err != nil {
				return nil, err
			}
			if !obj.Type.IsDir() {
				return []*storage.Object{obj}, nil
			}
			// Fall through to List, which walks directories.
		} else if !src.IsBucket() && !src.IsPrefix() {
			// Remote single object: use its Stat directly.
			obj, err := store.Stat(ctx, src)
			if err != nil {
				return nil, err
			}
			return []*storage.Object{obj}, nil
		}
	}

	listSrc := src
	listPrefix := ""
	remotePrefix := src.IsRemote() && (src.IsBucket() || src.IsPrefix())
	if remotePrefix {
		clone := src.Clone()
		clone.Delimiter = ""
		listPrefix = clone.Prefix
		listSrc = clone
	}

	// Prefix / bucket / wildcard / local dir: drain the List channel into
	// a slice.
	out := make([]*storage.Object, 0, 64)
	for obj := range store.List(ctx, listSrc, followSymlinks) {
		if obj.Err != nil && errorpkg.IsCancelation(obj.Err) {
			continue
		}
		if obj.Err == nil && remotePrefix && obj.StorageURL != nil {
			rel := strings.TrimPrefix(obj.StorageURL.Path, listPrefix)
			rel = strings.TrimPrefix(rel, "/")
			obj.StorageURL.SetRelativePath(rel)
		}
		out = append(out, obj)
	}
	return out, nil
}

// PrepareRemoteDestination resolves the destination URL for a remote
// target: when the dst is a prefix/bucket, the source object's base (or,
// for batch sources without --flatten, the relative path) is appended.
func PrepareRemoteDestination(srcURL, dstURL *storage.StorageURL, flatten, isBatch bool) *storage.StorageURL {
	objname := srcURL.Base()
	if isBatch && !flatten {
		objname = srcURL.Relative()
	}
	if dstURL.IsPrefix() || dstURL.IsBucket() {
		return dstURL.Join(objname)
	}
	return dstURL.Clone()
}

// PrepareLocalDestination resolves the destination URL for a local target:
// for batch sources the dst is a directory; otherwise a single-file dst
// may be renamed to the source's base name when it points at a directory.
// Whenever a name derived from a remote key is joined onto the local
// destination it is validated first, so a malicious object key (e.g.
// "../../x") cannot escape the destination directory.
func PrepareLocalDestination(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, flatten, isBatch bool) (*storage.StorageURL, error) {
	objname := srcURL.Base()
	if isBatch && !flatten {
		objname = srcURL.Relative()
	}
	// checkObjname guards the Join calls below; it is a no-op for local
	// sources whose relative paths are derived by filepath.Rel.
	checkObjname := func() error {
		if srcURL.IsRemote() {
			return storage.EnsureLocalRelPath(srcURL.Path, objname)
		}
		return nil
	}

	if isBatch {
		if err := MkdirAllLocal(store, dstURL.Absolute()); err != nil {
			return nil, err
		}
	}

	obj, err := store.Stat(ctx, dstURL)
	if err != nil && !errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		return nil, err
	}
	if errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		if err := MkdirAllLocal(store, dstURL.Dir()); err != nil {
			return nil, err
		}
		if strings.HasSuffix(dstURL.Absolute(), "/") {
			if err := checkObjname(); err != nil {
				return nil, err
			}
			return dstURL.Join(objname), nil
		}
		return dstURL, nil
	}
	if obj != nil && obj.Type.IsDir() {
		if err := checkObjname(); err != nil {
			return nil, err
		}
		return obj.StorageURL.Join(objname), nil
	}
	return dstURL, nil
}

// MkdirAllLocal is a tiny shim that calls MkdirAll on the local backend
// when it exposes the method (which *fsstore.FileStore does; it also
// honours dry-run). Using a type-assertion keeps the storage.Storage
// surface small.
func MkdirAllLocal(store *storage.Storage, dir string) error {
	type mkdirAller interface {
		MkdirAll(string) error
	}
	// Use a local URL so ClientFor dispatches to the filesystem store. We
	// can't reference the unexported localObject constant from here, so
	// build the URL from an empty string (NewStorageURL treats a string
	// with no "://" as a local path).
	local, _ := storage.NewStorageURL("")
	if ml, ok := store.ClientFor(local).(mkdirAller); ok {
		return ml.MkdirAll(dir)
	}
	return os.MkdirAll(dir, 0o755)
}

// fsStoreWithTemp is the interface the local backend must satisfy for
// Download/Upload to use the temp-file + rename + counting reader path. It
// is unexported because it is an internal contract between the transfer
// helpers and the filesystem store; callers must not implement it
// themselves.
type fsStoreWithTemp interface {
	CreateTemp(dir, pattern string) (*os.File, error)
	MkdirAll(path string) error
	Rename(oldpath, newpath string) error
	Open(path string) (*os.File, error)
}

// localTempStore returns the local backend as a fsStoreWithTemp, or nil
// when the backend does not expose the temp-file methods (in which case
// Download/Upload fall back to the legacy wrappers).
func localTempStore(store *storage.Storage, url *storage.StorageURL) fsStoreWithTemp {
	if ts, ok := store.ClientFor(url).(fsStoreWithTemp); ok {
		return ts
	}
	return nil
}
