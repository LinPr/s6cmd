// Package cp implements the `s6cmd cp` command. The cp command structure
// is SharedFlags + shouldOverride + prepareDownloadTask/
// prepareUploadTask/prepareCopyTask + countingReaderWriter, and uses cobra
// + aws-sdk-go-v2 + the s6cmd parallel.Manager/Waiter framework.
//
// The command dispatches each source object to one of three do* functions
// based on the src/dst URL types:
//
//	src remote, dst remote   -> doCopy   (server-side CopyObject)
//	src remote, dst local    -> doDownload (manager.Downloader)
//	src local,  dst remote   -> doUpload   (manager.Uploader)
//	src local,  dst local    -> local copy via the filesystem store
//
// Tasks run on the global parallel.Manager; errors are aggregated from the
// Waiter's error channel and returned as a single errors.Join error so the
// RunE contract surfaces every failure instead of just the first.
package cp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/internal/progressbar"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NewCpCmd creates the `cp` command. The command registers the shared
// flags (so --concurrency/--part-size/--acl/... are available on cp, mv
// and sync) plus the cp-specific flags (--flatten/--no-clobber/...).
func NewCpCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "cp [flags] <source> <destination>",
		Short:   "copy file or files from source to destination",
		Example: cp_examples,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: cp %s %s\n", o.SrcUri, o.DestUri)
				return nil
			}
			return o.run(ctx)
		},
	}

	// cp-specific flags. They are not in SharedFlags because mv/sync do
	// not expose them.
	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
	cmd.Flags().BoolVarP(&o.Flatten, "flatten", "f", false, "flatten directory structure of source, starting from the first wildcard")
	cmd.Flags().BoolVar(&o.NoClobber, "no-clobber", false, "do not overwrite destination if already exists")
	cmd.Flags().BoolVarP(&o.IfSizeDiffer, "if-size-differ", "s", false, "only overwrite destination if size differs")
	cmd.Flags().BoolVarP(&o.IfSourceNewer, "if-source-newer", "u", false, "only overwrite destination if source modtime is newer")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")
	cmd.Flags().BoolVar(&o.ShowProgress, "show-progress", false, "show a progress bar")
	cmd.Flags().BoolVar(&o.Recursive, "recursive", false, "copy objects recursively (kept for backwards compatibility with prior s6cmd releases)")

	// Shared flags: --concurrency, --part-size, --acl, --metadata, ...
	o.Shared.AddToCmd(&cmd)

	return &cmd
}

// Args holds the positional arguments. They are validated via the
// `validate:"required"` tag so an empty cp invocation surfaces a clear
// error before any S3 call is made.
type Args struct {
	SrcUri  string `validate:"required"`
	DestUri string `validate:"required"`
}

// Flags holds the cp-specific flags. Shared flags live in SharedFlags
// below.
type Flags struct {
	DryRun        bool
	Flatten       bool
	NoClobber     bool
	IfSizeDiffer  bool
	IfSourceNewer bool
	VersionID     string
	ShowProgress  bool
	Recursive     bool

	// CommonFlags holds the global flags inherited from the parent
	// command (endpoint, region, profile, ...). It is populated in
	// complete() so run() does not have to touch cobra again.
	cliutil.CommonFlags
}

// Options is the closure of Args + Flags + SharedFlags. It is the single
// value passed through complete -> validate -> run.
type Options struct {
	Args
	Flags
	Shared *cliutil.SharedFlags
}

func newOptions() *Options {
	return &Options{Shared: cliutil.NewSharedFlags()}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	if len(args) >= 2 {
		o.SrcUri = args[0]
		o.DestUri = args[1]
	}
	o.CommonFlags = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	if err := o.Shared.ValidateMetadataDirective(); err != nil {
		return err
	}
	return nil
}

func (o *Options) run(ctx context.Context) error {
	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	srcURL, err := storage.NewStorageURL(o.SrcUri, storage.WithVersion(o.VersionID), storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	dstURL, err := storage.NewStorageURL(o.DestUri, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}

	// dst must not be a wildcard.
	if dstURL.IsWildcard() {
		return fmt.Errorf("target %q can not contain glob characters", o.DestUri)
	}

	// Pre-compile exclude/include patterns once; isObjectExcluded uses
	// them per object.
	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Exclude)
	if err != nil {
		return err
	}
	includePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Include)
	if err != nil {
		return err
	}

	// Local->local copy does not need the parallel.Manager; the filesystem
	// store's Copy is synchronous and cheap. Keep it on a tiny worker pool
	// for parity with the other paths.
	if !srcURL.IsRemote() && !dstURL.IsRemote() {
		return copyLocalToLocal(ctx, store, srcURL, dstURL, o)
	}

	pb := progressbar.New(o.ShowProgress && srcURL.IsRemote() != dstURL.IsRemote())
	pb.Start()
	defer pb.Finish()

	// Build a list of source objects first so the waiter goroutine can
	// start draining before the first parallel.Run. The list is bounded by
	// the size of the source, which is acceptable for the same reason a
	// single cp invocation is not expected to enumerate millions of
	// objects (that is what sync is for).
	objects, err := expandSource(ctx, store, srcURL, !o.Shared.NoFollowSymlinks, o.Recursive)
	if err != nil {
		return err
	}

	waiter := parallel.NewWaiter()
	errs := make([]error, 0)
	errDoneCh := make(chan struct{})
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			if errorpkg.IsCancelation(err) {
				continue
			}
			if errorpkg.IsWarning(err) {
				log.Debug(log.DebugMessage{Err: err.Error()})
				continue
			}
			log.Error(log.ErrorMessage{Operation: "cp", Err: err.Error()})
			errs = append(errs, err)
		}
	}()

	isBatch := srcURL.IsWildcard()
	if !isBatch && !srcURL.IsRemote() {
		obj, statErr := store.Stat(ctx, srcURL)
		if statErr != nil {
			return statErr
		}
		isBatch = obj != nil && obj.Type.IsDir()
	}

	for _, object := range objects {
		if object.Err != nil {
			errs = append(errs, object.Err)
			log.Error(log.ErrorMessage{Operation: "cp", Err: object.Err.Error()})
			continue
		}
		if object.Type.IsDir() {
			continue
		}
		if !object.Type.IsRegular() {
			err := fmt.Errorf("object %v is not a regular file", object)
			errs = append(errs, err)
			log.Error(log.ErrorMessage{Operation: "cp", Err: err.Error()})
			continue
		}
		// Exclude/include filtering uses the relative path so patterns
		// behave the same way for prefix and wildcard sources.
		name := object.StorageURL.Relative()
		if name == "" {
			name = object.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}

		pb.AddTotalBytes(object.Size)
		pb.IncrementTotalObjects()

		var task parallel.Task
		switch {
		case srcURL.IsRemote() && dstURL.IsRemote():
			task = o.prepareCopyTask(ctx, store, object.StorageURL, dstURL, isBatch)
		case srcURL.IsRemote() && !dstURL.IsRemote():
			task = o.prepareDownloadTask(ctx, store, object.StorageURL, dstURL, isBatch, pb)
		case !srcURL.IsRemote() && dstURL.IsRemote():
			task = o.prepareUploadTask(ctx, store, object.StorageURL, dstURL, isBatch, pb)
		default:
			// Local->local should have been handled above; guard against
			// future src/dst type combinations surfacing as silent no-ops.
			err := fmt.Errorf("unsupported cp pair: src=%v dst=%v", srcURL, dstURL)
			errs = append(errs, err)
			log.Error(log.ErrorMessage{Operation: "cp", Err: err.Error()})
			continue
		}
		parallel.Run(task, waiter)
	}
	waiter.Wait()
	<-errDoneCh

	return cliutil.AggregateErrors(errs)
}

// expandSource materializes the list of source objects. For a single
// non-wildcard, non-directory source it returns a one-element slice;
// otherwise it drains the channel returned by storage.List.
//
// The function deliberately returns a slice rather than a channel so the
// caller can iterate it without spawning a second consumer goroutine,
// which would race with the waiter goroutine on the errs slice.
func expandSource(ctx context.Context, store *storage.Storage, src *storage.StorageURL, followSymlinks, recursive bool) ([]*storage.Object, error) {
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
			// Remote single object: List will HEAD/GET it; use it directly.
			obj, err := store.Stat(ctx, src)
			if err != nil {
				return nil, err
			}
			return []*storage.Object{obj}, nil
		}
	}

	// Prefix / bucket / wildcard / local dir: drain the List channel into
	// a slice. We collect eagerly because the consumer of the slice (the
	// task-submission loop) must run on the main goroutine to avoid
	// racing the errs slice.
	out := make([]*storage.Object, 0, 64)
	for obj := range store.List(ctx, src, followSymlinks) {
		if obj.Err != nil && errorpkg.IsCancelation(obj.Err) {
			continue
		}
		out = append(out, obj)
	}
	_ = recursive
	return out, nil
}

// prepareCopyTask builds a server-side copy task (S3 -> S3). It is the
// only path that honours --metadata-directive.
func (o *Options) prepareCopyTask(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, isBatch bool) parallel.Task {
	return func() error {
		dst := prepareRemoteDestination(srcURL, dstURL, o.Flatten, isBatch)
		if err := o.doCopy(ctx, store, srcURL, dst); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dst.String(), Err: err}
		}
		return nil
	}
}

// prepareDownloadTask builds a remote -> local download task.
func (o *Options) prepareDownloadTask(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, isBatch bool, pb progressbar.ProgressBar) parallel.Task {
	return func() error {
		dst, err := prepareLocalDestination(ctx, store, srcURL, dstURL, o.Flatten, isBatch)
		if err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dstURL.String(), Err: err}
		}
		if err := o.doDownload(ctx, store, srcURL, dst, pb); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dst.String(), Err: err}
		}
		return nil
	}
}

// prepareUploadTask builds a local -> remote upload task.
func (o *Options) prepareUploadTask(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, isBatch bool, pb progressbar.ProgressBar) parallel.Task {
	return func() error {
		dst := prepareRemoteDestination(srcURL, dstURL, o.Flatten, isBatch)
		if err := o.doUpload(ctx, store, srcURL, dst, pb); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dst.String(), Err: err}
		}
		return nil
	}
}

// doCopy performs a server-side CopyObject. Metadata is assembled from the
// shared flags and the metadata-directive default follows the rule:
// COPY for local->local (irrelevant here), REPLACE for S3->S3 when the
// user did not pass a directive explicitly.
func (o *Options) doCopy(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL) error {
	directive := o.Shared.MetadataDirective
	if directive == "" {
		directive = cliutil.MetadataDirectiveReplace
	}
	md := o.sharedMetadata()
	md.Directive = directive

	if err := o.shouldOverride(ctx, store, srcURL, dstURL); err != nil {
		if errorpkg.IsWarning(err) {
			log.Debug(log.DebugMessage{Operation: "cp", Err: err.Error()})
			return nil
		}
		return err
	}

	if err := store.Copy(ctx, srcURL, dstURL, md); err != nil {
		return err
	}

	log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.String(), Destination: dstURL.String()})
	return nil
}

// doDownload downloads a remote object to a local file via the multipart
// downloader. It writes to a temp file in the destination directory and
// renames on success so a partial download never replaces a complete file.
func (o *Options) doDownload(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, pb progressbar.ProgressBar) error {
	if err := o.shouldOverride(ctx, store, srcURL, dstURL); err != nil {
		if errorpkg.IsWarning(err) {
			log.Debug(log.DebugMessage{Operation: "cp", Err: err.Error()})
			return nil
		}
		return err
	}

	local := localTempStore(store, dstURL)
	if local == nil {
		// Fall back to the legacy DownloadFile wrapper when the local
		// backend does not expose CreateTemp/Rename (it always does in
		// practice, but we do not want a panic if a mock is plugged in).
		return store.DownloadFile(ctx, srcURL.Bucket, srcURL.Path, dstURL.Absolute())
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

	writer := cliutil.NewCountingReaderWriter(file, pb)
	_, err = store.Get(ctx, srcURL, writer, o.Shared.Concurrency, o.Shared.PartSizeBytes())
	_ = file.Close()
	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := local.Rename(tempPath, dstURL.Absolute()); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.String(), Destination: dstURL.Absolute()})
	return nil
}

// doUpload uploads a local file to S3 via the multipart uploader. The
// content type is guessed from the extension (and the first 512 bytes)
// when --content-type is not set explicitly.
func (o *Options) doUpload(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, pb progressbar.ProgressBar) error {
	local := localTempStore(store, srcURL)
	if local == nil {
		_, err := store.UploadFile(ctx, srcURL.Absolute(), dstURL.Bucket, dstURL.Path)
		if err == nil {
			log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.Absolute(), Destination: dstURL.String()})
		}
		return err
	}

	file, err := local.Open(srcURL.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	if err := o.shouldOverride(ctx, store, srcURL, dstURL); err != nil {
		if errorpkg.IsWarning(err) {
			log.Debug(log.DebugMessage{Operation: "cp", Err: err.Error()})
			return nil
		}
		return err
	}

	md := o.sharedMetadata()
	if md.ContentType == "" {
		md.ContentType = cliutil.GuessContentType(file)
	}

	reader := cliutil.NewCountingReaderWriter(file, pb)
	if err := store.Put(ctx, reader, dstURL, md, o.Shared.Concurrency, o.Shared.PartSizeBytes()); err != nil {
		return err
	}

	log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.Absolute(), Destination: dstURL.String()})
	return nil
}

// shouldOverride returns nil when the destination should be overwritten,
// and an errorpkg.ErrObject* sentinel (which is a warning, not a failure)
// when it should not. The flags --no-clobber / --if-size-differ /
// --if-source-newer gate the behaviour.
//
// The sentinel errors are recognized by errorpkg.IsWarning so the task
// function can short-circuit without surfacing as a command failure.
func (o *Options) shouldOverride(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL) error {
	if !o.NoClobber && !o.IfSizeDiffer && !o.IfSourceNewer {
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
	if o.NoClobber {
		stickyErr = errorpkg.ErrObjectExists
	}
	if o.IfSizeDiffer {
		if srcObj.Size == dstObj.Size {
			stickyErr = errorpkg.ErrObjectSizesMatch
		} else {
			stickyErr = nil
		}
	}
	if o.IfSourceNewer {
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

// sharedMetadata assembles a storage.Metadata from the SharedFlags. It is
// the single source of truth for doCopy/doUpload so the metadata fields
// never drift between the two paths.
func (o *Options) sharedMetadata() storage.Metadata {
	return storage.Metadata{
		UserDefined:        o.Shared.MetadataMap(),
		ACL:                o.Shared.ACL,
		CacheControl:       o.Shared.CacheControl,
		Expires:            o.Shared.Expires,
		StorageClass:       o.Shared.StorageClass,
		ContentType:        o.Shared.ContentType,
		ContentEncoding:    o.Shared.ContentEncoding,
		ContentDisposition: o.Shared.ContentDisposition,
		EncryptionMethod:   o.Shared.SSE,
		EncryptionKeyID:    o.Shared.SSEKMSKeyID,
	}
}

// prepareRemoteDestination resolves the destination URL for a remote
// target: when the dst is a prefix/bucket, the source object's base (or,
// for batch sources without --flatten, the relative path) is appended.
func prepareRemoteDestination(srcURL, dstURL *storage.StorageURL, flatten, isBatch bool) *storage.StorageURL {
	objname := srcURL.Base()
	if isBatch && !flatten {
		objname = srcURL.Relative()
	}
	if dstURL.IsPrefix() || dstURL.IsBucket() {
		return dstURL.Join(objname)
	}
	return dstURL.Clone()
}

// prepareLocalDestination resolves the destination URL for a local target:
// for batch sources the dst is a directory; otherwise a single-file dst
// may be renamed to the source's base name when it points at a directory.
func prepareLocalDestination(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, flatten, isBatch bool) (*storage.StorageURL, error) {
	objname := srcURL.Base()
	if isBatch && !flatten {
		objname = srcURL.Relative()
	}

	if isBatch {
		if err := mkdirAllLocal(store, dstURL.Absolute()); err != nil {
			return nil, err
		}
	}

	obj, err := store.Stat(ctx, dstURL)
	if err != nil && !errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		return nil, err
	}
	if errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		if err := mkdirAllLocal(store, dstURL.Dir()); err != nil {
			return nil, err
		}
		if strings.HasSuffix(dstURL.Absolute(), "/") {
			return dstURL.Join(objname), nil
		}
		return dstURL, nil
	}
	if obj != nil && obj.Type.IsDir() {
		return obj.StorageURL.Join(objname), nil
	}
	return dstURL, nil
}

// mkdirAllLocal is a tiny shim that calls MkdirAll on the local backend
// when it exposes the method (which *fsstore.FileStore does). Using a
// type-assertion keeps the storage.Storage surface small.
func mkdirAllLocal(store *storage.Storage, dir string) error {
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

// copyLocalToLocal handles the local->local case. It walks the source
// (directory or wildcard) and copies each file to the destination using
// the local store's Copy, which is a plain io.Copy with MkdirAll.
func copyLocalToLocal(ctx context.Context, store *storage.Storage, srcURL, dstURL *storage.StorageURL, o *Options) error {
	srcBase := srcURL.Absolute()
	if srcURL.IsWildcard() {
		srcBase = cliutil.WildcardBasePath(srcBase)
	}

	files, err := cliutil.ListLocalFiles(srcURL.Absolute(), true)
	if err != nil {
		return err
	}
	if len(files) > 1 {
		isDir, err := cliutil.IsLocalDir(dstURL.Absolute())
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when copying multiple sources")
		}
	}

	waiter := parallel.NewWaiter()
	errs := make([]error, 0)
	errDoneCh := make(chan struct{})
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			if errorpkg.IsCancelation(err) {
				continue
			}
			errs = append(errs, err)
		}
	}()

	for _, file := range files {
		fileURL, err := storage.NewStorageURL(file)
		if err != nil {
			return err
		}
		var dst string
		if len(files) > 1 {
			rel, err := filepath.Rel(srcBase, file)
			if err != nil {
				return err
			}
			dst = filepath.Join(dstURL.Absolute(), filepath.FromSlash(rel))
		} else {
			isDir, err := cliutil.IsLocalDir(dstURL.Absolute())
			if err != nil {
				return err
			}
			if isDir {
				dst = filepath.Join(dstURL.Absolute(), filepath.Base(file))
			} else {
				dst = dstURL.Absolute()
			}
		}
		src := fileURL
		dstCopy := dst
		parallel.Run(func() error {
			dstURLCopy, err := storage.NewStorageURL(dstCopy)
			if err != nil {
				return err
			}
			if err := store.Copy(ctx, src, dstURLCopy, storage.Metadata{}); err != nil {
				return &errorpkg.Error{Op: "cp", Src: src.Absolute(), Dst: dstCopy, Err: err}
			}
			log.Info(log.InfoMessage{Operation: "cp", Source: src.Absolute(), Destination: dstCopy})
			return nil
		}, waiter)
	}
	waiter.Wait()
	<-errDoneCh
	return cliutil.AggregateErrors(errs)
}

// fsStoreWithTemp is the interface the local backend must satisfy for
// doDownload/doUpload to use the temp-file + rename + counting reader
// path. It is unexported because it is an internal contract between cp
// and the filesystem store; callers must not implement it themselves.
type fsStoreWithTemp interface {
	CreateTemp(dir, pattern string) (*os.File, error)
	MkdirAll(path string) error
	Rename(oldpath, newpath string) error
	Open(path string) (*os.File, error)
}

// localTempStore returns the local backend as a fsStoreWithTemp, or nil
// when the backend does not expose the temp-file methods (in which case
// doDownload/doUpload fall back to the legacy wrappers).
func localTempStore(store *storage.Storage, url *storage.StorageURL) fsStoreWithTemp {
	if ts, ok := store.ClientFor(url).(fsStoreWithTemp); ok {
		return ts
	}
	return nil
}
