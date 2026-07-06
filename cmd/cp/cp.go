// Package cp implements the `s6cmd cp` command. The cp command structure
// is SharedFlags + cliutil.TransferSpec (ShouldOverride / Copy / Download /
// Upload) + countingReaderWriter, and uses cobra + aws-sdk-go-v2 + the
// s6cmd parallel.Manager/Waiter framework.
//
// The command dispatches each source object to one of three transfer
// primitives based on the src/dst URL types:
//
//	src remote, dst remote   -> TransferSpec.Copy   (server-side CopyObject)
//	src remote, dst local    -> TransferSpec.Download (manager.Downloader)
//	src local,  dst remote   -> TransferSpec.Upload   (manager.Uploader)
//	src local,  dst local    -> local copy via the filesystem store
//
// The transfer primitives live in internal/cliutil so mv shares the exact
// same metadata/exclude/concurrency plumbing.
//
// Tasks run on the global parallel.Manager; errors are aggregated from the
// Waiter's error channel and returned as a single errors.Join error so the
// RunE contract surfaces every failure instead of just the first.
package cp

import (
	"context"
	"fmt"
	"path/filepath"

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
			return o.run(cmd.Context())
		},
	}

	// cp-specific flags. They are not in SharedFlags because mv/sync do
	// not expose them. --dry-run keeps -n; --flatten deliberately has no
	// shorthand (-f is reserved for rb --force, and a fat-fingered -f on a
	// cp must not silently change key layouts).
	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the copy and print one line per operation without transferring anything")
	cmd.Flags().BoolVar(&o.Flatten, "flatten", false, "flatten directory structure of source, starting from the first wildcard")
	cmd.Flags().BoolVar(&o.NoClobber, "no-clobber", false, "do not overwrite destination if already exists")
	cmd.Flags().BoolVarP(&o.IfSizeDiffer, "if-size-differ", "s", false, "only overwrite destination if size differs")
	cmd.Flags().BoolVarP(&o.IfSourceNewer, "if-source-newer", "u", false, "only overwrite destination if source modtime is newer")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")
	cmd.Flags().BoolVar(&o.ShowProgress, "show-progress", false, "show a progress bar on stderr (only when stderr is a terminal; applies to uploads/downloads)")
	cmd.Flags().BoolVar(&o.Recursive, "recursive", false, "copy prefix/bucket/directory sources recursively (required for such sources)")

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
	// Propagate --dry-run into the store constructors so every mutating
	// storage call becomes a no-op while the plan/list/filter phases run
	// for real.
	o.CommonFlags.DryRun = o.DryRun
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

// spec bundles the per-invocation transfer knobs for cliutil's shared
// transfer primitives.
func (o *Options) spec() *cliutil.TransferSpec {
	return &cliutil.TransferSpec{
		Op:            "cp",
		Flatten:       o.Flatten,
		NoClobber:     o.NoClobber,
		IfSizeDiffer:  o.IfSizeDiffer,
		IfSourceNewer: o.IfSourceNewer,
		DryRun:        o.DryRun,
		Shared:        o.Shared,
	}
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

	// Multi-object sources require an explicit --recursive (mirroring rm):
	// a prefix/bucket/directory source expands to every object under it,
	// and a typo'd cp must not copy an entire prefix by accident. Wildcard
	// sources stay allowed without the flag — the pattern itself is an
	// explicit multi-object request.
	if err := o.checkRecursive(srcURL); err != nil {
		return err
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
	objects, err := cliutil.ExpandSource(ctx, store, srcURL, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}

	// Resolve isBatch BEFORE starting the drain goroutine: the Stat error
	// path returns early, and an early return after Drain would leak the
	// drain goroutine blocked on the waiter's never-closed error channel.
	isBatch := srcURL.IsWildcard() || (srcURL.IsRemote() && (srcURL.IsBucket() || srcURL.IsPrefix()))
	if !isBatch && !srcURL.IsRemote() {
		obj, statErr := store.Stat(ctx, srcURL)
		if statErr != nil {
			return statErr
		}
		isBatch = obj != nil && obj.Type.IsDir()
	}

	// The collector serializes appends from the drain goroutine and the
	// submission loop below; both used to append to a shared slice, which
	// was a data race.
	waiter := parallel.NewWaiter()
	ec := cliutil.NewErrorCollector("cp")
	drainDone := ec.Drain(waiter)

	spec := o.spec()
	for _, object := range objects {
		if object.Err != nil {
			ec.Collect(object.Err)
			continue
		}
		if object.Type.IsDir() {
			continue
		}
		if !object.Type.IsRegular() {
			ec.Collect(fmt.Errorf("object %v is not a regular file", object))
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
			task = prepareCopyTask(ctx, store, spec, object.StorageURL, dstURL, isBatch)
		case srcURL.IsRemote() && !dstURL.IsRemote():
			task = prepareDownloadTask(ctx, store, spec, object.StorageURL, dstURL, isBatch, pb)
		case !srcURL.IsRemote() && dstURL.IsRemote():
			task = prepareUploadTask(ctx, store, spec, object.StorageURL, dstURL, isBatch, pb)
		default:
			// Local->local should have been handled above; guard against
			// future src/dst type combinations surfacing as silent no-ops.
			ec.Collect(fmt.Errorf("unsupported cp pair: src=%v dst=%v", srcURL, dstURL))
			continue
		}
		parallel.Run(task, waiter)
	}
	waiter.Wait()
	drainDone()

	return ec.Aggregate()
}

// checkRecursive rejects prefix/bucket/directory sources unless
// --recursive was passed. Wildcard (and --raw) sources are exempt.
func (o *Options) checkRecursive(srcURL *storage.StorageURL) error {
	if o.Recursive || srcURL.IsWildcard() {
		return nil
	}
	if srcURL.IsRemote() {
		if srcURL.IsBucket() || srcURL.IsPrefix() {
			return fmt.Errorf("source %q is a bucket/prefix (use --recursive)", o.SrcUri)
		}
		return nil
	}
	isDir, err := cliutil.IsLocalDir(srcURL.Absolute())
	if err != nil {
		return err
	}
	if isDir {
		return fmt.Errorf("source %q is a directory (use --recursive)", o.SrcUri)
	}
	return nil
}

// prepareCopyTask builds a server-side copy task (S3 -> S3). It is the
// only path that honours --metadata-directive.
func prepareCopyTask(ctx context.Context, store *storage.Storage, spec *cliutil.TransferSpec, srcURL, dstURL *storage.StorageURL, isBatch bool) parallel.Task {
	return func() error {
		dst := cliutil.PrepareRemoteDestination(srcURL, dstURL, spec.Flatten, isBatch)
		if err := spec.Copy(ctx, store, srcURL, dst); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dst.String(), Err: err}
		}
		return nil
	}
}

// prepareDownloadTask builds a remote -> local download task.
func prepareDownloadTask(ctx context.Context, store *storage.Storage, spec *cliutil.TransferSpec, srcURL, dstURL *storage.StorageURL, isBatch bool, pb progressbar.ProgressBar) parallel.Task {
	return func() error {
		dst, err := cliutil.PrepareLocalDestination(ctx, store, srcURL, dstURL, spec.Flatten, isBatch)
		if err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dstURL.String(), Err: err}
		}
		if err := spec.Download(ctx, store, srcURL, dst, pb); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dst.String(), Err: err}
		}
		return nil
	}
}

// prepareUploadTask builds a local -> remote upload task.
func prepareUploadTask(ctx context.Context, store *storage.Storage, spec *cliutil.TransferSpec, srcURL, dstURL *storage.StorageURL, isBatch bool, pb progressbar.ProgressBar) parallel.Task {
	return func() error {
		dst := cliutil.PrepareRemoteDestination(srcURL, dstURL, spec.Flatten, isBatch)
		if err := spec.Upload(ctx, store, srcURL, dst, pb); err != nil {
			return &errorpkg.Error{Op: "cp", Src: srcURL.Absolute(), Dst: dst.String(), Err: err}
		}
		return nil
	}
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
	ec := cliutil.NewErrorCollector("cp")
	drainDone := ec.Drain(waiter)

	// Per-file errors are collected instead of returned: an early return
	// here would leave in-flight copies running after run() has exited
	// and leak the drain goroutine. Every path falls through to
	// waiter.Wait() + drainDone() below.
	for _, file := range files {
		fileURL, err := storage.NewStorageURL(file)
		if err != nil {
			ec.Collect(err)
			continue
		}
		var dst string
		if len(files) > 1 {
			rel, err := filepath.Rel(srcBase, file)
			if err != nil {
				ec.Collect(err)
				continue
			}
			dst = filepath.Join(dstURL.Absolute(), filepath.FromSlash(rel))
		} else {
			isDir, err := cliutil.IsLocalDir(dstURL.Absolute())
			if err != nil {
				ec.Collect(err)
				continue
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
	drainDone()
	return ec.Aggregate()
}
