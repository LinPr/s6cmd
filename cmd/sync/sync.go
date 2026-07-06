// Package sync implements the `s6cmd sync` command. The sync command
// structure is compareObjects + planRun + SyncStrategy, and uses cobra +
// aws-sdk-go-v2 + the s6cmd parallel.Manager framework.
//
// The previous implementation had a data race: the `expected` map was
// written from inside task functions that ran on cliutil.RunTasks worker
// goroutines, while the post-run delete-extra phase read it on the main
// goroutine. This rewrite fixes the race by collecting all source and
// destination objects into slices on the main goroutine, merge-comparing
// them in memory (no goroutine needed for the compare), and only then
// submitting cp/rm tasks to the parallel.Manager.
//
// The plan is keyed by full destination path: every source object is
// resolved to the exact destination URL it will be written to, paired with
// the existing destination object under that key (if any), and the delete
// set for --delete is (destination keys) minus (keys written this run).
// The previous merge-compare matched single-object syncs by Base() names,
// so `sync --delete newsrc.txt s3://bucket/target.txt` classified the
// destination it had just written as "only-destination" and deleted it.
package sync

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
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

func NewSyncCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "sync [flags] <source> <destination>",
		Short:   "sync objects between source and destination",
		Example: sync_examples,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), cmd.InOrStdin(), cmd.ErrOrStderr())
		},
	}

	// sync-specific flags.
	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the sync and print one line per operation without transferring or deleting anything")
	cmd.Flags().BoolVarP(&o.Delete, "delete", "D", false, "delete objects in destination that are not in source")
	cmd.Flags().BoolVarP(&o.Yes, "yes", "y", false, "skip risk prompt for delete")
	cmd.Flags().BoolVar(&o.SizeOnly, "size-only", false, "make size of object the only comparison criterion")
	cmd.Flags().BoolVar(&o.ExitOnError, "exit-on-error", false, "stop the sync process on the first error")
	cmd.Flags().BoolVar(&o.Recursive, "recursive", false, "sync objects recursively (kept for backwards compatibility)")

	// Shared flags: --concurrency, --part-size, --acl, --metadata, ...
	o.Shared.AddToCmd(&cmd)

	return &cmd
}

type Args struct {
	Source      string `validate:"required"`
	Destination string `validate:"required"`
}

type Flags struct {
	DryRun      bool
	Delete      bool
	Yes         bool
	SizeOnly    bool
	ExitOnError bool
	Recursive   bool
	cliutil.CommonFlags
}

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
		o.Source = args[0]
		o.Destination = args[1]
	}
	o.CommonFlags = cliutil.LoadParentFlags(cmd)
	// Propagate --dry-run into the store constructors so every mutating
	// storage call becomes a no-op while the plan/compare phases run for
	// real.
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

func (o *Options) run(ctx context.Context, stdin io.Reader, stderr io.Writer) error {
	// --delete destroys destination objects, so it needs an explicit
	// confirmation: --yes, or an interactive y at the prompt. A dry run
	// deletes nothing and skips the prompt. Non-interactive runs without
	// --yes fail loudly instead of silently skipping the deletes (the
	// previous behaviour, which also ate a line of piped stdin).
	if o.Delete && !o.Yes && !o.DryRun {
		fmt.Fprintf(stderr, "WARNING: this will delete objects in destination that are not in source.\n")
		fmt.Fprintf(stderr, "  source: %s\n  destination: %s\n", o.Source, o.Destination)
		if err := cliutil.Confirm(ctx, stdin, stderr, "Continue?"); err != nil {
			return fmt.Errorf("sync --delete: %w", err)
		}
	}

	srcURL, err := storage.NewStorageURL(o.Source, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	dstURL, err := storage.NewStorageURL(o.Destination, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	if dstURL.IsWildcard() {
		return fmt.Errorf("destination %q can not contain glob characters", o.Destination)
	}

	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	switch {
	case srcURL.IsRemote() && dstURL.IsRemote():
		return o.syncS3ToS3(ctx, store, srcURL, dstURL)
	case srcURL.IsRemote() && !dstURL.IsRemote():
		return o.syncS3ToLocal(ctx, store, srcURL, dstURL)
	case !srcURL.IsRemote() && dstURL.IsRemote():
		return o.syncLocalToS3(ctx, store, srcURL, dstURL)
	default:
		return o.syncLocalToLocal(ctx, store, srcURL, dstURL)
	}
}

// syncPair captures the per-direction dispatch for sync. Each variant
// (S3ToS3/S3ToLocal/LocalToS3/LocalToLocal) populates the source and
// destination object slices via listObjects, then calls planAndRun with
// the right task builder.
type syncPair struct {
	src, dst *storage.StorageURL
}

// listObjects collects every regular object under src into a slice. The
// slice is sorted by the object's relative path so the merge-compare in
// planAndRun can do a single linear sweep. Errors from the listing
// goroutine are surfaced via Object.Err on the offending element so the
// caller can decide whether to abort (when --exit-on-error is set) or
// skip.
//
// When src is a remote bucket/prefix URL, the clone's Delimiter is cleared
// so S3 returns a flat recursive listing of every key under the prefix,
// and the relative path of each object is reset to the full key with the
// listing prefix stripped. Without the reset, storage.URL.Match (via
// parseNonBatch) only surfaces the first path segment — so "src/a.txt"
// would get relative="src/" and the merge-compare would never pair it
// with the corresponding source object, causing every file to be
// re-uploaded even when --size-only should skip it.
//
// isDestination distinguishes destination listings from source listings:
// a destination that does not exist yet is an empty listing, not a
// failure — every first-time sync writes into a missing destination
// prefix/directory, and the not-found sentinels are warnings
// (errorpkg.IsWarning), mirroring ErrorCollector.Collect. Source-side
// not-found errors keep their error semantics: syncing from a nonexistent
// source is a real failure.
func (o *Options) listObjects(ctx context.Context, store *storage.Storage, src *storage.StorageURL, followSymlinks, isDestination bool) ([]*storage.Object, error) {
	listSrc := src
	listPrefix := ""
	if src.IsRemote() && (src.IsBucket() || src.IsPrefix()) {
		clone := src.Clone()
		clone.Delimiter = ""
		listPrefix = clone.Prefix
		listSrc = clone
	}
	out := make([]*storage.Object, 0, 64)
	for obj := range store.List(ctx, listSrc, followSymlinks) {
		if obj.Err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}
			if isDestination && errorpkg.IsWarning(obj.Err) {
				log.Debug(log.DebugMessage{Operation: "sync", Err: obj.Err.Error()})
				continue
			}
			if o.ExitOnError {
				return nil, obj.Err
			}
			log.Error(log.ErrorMessage{Operation: "sync", Err: obj.Err.Error()})
			continue
		}
		if obj.Type.IsDir() {
			continue
		}
		// For remote listings, reset the relative path so it is the full
		// key with the listing prefix trimmed. This makes the SRC and DST
		// listings produce the same relative path for the same key, which
		// is what the merge-compare relies on.
		if src.IsRemote() {
			rel := strings.TrimPrefix(obj.StorageURL.Path, listPrefix)
			rel = strings.TrimPrefix(rel, "/")
			obj.StorageURL.SetRelativePath(rel)
		}
		out = append(out, obj)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StorageURL.Relative() < out[j].StorageURL.Relative()
	})
	return out, nil
}

// planAndRun is the canonical sync flow:
//
//  1. Collect source and destination objects into slices (already done
//     by the caller; we receive them).
//  2. Resolve every source object to its exact destination URL and pair
//     it with the existing destination object under that key, if any
//     (buildSyncPlan).
//  3. Submit cp tasks for every pair the strategy says should be copied
//     on the parallel.Manager.
//  4. When --delete is set, submit rm tasks for the extra destination
//     objects: (destination keys) minus (keys written this run).
//
// All task-submission happens on the main goroutine so the plan maps are
// never written from a worker.
func (o *Options) planAndRun(
	ctx context.Context,
	store *storage.Storage,
	pair syncPair,
	srcObjects, dstObjects []*storage.Object,
	isBatch bool,
	buildTask func(srcURL, dstURL *storage.StorageURL) parallel.Task,
) error {
	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Exclude)
	if err != nil {
		return err
	}
	includePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Include)
	if err != nil {
		return err
	}

	// A non-batch sync onto a local path needs to know whether the path is
	// a directory (copy under it) or a file (copy onto it exactly).
	dstIsDir := false
	if !pair.dst.IsRemote() {
		dstIsDir, err = cliutil.IsLocalDir(pair.dst.Absolute())
		if err != nil {
			return err
		}
	}

	pb := progressbar.New(false)
	pb.Start()
	defer pb.Finish()

	// The collector serializes appends from the drain goroutine and the
	// submission loop below. The drain never stops early: the Waiter's
	// error channel is unbuffered, so the old break-on-first-error drain
	// deadlocked waiter.Wait() as soon as a second task errored. Instead,
	// --exit-on-error stops FURTHER submissions via ec.HasError() checks
	// in the submission loops; tasks already in flight run to completion
	// and their errors are still drained.
	waiter := parallel.NewWaiter()
	ec := cliutil.NewErrorCollector("sync")
	drainDone := ec.Drain(waiter)

	strategy := newStrategy(o.SizeOnly)

	items, extras, planErrs := buildSyncPlan(srcObjects, dstObjects, pair.dst, isBatch, dstIsDir)
	for _, err := range planErrs {
		ec.Collect(err)
	}

	for _, item := range items {
		if o.ExitOnError && ec.HasError() {
			// Stop scheduling new work after the first failure; the
			// drain goroutine keeps consuming errors from in-flight
			// tasks so waiter.Wait() below cannot deadlock.
			break
		}
		// Apply exclude/include on the source name. The destination name
		// is derived from the source name so it does not need separate
		// filtering. Excluded sources still keep their destination key in
		// the plan's written set, so --delete never removes the untouched
		// counterpart of an excluded source.
		name := item.srcObj.StorageURL.Relative()
		if name == "" {
			name = item.srcObj.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}
		if item.dstObj != nil {
			// The destination key already exists: ask the strategy
			// whether to copy over it.
			if err := strategy.ShouldSync(item.srcObj, item.dstObj); err != nil {
				ec.Collect(err)
				continue
			}
		}

		pb.AddTotalBytes(item.srcObj.Size)
		pb.IncrementTotalObjects()

		parallel.Run(buildTask(item.srcObj.StorageURL, item.dstURL), waiter)
	}

	// The delete set is keyed by full destination path — never Base()
	// names — so --delete can never enqueue the destination key of an
	// in-flight copy scheduled above.
	if o.Delete {
		for _, extra := range extras {
			if o.ExitOnError && ec.HasError() {
				break
			}
			o.queueDelete(ctx, store, waiter, extra.StorageURL)
		}
	}

	waiter.Wait()
	drainDone()
	return ec.Aggregate()
}

// syncPlanItem pairs a source object with its resolved destination URL and
// the existing destination object under that key (nil when the key does
// not exist yet).
type syncPlanItem struct {
	srcObj *storage.Object
	dstObj *storage.Object
	dstURL *storage.StorageURL
}

// syncPlanKey canonicalizes a URL for plan matching. Remote URLs use the
// absolute s3://bucket/key form; local URLs are path.Clean'ed so a walked
// path and a Join'ed path for the same file always produce the same key.
func syncPlanKey(u *storage.StorageURL) string {
	if u.IsRemote() {
		return u.Absolute()
	}
	return path.Clean(u.Absolute())
}

// buildSyncPlan resolves every source object to the exact destination URL
// it will be written to, pairs it with the existing destination object
// under the same key (if any), and computes the extra destination objects:
// (destination keys) minus (keys written this run). Matching is by full
// destination key — never Base() names — so a single-object sync to a
// destination with a different basename can never classify its own
// destination as extra (which previously made --delete remove the object
// the copy had just written).
func buildSyncPlan(srcObjects, dstObjects []*storage.Object, dst *storage.StorageURL, isBatch, dstIsDir bool) (items []syncPlanItem, extras []*storage.Object, errs []error) {
	existing := make(map[string]*storage.Object, len(dstObjects))
	for _, dstObj := range dstObjects {
		existing[syncPlanKey(dstObj.StorageURL)] = dstObj
	}

	written := make(map[string]struct{}, len(srcObjects))
	for _, srcObj := range srcObjects {
		dstURL, err := generateDestinationURL(srcObj.StorageURL, dst, isBatch, dstIsDir)
		if err != nil {
			errs = append(errs, &errorpkg.Error{Op: "sync", Src: srcObj.StorageURL.String(), Err: err})
			continue
		}
		key := syncPlanKey(dstURL)
		written[key] = struct{}{}
		items = append(items, syncPlanItem{
			srcObj: srcObj,
			dstObj: existing[key],
			dstURL: dstURL,
		})
	}

	for _, dstObj := range dstObjects {
		if _, ok := written[syncPlanKey(dstObj.StorageURL)]; !ok {
			extras = append(extras, dstObj)
		}
	}
	return items, extras, errs
}

// queueDelete submits a MultiDelete-friendly delete for a single URL. It
// does not call MultiDelete (which expects a channel); instead it uses
// the per-URL Delete on the store. The waiter aggregates the error.
func (o *Options) queueDelete(ctx context.Context, store *storage.Storage, waiter *parallel.Waiter, url *storage.StorageURL) {
	parallel.Run(func() error {
		if err := store.Delete(ctx, url); err != nil {
			return &errorpkg.Error{Op: "sync", Dst: url.String(), Err: err}
		}
		log.Info(log.InfoMessage{Operation: "rm", Source: url.String()})
		return nil
	}, waiter)
}

// generateDestinationURL resolves the destination URL: for batch sources
// the destination key is the source's relative path under the destination
// prefix; for single-file sources it is the source's base name joined
// to the destination when the destination is a prefix/bucket/directory,
// or the destination itself when it is a full key or a local file path.
// When a remote source is joined onto a local destination the relative
// path is validated so a malicious object key (e.g. "../../x") cannot
// escape the destination directory.
func generateDestinationURL(srcURL, dstURL *storage.StorageURL, isBatch, dstIsDir bool) (*storage.StorageURL, error) {
	objname := srcURL.Base()
	if isBatch {
		objname = srcURL.Relative()
	}
	if dstURL.IsRemote() {
		if dstURL.IsPrefix() || dstURL.IsBucket() {
			return dstURL.Join(objname), nil
		}
		return dstURL.Clone(), nil
	}
	if !isBatch && !dstIsDir {
		// Single-object sync onto an explicit local file path: the
		// destination is the file itself, not <dst>/<basename>. Joining
		// the basename would both write to the wrong path and let
		// --delete classify the real destination as extra.
		return dstURL.Clone(), nil
	}
	if srcURL.IsRemote() {
		if err := storage.EnsureLocalRelPath(srcURL.Path, objname); err != nil {
			return nil, err
		}
	}
	return dstURL.Join(objname), nil
}

// --- S3 -> S3 ---

func (o *Options) syncS3ToS3(ctx context.Context, store *storage.Storage, src, dst *storage.StorageURL) error {
	srcIsPrefix := src.IsBucket() || src.IsPrefix()
	if srcIsPrefix && !(dst.IsBucket() || dst.IsPrefix()) {
		return fmt.Errorf("destination must be a prefix when source is a prefix")
	}

	srcObjects, err := o.listObjects(ctx, store, src, false, false)
	if err != nil {
		return err
	}
	dstObjects, err := o.listDestObjects(ctx, store, dst, false)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsPrefix, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			md := o.sharedMetadata()
			md.Directive = cliutil.MetadataDirectiveReplace
			if err := store.Copy(ctx, srcURL, dstURL, md); err != nil {
				return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dstURL.String(), Err: err}
			}
			log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.String(), Destination: dstURL.String()})
			return nil
		}
	})
}

// --- S3 -> local ---

func (o *Options) syncS3ToLocal(ctx context.Context, store *storage.Storage, src, dst *storage.StorageURL) error {
	srcIsPrefix := src.IsBucket() || src.IsPrefix()
	if srcIsPrefix {
		isDir, err := cliutil.IsLocalDir(dst.Absolute())
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when source is a prefix")
		}
	}

	srcObjects, err := o.listObjects(ctx, store, src, false, false)
	if err != nil {
		return err
	}
	// The local "list" walks the destination directory.
	dstObjects, err := o.listDestObjects(ctx, store, dst, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsPrefix, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			if err := store.DownloadFile(ctx, srcURL.Bucket, srcURL.Path, dstURL.Absolute(), o.Shared.Concurrency, o.Shared.PartSizeBytes()); err != nil {
				return &errorpkg.Error{Op: "cp", Src: srcURL.String(), Dst: dstURL.Absolute(), Err: err}
			}
			log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.String(), Destination: dstURL.Absolute()})
			return nil
		}
	})
}

// --- local -> S3 ---

func (o *Options) syncLocalToS3(ctx context.Context, store *storage.Storage, src, dst *storage.StorageURL) error {
	srcIsDir, err := cliutil.IsLocalDir(src.Absolute())
	if err != nil {
		return err
	}
	if srcIsDir && !(dst.IsBucket() || dst.IsPrefix()) {
		return fmt.Errorf("destination must be a prefix when source is a directory")
	}

	srcObjects, err := o.listObjects(ctx, store, src, !o.Shared.NoFollowSymlinks, false)
	if err != nil {
		return err
	}
	dstObjects, err := o.listDestObjects(ctx, store, dst, false)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsDir, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			_, err := store.UploadFile(ctx, srcURL.Absolute(), dstURL.Bucket, dstURL.Path, o.Shared.Concurrency, o.Shared.PartSizeBytes())
			if err != nil {
				return &errorpkg.Error{Op: "cp", Src: srcURL.Absolute(), Dst: dstURL.String(), Err: err}
			}
			log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.Absolute(), Destination: dstURL.String()})
			return nil
		}
	})
}

// --- local -> local ---

func (o *Options) syncLocalToLocal(ctx context.Context, store *storage.Storage, src, dst *storage.StorageURL) error {
	srcIsDir, err := cliutil.IsLocalDir(src.Absolute())
	if err != nil {
		return err
	}
	if srcIsDir {
		isDir, err := cliutil.IsLocalDir(dst.Absolute())
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when source is a directory")
		}
	}

	srcObjects, err := o.listObjects(ctx, store, src, !o.Shared.NoFollowSymlinks, false)
	if err != nil {
		return err
	}
	dstObjects, err := o.listDestObjects(ctx, store, dst, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsDir, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			if err := store.Copy(ctx, srcURL, dstURL, storage.Metadata{}); err != nil {
				return &errorpkg.Error{Op: "cp", Src: srcURL.Absolute(), Dst: dstURL.Absolute(), Err: err}
			}
			log.Info(log.InfoMessage{Operation: "cp", Source: srcURL.Absolute(), Destination: dstURL.Absolute()})
			return nil
		}
	})
}

// sharedMetadata assembles a storage.Metadata from the SharedFlags. It is
// the single source of truth for sync's cp tasks so the metadata fields
// never drift between the S3->S3 / local->S3 paths.
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

// listDestObjects collects the destination objects for the plan. A single
// remote destination key is Stat'ed directly instead of listing its parent
// prefix: the parent listing inherited the destination URL's filter (so it
// returned exactly the key the sync was about to overwrite, which --delete
// then raced), and for keys under a sub-prefix the delimiter listing only
// returned a CommonPrefix so the existing destination was never seen at
// all. A missing destination is an empty listing, not an error: every
// first-time sync writes to a destination that does not exist yet.
func (o *Options) listDestObjects(ctx context.Context, store *storage.Storage, dst *storage.StorageURL, followSymlinks bool) ([]*storage.Object, error) {
	if dst.IsRemote() && !dst.IsBucket() && !dst.IsPrefix() {
		obj, err := store.Stat(ctx, dst)
		if err != nil {
			if errorpkg.IsWarning(err) {
				return nil, nil
			}
			return nil, err
		}
		return []*storage.Object{obj}, nil
	}
	return o.listObjects(ctx, store, dst, followSymlinks, true)
}

// --- sync strategy ---

// syncStrategy is the interface that decides whether a common source and
// destination object should be re-copied.
type syncStrategy interface {
	ShouldSync(src, dst *storage.Object) error
}

// newStrategy returns the strategy selected by the --size-only flag.
func newStrategy(sizeOnly bool) syncStrategy {
	if sizeOnly {
		return &sizeOnlyStrategy{}
	}
	return &sizeAndModificationStrategy{}
}

// sizeOnlyStrategy copies when the sizes differ.
type sizeOnlyStrategy struct{}

func (s *sizeOnlyStrategy) ShouldSync(src, dst *storage.Object) error {
	if src.Size == dst.Size {
		return errorpkg.ErrObjectSizesMatch
	}
	return nil
}

// sizeAndModificationStrategy copies when the source is newer or the
// sizes differ.
type sizeAndModificationStrategy struct{}

func (s *sizeAndModificationStrategy) ShouldSync(src, dst *storage.Object) error {
	srcMod, dstMod := src.ModTime, dst.ModTime
	if srcMod != nil && dstMod != nil && srcMod.After(*dstMod) {
		return nil
	}
	if src.Size != dst.Size {
		return nil
	}
	return errorpkg.ErrObjectIsNewerAndSizesMatch
}

// Compile-time assertion that the strategies satisfy the interface.
var (
	_ syncStrategy = (*sizeOnlyStrategy)(nil)
	_ syncStrategy = (*sizeAndModificationStrategy)(nil)
)
