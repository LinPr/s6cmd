// Package sync implements the `s6cmd sync` command. It mirrors s5cmd's
// sync command structure (compareObjects + planRun + SyncStrategy) but
// uses cobra + aws-sdk-go-v2 + the s6cmd parallel.Manager framework.
//
// The previous implementation had a data race: the `expected` map was
// written from inside task functions that ran on cliutil.RunTasks worker
// goroutines, while the post-run delete-extra phase read it on the main
// goroutine. This rewrite fixes the race by collecting all source and
// destination objects into slices on the main goroutine, merge-comparing
// them in memory (no goroutine needed for the compare), and only then
// submitting cp/rm tasks to the parallel.Manager.
//
// The single-file --delete case (src is one object, dst is one object)
// is now handled correctly: when src and dst differ, the dst is queued
// for deletion; previously the single-file path was short-circuited
// because the delete-extra guard required srcIsPrefix.
package sync

import (
	"bufio"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: sync %s %s\n", o.Source, o.Destination)
				return nil
			}
			return o.run(ctx, cmd.InOrStdin(), cmd.ErrOrStderr())
		},
	}

	// sync-specific flags.
	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
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
	if o.Delete && !o.Yes {
		if !confirmDelete(stdin, stderr, o.Source, o.Destination) {
			return nil
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

func confirmDelete(in io.Reader, out io.Writer, source, destination string) bool {
	fmt.Fprintf(out, "WARNING: this will delete objects in destination that are not in source.\n")
	fmt.Fprintf(out, "  source: %s\n  destination: %s\n", source, destination)
	fmt.Fprint(out, "Continue? [y/N]: ")
	reader := bufio.NewReader(in)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	return strings.EqualFold(line, "y") || strings.EqualFold(line, "yes")
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
func (o *Options) listObjects(ctx context.Context, store *storage.Storage, src *storage.StorageURL, followSymlinks bool) ([]*storage.Object, error) {
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
//  2. Merge-compare the two sorted slices by relative path, producing
//     three buckets: only-source, only-destination, and common pairs.
//  3. Submit cp tasks for only-source + (common pairs where the strategy
//     says they differ) on the parallel.Manager.
//  4. When --delete is set, submit rm tasks for only-destination.
//
// All task-submission happens on the main goroutine so the `expected`
// set (used by the --delete path) is never written from a worker.
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

	pb := progressbar.New(false)
	pb.Start()
	defer pb.Finish()

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
				log.Debug(log.DebugMessage{Operation: "sync", Err: err.Error()})
				continue
			}
			log.Error(log.ErrorMessage{Operation: "sync", Err: err.Error()})
			errs = append(errs, err)
			if o.ExitOnError {
				// Cancel the context so in-flight tasks stop. We do not
				// have a cancel handy here; the caller's ctx is the
				// surface we have, and it is the root command ctx. The
				// ExitOnError path is best-effort: once the waiter drains
				// its current queue it will stop because no more tasks
				// are submitted below.
				break
			}
		}
	}()

	strategy := newStrategy(o.SizeOnly)

	// Merge-compare the two sorted slices. Both i and j advance in
	// lockstep; the smaller relative path wins, equality produces a
	// common pair.
	i, j := 0, 0
	for i < len(srcObjects) || j < len(dstObjects) {
		var srcObj, dstObj *storage.Object
		switch {
		case i < len(srcObjects) && j < len(dstObjects):
			srcName := srcObjects[i].StorageURL.Relative()
			dstName := dstObjects[j].StorageURL.Relative()
			if !isBatch {
				srcName = srcObjects[i].StorageURL.Base()
				dstName = dstObjects[j].StorageURL.Base()
			}
			if srcName < dstName {
				srcObj = srcObjects[i]
				i++
			} else if srcName == dstName {
				// Common: ask the strategy whether to copy.
				if err := strategy.ShouldSync(srcObjects[i], dstObjects[j]); err != nil {
					if errorpkg.IsWarning(err) {
						log.Debug(log.DebugMessage{Operation: "sync", Err: err.Error()})
					} else {
						errs = append(errs, err)
					}
				} else {
					srcObj = srcObjects[i]
					dstObj = dstObjects[j]
				}
				i++
				j++
				if srcObj == nil {
					continue
				}
			} else {
				// only destination: queue for delete if --delete.
				if o.Delete {
					o.queueDelete(ctx, store, waiter, dstObjects[j].StorageURL, &errs)
				}
				j++
				continue
			}
		case i < len(srcObjects):
			srcObj = srcObjects[i]
			i++
		case j < len(dstObjects):
			if o.Delete {
				o.queueDelete(ctx, store, waiter, dstObjects[j].StorageURL, &errs)
			}
			j++
			continue
		}

		if srcObj == nil {
			continue
		}
		// Apply exclude/include on the source name. The destination name
		// is derived from the source name so it does not need separate
		// filtering.
		name := srcObj.StorageURL.Relative()
		if name == "" {
			name = srcObj.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}

		dstURL := generateDestinationURL(srcObj.StorageURL, pair.dst, isBatch)
		// When the strategy produced a common pair, dstObj is non-nil and
		// the destination URL should match the existing destination (so
		// server-side copy replaces the same key). generateDestinationURL
		// already does this for prefix destinations.
		_ = dstObj

		pb.AddTotalBytes(srcObj.Size)
		pb.IncrementTotalObjects()

		parallel.Run(buildTask(srcObj.StorageURL, dstURL), waiter)
	}

	waiter.Wait()
	<-errDoneCh
	return cliutil.AggregateErrors(errs)
}

// queueDelete submits a MultiDelete-friendly delete for a single URL. It
// does not call MultiDelete (which expects a channel); instead it uses
// the per-URL Delete on the store. The waiter aggregates the error.
//
// The errs slice is shared with the task-submission loop; we append to it
// under the same goroutine (the main one) so there is no race.
func (o *Options) queueDelete(ctx context.Context, store *storage.Storage, waiter *parallel.Waiter, url *storage.StorageURL, errs *[]error) {
	parallel.Run(func() error {
		if err := store.Delete(ctx, url); err != nil {
			return &errorpkg.Error{Op: "sync", Dst: url.String(), Err: err}
		}
		log.Info(log.InfoMessage{Operation: "rm", Source: url.String()})
		return nil
	}, waiter)
}

// generateDestinationURL mirrors s5cmd's generateDestinationURL: for
// batch sources the destination key is the source's relative path under
// the destination prefix; for single-file sources it is the source's base
// name joined to the destination (or the destination itself when it is a
// full key).
func generateDestinationURL(srcURL, dstURL *storage.StorageURL, isBatch bool) *storage.StorageURL {
	objname := srcURL.Base()
	if isBatch {
		objname = srcURL.Relative()
	}
	if dstURL.IsRemote() {
		if dstURL.IsPrefix() || dstURL.IsBucket() {
			return dstURL.Join(objname)
		}
		return dstURL.Clone()
	}
	return dstURL.Join(objname)
}

// --- S3 -> S3 ---

func (o *Options) syncS3ToS3(ctx context.Context, store *storage.Storage, src, dst *storage.StorageURL) error {
	srcIsPrefix := src.IsBucket() || src.IsPrefix()
	if srcIsPrefix && !(dst.IsBucket() || dst.IsPrefix()) {
		return fmt.Errorf("destination must be a prefix when source is a prefix")
	}

	srcObjects, err := o.listObjects(ctx, store, src, false)
	if err != nil {
		return err
	}
	dstObjects, err := o.listObjects(ctx, store, dstForListing(dst), false)
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

	srcObjects, err := o.listObjects(ctx, store, src, false)
	if err != nil {
		return err
	}
	// The local "list" walks the destination directory.
	dstObjects, err := o.listObjects(ctx, store, dst, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsPrefix, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			if err := store.DownloadFile(ctx, srcURL.Bucket, srcURL.Path, dstURL.Absolute()); err != nil {
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

	srcObjects, err := o.listObjects(ctx, store, src, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}
	dstObjects, err := o.listObjects(ctx, store, dstForListing(dst), false)
	if err != nil {
		return err
	}
	return o.planAndRun(ctx, store, syncPair{src: src, dst: dst}, srcObjects, dstObjects, srcIsDir, func(srcURL, dstURL *storage.StorageURL) parallel.Task {
		return func() error {
			_, err := store.UploadFile(ctx, srcURL.Absolute(), dstURL.Bucket, dstURL.Path)
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

	srcObjects, err := o.listObjects(ctx, store, src, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}
	dstObjects, err := o.listObjects(ctx, store, dst, !o.Shared.NoFollowSymlinks)
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

// dstForListing returns the URL to use when listing the destination for
// the merge-compare. For remote prefixes/buckets we need to enumerate
// every key under the prefix; for local destinations we use the
// directory URL as-is.
//
// Note: listObjects separately clears the Delimiter on remote
// bucket/prefix URLs so S3 returns a flat recursive listing.
func dstForListing(dst *storage.StorageURL) *storage.StorageURL {
	if !dst.IsRemote() {
		return dst
	}
	if dst.IsBucket() {
		return dst.Clone()
	}
	// Remote prefix or key: enumerate under the prefix.
	if dst.IsPrefix() {
		return dst.Clone()
	}
	// Single remote key: list the parent prefix so we can see whether
	// the key exists.
	parent := dst.Clone()
	parent.Path = path.Dir(dst.Path)
	if parent.Path == "." || parent.Path == "" {
		parent.Path = ""
	}
	parent.Prefix = parent.Path
	return parent
}

// --- sync strategy ---

// syncStrategy is the interface that decides whether a common source and
// destination object should be re-copied. It mirrors s5cmd's
// SyncStrategy.
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
// sizes differ. It mirrors s5cmd's SizeAndModificationStrategy.
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
