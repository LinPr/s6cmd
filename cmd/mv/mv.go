// Package mv implements the `s6cmd mv` command. The mv command structure
// is cp + delete-source, and uses cobra + aws-sdk-go-v2 + the s6cmd
// parallel.Manager/Waiter framework.
//
// mv is structurally cp+delete: each source object is transferred via the
// same cliutil.TransferSpec plumbing cp uses (so --concurrency /
// --part-size / --metadata / --storage-class / --acl / --sse / --exclude /
// --include / --no-follow-symlinks all work), and each source is removed
// only after ITS OWN transfer succeeded. Tracking success per object (as
// opposed to the previous "upload a snapshot of the file list, then
// os.RemoveAll the source tree") closes a data-loss window: files created
// between the source walk and the delete phase, and files whose transfer
// failed, are left in place so a re-run does not lose data.
//
// Tasks run on the global parallel.Manager; errors are aggregated from the
// Waiter's error channel and returned as a single errors.Join error so the
// RunE contract surfaces every failure instead of just the first.
package mv

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/internal/progressbar"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewMvCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "mv [flags] <source> <destination>",
		Short:   "move objects between source and destination",
		Example: mv_examples,
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

	// mv-specific flags.
	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the move and print one line per operation without transferring or deleting anything")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "move prefix/bucket/directory sources recursively (required for such sources)")
	// --jobs is kept as a backwards-compatible alias for SharedFlags's
	// --concurrency. The SharedFlags value is the one the transfer path
	// reads; when the user passes --jobs we copy it into Shared.Concurrency
	// in complete() so legacy scripts that use --jobs keep working. When
	// both are passed, --concurrency wins (it is the more specific knob).
	cmd.Flags().IntVarP(&o.Jobs, "jobs", "j", 0, "number of concurrent operations (alias for --concurrency; --concurrency wins when both are set)")

	// Shared flags: --concurrency, --part-size, --acl, --metadata, ...
	o.Shared.AddToCmd(&cmd)

	return &cmd
}

type Args struct {
	Source      string `validate:"required"`
	Destination string `validate:"required"`
}

// Flags holds the mv-specific flags. Shared flags live in SharedFlags.
type Flags struct {
	DryRun    bool
	Recursive bool
	// Jobs is the legacy --jobs flag. When non-zero it is copied into
	// Shared.Concurrency in complete() unless the user passed
	// --concurrency explicitly. It does not drive the transfer path
	// directly.
	Jobs int

	// CommonFlags holds the global flags inherited from the parent
	// command (endpoint, region, profile, retry-count, ...). It is
	// populated in complete() so run() does not have to touch cobra
	// again.
	cliutil.CommonFlags
}

// Options is the closure of Args + Flags + SharedFlags.
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
	// storage call becomes a no-op while the plan/list/filter phases run
	// for real. The local delete phase checks the flag directly.
	o.CommonFlags.DryRun = o.DryRun

	// --jobs is a backwards-compatible alias for --concurrency. When the
	// user did not pass --concurrency explicitly but did pass --jobs,
	// propagate the value so legacy scripts keep working. --concurrency
	// always wins because it is the more specific knob.
	if !cmd.Flags().Changed("concurrency") && o.Jobs > 0 {
		o.Shared.Concurrency = o.Jobs
	}
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
// transfer primitives. mv does not expose the override flags
// (--no-clobber & friends), so ShouldOverride always allows the transfer.
func (o *Options) spec() *cliutil.TransferSpec {
	return &cliutil.TransferSpec{
		Op:     "mv",
		DryRun: o.DryRun,
		Shared: o.Shared,
	}
}

func (o *Options) run(ctx context.Context) error {
	srcURL, err := storage.NewStorageURL(o.Source, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	destURL, err := storage.NewStorageURL(o.Destination, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	if destURL.IsWildcard() {
		return fmt.Errorf("target %q can not contain glob characters", o.Destination)
	}

	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	// Local->local move is a plain rename (with a copy fallback across
	// filesystems); it never touches S3.
	if !srcURL.IsRemote() && !destURL.IsRemote() {
		return o.moveLocalToLocal(srcURL.Path, destURL.Path)
	}

	// Multi-object sources require an explicit --recursive (mirroring cp
	// and rm): mv additionally DELETES the source, so a typo'd mv on a
	// prefix must not wipe it by accident. Wildcard sources stay allowed
	// without the flag — the pattern itself is an explicit multi-object
	// request.
	if err := o.checkRecursive(srcURL); err != nil {
		return err
	}

	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Exclude)
	if err != nil {
		return err
	}
	includePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Shared.Include)
	if err != nil {
		return err
	}

	srcIsLocalDir := false
	if !srcURL.IsRemote() && !srcURL.IsWildcard() {
		srcIsLocalDir, err = cliutil.IsLocalDir(srcURL.Absolute())
		if err != nil {
			return err
		}
	}
	isBatch := srcURL.IsWildcard() || srcIsLocalDir ||
		(srcURL.IsRemote() && (srcURL.IsBucket() || srcURL.IsPrefix()))

	// Destination sanity: a multi-object move needs a prefix/directory
	// destination, otherwise every object would land on the same key.
	if isBatch {
		if destURL.IsRemote() && !(destURL.IsBucket() || destURL.IsPrefix()) {
			return fmt.Errorf("destination must be a bucket or prefix when moving multiple objects")
		}
	}

	objects, err := cliutil.ExpandSource(ctx, store, srcURL, !o.Shared.NoFollowSymlinks)
	if err != nil {
		return err
	}

	pb := progressbar.New(false)
	pb.Start()
	defer pb.Finish()

	waiter := parallel.NewWaiter()
	ec := cliutil.NewErrorCollector("mv")
	drainDone := ec.Drain(waiter)

	// moved collects the source URLs whose transfer succeeded; only those
	// are deleted afterwards. Tasks append concurrently, hence the mutex.
	var (
		movedMu sync.Mutex
		moved   []*storage.StorageURL
	)

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
		// For a local directory source mv moves the directory's CONTENTS
		// (mv dir s3://b/ produces s3://b/<rel>, not s3://b/dir/<rel>),
		// matching the previous behaviour; cp mirrors the base name
		// instead. Re-derive the relative path against the source dir.
		if srcIsLocalDir {
			if rel, relErr := filepath.Rel(srcURL.Absolute(), object.StorageURL.Absolute()); relErr == nil {
				object.StorageURL.SetRelativePath(filepath.ToSlash(rel))
			}
		}
		name := object.StorageURL.Relative()
		if name == "" {
			name = object.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}

		srcObj := object.StorageURL
		task := func() error {
			var terr error
			switch {
			case srcURL.IsRemote() && destURL.IsRemote():
				dst := cliutil.PrepareRemoteDestination(srcObj, destURL, false, isBatch)
				if srcObj.Bucket == dst.Bucket && srcObj.Path == dst.Path {
					// Moving an object onto itself would copy then delete
					// it; skip it instead.
					return nil
				}
				if terr = spec.Copy(ctx, store, srcObj, dst); terr != nil {
					terr = &errorpkg.Error{Op: "mv", Src: srcObj.String(), Dst: dst.String(), Err: terr}
				}
			case srcURL.IsRemote() && !destURL.IsRemote():
				dst, derr := cliutil.PrepareLocalDestination(ctx, store, srcObj, destURL, false, isBatch)
				if derr != nil {
					return &errorpkg.Error{Op: "mv", Src: srcObj.String(), Dst: destURL.String(), Err: derr}
				}
				if terr = spec.Download(ctx, store, srcObj, dst, pb); terr != nil {
					terr = &errorpkg.Error{Op: "mv", Src: srcObj.String(), Dst: dst.Absolute(), Err: terr}
				}
			case !srcURL.IsRemote() && destURL.IsRemote():
				dst := cliutil.PrepareRemoteDestination(srcObj, destURL, false, isBatch)
				if terr = spec.Upload(ctx, store, srcObj, dst, pb); terr != nil {
					terr = &errorpkg.Error{Op: "mv", Src: srcObj.Absolute(), Dst: dst.String(), Err: terr}
				}
			default:
				return fmt.Errorf("unsupported mv pair: src=%v dst=%v", srcURL, destURL)
			}
			if terr != nil {
				// Warnings (skipped transfers) propagate so the collector
				// logs them at debug level; either way the source is NOT
				// recorded as moved and stays in place.
				return terr
			}
			movedMu.Lock()
			moved = append(moved, srcObj)
			movedMu.Unlock()
			return nil
		}
		parallel.Run(task, waiter)
	}
	waiter.Wait()
	drainDone()

	// Delete-source phase: remove exactly the sources whose transfer
	// succeeded (mv semantics). Failed or skipped transfers keep their
	// source so a re-run can pick them up.
	ec.Collect(o.deleteMovedSources(ctx, store, srcURL, srcIsLocalDir, moved))

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
			return fmt.Errorf("source %q is a bucket/prefix (use --recursive)", o.Source)
		}
		return nil
	}
	isDir, err := cliutil.IsLocalDir(srcURL.Absolute())
	if err != nil {
		return err
	}
	if isDir {
		return fmt.Errorf("source %q is a directory (use --recursive)", o.Source)
	}
	return nil
}

// deleteMovedSources removes exactly the sources whose transfer succeeded.
// Remote sources are deleted in batched DeleteObjects calls (the store
// no-ops them under dry-run); local sources are removed per file, then
// now-empty directories under a directory source are pruned. Files that
// appeared after the source listing — or whose transfer failed — are left
// untouched, so a partial mv never destroys data that was not transferred.
func (o *Options) deleteMovedSources(ctx context.Context, store *storage.Storage, src *storage.StorageURL, srcIsLocalDir bool, moved []*storage.StorageURL) error {
	if len(moved) == 0 {
		return nil
	}
	if src.IsRemote() {
		keys := make([]string, 0, len(moved))
		for _, u := range moved {
			keys = append(keys, u.Path)
		}
		return cliutil.DeleteS3KeysInBatches(ctx, store, src.Bucket, keys)
	}

	// Local source: never delete anything under dry-run (the transfers
	// above were store-level no-ops).
	if o.DryRun {
		return nil
	}
	var errs []error
	for _, u := range moved {
		if err := os.Remove(u.Absolute()); err != nil {
			errs = append(errs, err)
		}
	}
	if srcIsLocalDir {
		pruneEmptyDirs(src.Absolute())
	}
	return errors.Join(errs...)
}

// pruneEmptyDirs removes the directories under root (and root itself)
// that are empty after the per-file deletes, children first. Directories
// that still contain files — e.g. files created between the source
// listing and the delete phase, or files whose transfer failed — are left
// in place: os.Remove refuses to remove a non-empty directory, which is
// exactly the guard the data-safety contract relies on.
func pruneEmptyDirs(root string) {
	var dirs []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	// WalkDir visits parents before children; delete in reverse so leaf
	// directories go first.
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i])
	}
}

// moveLocalToLocal renames srcPath to destPath with POSIX mv semantics:
// when destPath is an existing directory the source is moved INTO it
// (destPath/Base(srcPath)) rather than having its contents merged into the
// directory, which is what the previous implementation did.
func (o *Options) moveLocalToLocal(srcPath, destPath string) error {
	srcIsDir, err := cliutil.IsLocalDir(srcPath)
	if err != nil {
		return err
	}
	destIsDir, err := cliutil.IsLocalDir(destPath)
	if err != nil {
		return err
	}
	if destIsDir {
		destPath = filepath.Join(destPath, filepath.Base(filepath.Clean(srcPath)))
	}
	if o.DryRun {
		// The filesystem store is bypassed here, so honour --dry-run
		// explicitly: report the rename that would happen and stop.
		log.Info(log.InfoMessage{Operation: "mv", Source: srcPath, Destination: destPath})
		return nil
	}
	if err := os.Rename(srcPath, destPath); err != nil {
		// os.Rename across filesystems returns EXDEV or a link error;
		// fall back to copy+remove so mv still works on cross-mount
		// paths.
		if fbErr := crossDeviceFallback(srcPath, destPath, srcIsDir); fbErr != nil {
			return fmt.Errorf("rename failed (%v): %v", err, fbErr)
		}
		return nil
	}
	return nil
}

// crossDeviceFallback implements mv's copy+delete fallback when os.Rename
// fails (typically cross-mount). It matches the cp+delete semantics mv
// uses for S3: only the files that were actually copied are removed — a
// blanket os.RemoveAll(src) would also destroy files created between the
// copy walk and the delete, which were never transferred anywhere.
func crossDeviceFallback(srcPath, destPath string, srcIsDir bool) error {
	copied, copyErr := copyLocalTree(srcPath, destPath)
	if copyErr != nil {
		return fmt.Errorf("fallback copy failed (%v)", copyErr)
	}
	if !srcIsDir {
		return os.Remove(srcPath)
	}
	var errs []error
	for _, p := range copied {
		if rmErr := os.Remove(p); rmErr != nil {
			errs = append(errs, rmErr)
		}
	}
	// Prune now-empty directories (children first). os.Remove refuses to
	// remove a non-empty directory, so any directory still holding an
	// un-copied file survives — exactly the data-safety guard the
	// per-file delete provides.
	pruneEmptyDirs(srcPath)
	return errors.Join(errs...)
}

// copyLocalTree is the cross-filesystem fallback for moveLocalToLocal. It
// is deliberately minimal: walk src, mkdirAll + copy each file to dst. It
// is only used when os.Rename fails (typically cross-mount), so the hot
// path is unchanged. It returns the source paths of the files it copied so
// the caller can delete exactly those and nothing else.
func copyLocalTree(srcPath, destPath string) ([]string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		if err := cliutil.CopyLocalFile(srcPath, destPath); err != nil {
			return nil, err
		}
		return []string{srcPath}, nil
	}
	if err := os.MkdirAll(destPath, 0o755); err != nil {
		return nil, err
	}
	var copied []string
	err = filepath.Walk(srcPath, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcPath, p)
		if err != nil {
			return err
		}
		dst := filepath.Join(destPath, rel)
		if fi.IsDir() {
			return os.MkdirAll(dst, fi.Mode())
		}
		if err := cliutil.CopyLocalFile(p, dst); err != nil {
			return err
		}
		copied = append(copied, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return copied, nil
}
