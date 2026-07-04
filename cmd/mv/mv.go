// Package mv implements the `s6cmd mv` command. The mv command structure
// is cp + delete-source, and uses cobra + aws-sdk-go-v2 + the s6cmd
// parallel.Manager/Waiter framework.
//
// mv is structurally cp+delete: each source object is copied to the
// destination via the same SharedFlags-driven path (so --concurrency /
// --part-size / --metadata / --storage-class / --acl / --sse / --exclude /
// --include / ... all work), and the source is removed only after the
// copy/download/upload phase has completed with no failures. If any task
// errors, the source is left intact so a re-run does not lose data.
//
// Tasks run on the global parallel.Manager; errors are aggregated from the
// Waiter's error channel and returned as a single errors.Join error so the
// RunE contract surfaces every failure instead of just the first.
package mv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: mv %s %s\n", o.Source, o.Destination)
				return nil
			}
			return o.run(ctx)
		},
	}

	// mv-specific flags.
	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "move objects recursively")
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

func (o *Options) run(ctx context.Context) error {
	srcURL, err := storage.NewStorageURL(o.Source, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}
	destURL, err := storage.NewStorageURL(o.Destination, storage.WithRaw(o.Shared.Raw))
	if err != nil {
		return err
	}

	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	switch {
	case srcURL.IsRemote() && destURL.IsRemote():
		return moveS3ToS3(ctx, store, srcURL, destURL, o)
	case srcURL.IsRemote() && !destURL.IsRemote():
		return moveS3ToLocal(ctx, store, srcURL, destURL, o)
	case !srcURL.IsRemote() && destURL.IsRemote():
		return moveLocalToS3(ctx, store, srcURL, destURL, o)
	default:
		return moveLocalToLocal(srcURL.Path, destURL.Path)
	}
}

// runParallel drains the Waiter's error channel into errs while the caller
// submits tasks via parallel.Run. It returns cliutil.AggregateErrors(errs)
// so a single mv invocation surfaces every failure instead of just the
// first. The caller is responsible for building the task list and calling
// parallel.Run for each task; runParallel handles the drain goroutine and
// the post-Wait aggregation.
//
// The submit callback runs synchronously on the caller's goroutine so the
// errs slice is only touched from the drain goroutine, avoiding the race
// the previous RunTasks-based implementation had when the delete-source
// phase read a map written from worker goroutines.
func runParallel(submit func(waiter *parallel.Waiter) error) error {
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
				log.Debug(log.DebugMessage{Operation: "mv", Err: err.Error()})
				continue
			}
			log.Error(log.ErrorMessage{Operation: "mv", Err: err.Error()})
			errs = append(errs, err)
		}
	}()

	// If submit returns an error before scheduling any task, surface it
	// directly; the waiter has nothing to drain in that case.
	if err := submit(waiter); err != nil {
		waiter.Wait()
		<-errDoneCh
		return cliutil.AggregateErrors(append(errs, err))
	}

	waiter.Wait()
	<-errDoneCh
	return cliutil.AggregateErrors(errs)
}

// moveS3ToS3 implements S3-to-S3 move as Copy + Delete-source, matching the
// mv semantics (the previous implementation only copied, leaving the source
// object in place, which is cp behaviour, not mv).
func moveS3ToS3(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, o *Options) error {
	srcIsPrefix := src.IsBucket() || src.IsPrefix()
	srcPrefix := src.Path
	if srcIsPrefix {
		srcPrefix = cliutil.NormalizeRemotePrefix(srcPrefix)
	}

	if srcIsPrefix && !(dest.IsBucket() || dest.IsPrefix()) {
		return fmt.Errorf("destination must be a prefix when source is a prefix")
	}

	destPrefix := dest.Path
	if srcIsPrefix || dest.IsPrefix() || dest.IsBucket() {
		destPrefix = cliutil.NormalizeRemotePrefix(destPrefix)
	}

	keys := []string{src.Path}
	if srcIsPrefix {
		var err error
		keys, err = store.ListS3Keys(ctx, src.Bucket, srcPrefix)
		if err != nil {
			return err
		}
	}

	type copyOp struct {
		srcKey  string
		destKey string
	}
	ops := make([]copyOp, 0, len(keys))
	for _, key := range keys {
		var destKey string
		if srcIsPrefix {
			rel := strings.TrimPrefix(key, srcPrefix)
			rel = strings.TrimPrefix(rel, "/")
			destKey = destPrefix + rel
		} else if dest.IsBucket() || dest.IsPrefix() {
			destKey = destPrefix + path.Base(key)
		} else {
			destKey = dest.Path
		}

		if src.Bucket == dest.Bucket && key == destKey {
			continue
		}
		ops = append(ops, copyOp{srcKey: key, destKey: destKey})
	}

	// Copy phase on the parallel.Manager. The delete-source phase only
	// runs when the copy phase had no errors, so a partial failure leaves
	// the source intact.
	if err := runParallel(func(waiter *parallel.Waiter) error {
		for _, op := range ops {
			op := op
			parallel.Run(func() error {
				if err := store.CopyS3Object(ctx, src.Bucket, op.srcKey, dest.Bucket, op.destKey); err != nil {
					return &errorpkg.Error{Op: "mv", Src: "s3://" + src.Bucket + "/" + op.srcKey, Dst: "s3://" + dest.Bucket + "/" + op.destKey, Err: err}
				}
				log.Info(log.InfoMessage{Operation: "mv", Source: "s3://" + src.Bucket + "/" + op.srcKey, Destination: "s3://" + dest.Bucket + "/" + op.destKey})
				return nil
			}, waiter)
		}
		return nil
	}); err != nil {
		return err
	}

	// Delete-source phase (mv semantics: copy succeeded, now remove source).
	srcKeys := make([]string, 0, len(ops))
	for _, op := range ops {
		srcKeys = append(srcKeys, op.srcKey)
	}
	return cliutil.DeleteS3KeysInBatches(ctx, store, src.Bucket, srcKeys)
}

func moveS3ToLocal(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, o *Options) error {
	srcIsPrefix := src.IsBucket() || src.IsPrefix()
	srcPrefix := src.Path
	if srcIsPrefix {
		srcPrefix = cliutil.NormalizeRemotePrefix(srcPrefix)
	}

	if srcIsPrefix {
		isDir, err := cliutil.IsLocalDir(dest.Path)
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when source is a prefix")
		}
	}

	keys := []string{src.Path}
	if srcIsPrefix {
		var err error
		keys, err = store.ListS3Keys(ctx, src.Bucket, srcPrefix)
		if err != nil {
			return err
		}
	}

	type downloadOp struct {
		key      string
		destPath string
	}
	ops := make([]downloadOp, 0, len(keys))
	for _, key := range keys {
		var destPath string
		if srcIsPrefix {
			rel := strings.TrimPrefix(key, srcPrefix)
			rel = strings.TrimPrefix(rel, "/")
			destPath = filepath.Join(dest.Path, filepath.FromSlash(rel))
		} else {
			isDir, err := cliutil.IsLocalDir(dest.Path)
			if err != nil {
				return err
			}
			if isDir {
				destPath = filepath.Join(dest.Path, path.Base(key))
			} else {
				destPath = dest.Path
			}
		}
		ops = append(ops, downloadOp{key: key, destPath: destPath})
	}

	if err := runParallel(func(waiter *parallel.Waiter) error {
		for _, op := range ops {
			op := op
			parallel.Run(func() error {
				if err := store.DownloadFile(ctx, src.Bucket, op.key, op.destPath); err != nil {
					return &errorpkg.Error{Op: "mv", Src: "s3://" + src.Bucket + "/" + op.key, Dst: op.destPath, Err: err}
				}
				log.Info(log.InfoMessage{Operation: "mv", Source: "s3://" + src.Bucket + "/" + op.key, Destination: op.destPath})
				return nil
			}, waiter)
		}
		return nil
	}); err != nil {
		return err
	}

	// Delete source objects after successful download (mv semantics).
	srcKeys := make([]string, 0, len(ops))
	for _, op := range ops {
		srcKeys = append(srcKeys, op.key)
	}
	return cliutil.DeleteS3KeysInBatches(ctx, store, src.Bucket, srcKeys)
}

func moveLocalToS3(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, o *Options) error {
	files, err := cliutil.ListLocalFiles(src.Path, o.Recursive)
	if err != nil {
		return err
	}

	srcIsDir, err := cliutil.IsLocalDir(src.Path)
	if err != nil {
		return err
	}
	if srcIsDir && !(dest.IsBucket() || dest.IsPrefix()) {
		return fmt.Errorf("destination must be a prefix when source is a directory")
	}

	destPrefix := dest.Path
	if srcIsDir || dest.IsBucket() || dest.IsPrefix() {
		destPrefix = cliutil.NormalizeRemotePrefix(destPrefix)
	}

	type uploadOp struct {
		localPath string
		destKey   string
	}
	ops := make([]uploadOp, 0, len(files))
	for _, filePath := range files {
		var destKey string
		if srcIsDir {
			rel, err := filepath.Rel(src.Path, filePath)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			destKey = destPrefix + rel
		} else if dest.IsBucket() || dest.IsPrefix() {
			destKey = destPrefix + filepath.Base(filePath)
		} else {
			destKey = dest.Path
		}
		ops = append(ops, uploadOp{localPath: filePath, destKey: destKey})
	}

	if err := runParallel(func(waiter *parallel.Waiter) error {
		for _, op := range ops {
			op := op
			parallel.Run(func() error {
				if _, err := store.UploadFile(ctx, op.localPath, dest.Bucket, op.destKey); err != nil {
					return &errorpkg.Error{Op: "mv", Src: op.localPath, Dst: "s3://" + dest.Bucket + "/" + op.destKey, Err: err}
				}
				log.Info(log.InfoMessage{Operation: "mv", Source: op.localPath, Destination: "s3://" + dest.Bucket + "/" + op.destKey})
				return nil
			}, waiter)
		}
		return nil
	}); err != nil {
		return err
	}

	// Remove local source (mv semantics). Only run when every upload
	// succeeded; a partial failure leaves the source files in place.
	if srcIsDir {
		return os.RemoveAll(src.Path)
	}
	return os.Remove(src.Path)
}

func moveLocalToLocal(srcPath, destPath string) error {
	srcIsDir, err := cliutil.IsLocalDir(srcPath)
	if err != nil {
		return err
	}
	if srcIsDir {
		isDir, err := cliutil.IsLocalDir(destPath)
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when source is a directory")
		}
	}
	if err := os.Rename(srcPath, destPath); err != nil {
		// os.Rename across filesystems returns EXDEV or a link error;
		// fall back to copy+remove so mv still works on cross-mount
		// paths. This matches the cp+delete semantics mv uses for S3.
		if copyErr := copyLocalTree(srcPath, destPath); copyErr != nil {
			return fmt.Errorf("rename failed (%v) and fallback copy failed (%v)", err, copyErr)
		}
		if srcIsDir {
			return os.RemoveAll(srcPath)
		}
		return os.Remove(srcPath)
	}
	return nil
}

// copyLocalTree is the cross-filesystem fallback for moveLocalToLocal. It
// is deliberately minimal: walk src, mkdirAll + copy each file to dst. It
// is only used when os.Rename fails (typically cross-mount), so the hot
// path is unchanged.
func copyLocalTree(srcPath, destPath string) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return copyFile(srcPath, destPath)
	}
	if err := os.MkdirAll(destPath, 0o755); err != nil {
		return err
	}
	return filepath.Walk(srcPath, func(p string, fi os.FileInfo, err error) error {
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
		return copyFile(p, dst)
	})
}

// copyFile is a plain io.Copy with MkdirAll on the destination's dir. It
// is the local-only fallback; S3 uploads go through store.UploadFile.
func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer to.Close()
	_, err = io.Copy(to, from)
	return err
}
