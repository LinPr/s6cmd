package rb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewRbCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "rb [flags] <s3uri>",
		Short:   "remove S3 bucket",
		Example: rb_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.S3Uri = args[0]
			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), cmd.InOrStdin(), cmd.ErrOrStderr())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the removal and print what would be deleted without removing anything")
	cmd.Flags().BoolVarP(&o.Force, "force", "f", false, "empty bucket before removal")
	cmd.Flags().BoolVarP(&o.Yes, "yes", "y", false, "skip the confirmation prompt for --force")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"required"`
}

type Flags struct {
	Force  bool
	DryRun bool
	Yes    bool
}

type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command) error {
	o.common = cliutil.LoadParentFlags(cmd)
	// Propagate --dry-run into the store constructors so the purge and
	// DeleteBucket calls become no-ops while listing runs for real.
	o.common.DryRun = o.DryRun
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	return nil
}

func (o *Options) run(ctx context.Context, stdin io.Reader, stderr io.Writer) error {
	url, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if !url.IsRemote() {
		return fmt.Errorf("rb only supports s3:// URLs")
	}
	if url.Bucket == "" {
		return fmt.Errorf("bucket name is required")
	}
	if strings.TrimSpace(url.Path) != "" {
		return fmt.Errorf("rb expects a bucket URL without key")
	}

	// --force destroys every object (and version) in the bucket, so it
	// needs an explicit confirmation: --yes, or an interactive y at the
	// prompt. A dry run deletes nothing and skips the prompt.
	// Non-interactive runs without --yes fail loudly.
	if o.Force && !o.Yes && !o.DryRun {
		fmt.Fprintf(stderr, "WARNING: rb --force will permanently delete every object (and version) in %s and remove the bucket.\n", o.S3Uri)
		if err := cliutil.Confirm(ctx, stdin, stderr, "Continue?"); err != nil {
			return fmt.Errorf("rb --force: %w", err)
		}
	}

	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}

	if o.Force {
		// A versioned bucket cannot be emptied by deleting current keys:
		// each delete only adds a delete marker and DeleteBucket still
		// fails with BucketNotEmpty. Detect versioning (Enabled or
		// Suspended — suspended buckets can still hold old versions) and
		// purge every version and delete marker instead. When the backend
		// does not support GetBucketVersioning, fall back to the plain
		// key deletion; DeleteBucket will surface any leftover state.
		status, vErr := store.GetBucketVersioning(ctx, url.Bucket)
		if vErr == nil && status != "" {
			if err := purgeAllVersions(ctx, store, url.Bucket); err != nil {
				return err
			}
		} else {
			keys, err := store.ListS3Keys(ctx, url.Bucket, "")
			if err != nil {
				return err
			}
			if err := cliutil.DeleteS3KeysInBatches(ctx, store, url.Bucket, keys); err != nil {
				return err
			}
		}
	}

	if err := store.DeleteBucket(ctx, url.Bucket); err != nil {
		return err
	}
	log.Info(log.InfoMessage{Operation: "rb", Source: url.String()})
	return nil
}

// purgeAllVersions deletes every object version and delete marker in the
// bucket so DeleteBucket succeeds on a versioning-enabled (or suspended)
// bucket. Deletion goes through MultiDelete so the keys are removed in
// batches of 1000.
func purgeAllVersions(ctx context.Context, store *storage.Storage, bucket string) error {
	listURL, err := storage.NewStorageURL("s3://"+bucket, storage.WithAllVersions(true))
	if err != nil {
		return err
	}
	// Clear the delimiter so the version listing returns every key under
	// the bucket rather than collapsing sub-prefixes.
	listURL.Delimiter = ""

	// listErrCh carries the first listing error (if any) to the main
	// goroutine; the buffered single slot plus the non-blocking send keeps
	// the producer from stalling on repeated errors.
	listErrCh := make(chan error, 1)
	urlCh := make(chan *storage.StorageURL)
	go func() {
		defer close(urlCh)
		for obj := range store.List(ctx, listURL, false) {
			if obj.Err != nil {
				// An empty bucket yields ErrNoObjectFound, which is fine.
				if !errors.Is(obj.Err, errorpkg.ErrNoObjectFound) {
					select {
					case listErrCh <- obj.Err:
					default:
					}
				}
				continue
			}
			if obj.Type.IsDir() {
				continue
			}
			urlCh <- obj.StorageURL
		}
	}()

	var errs []error
	for obj := range store.MultiDelete(ctx, urlCh) {
		if obj.Err != nil {
			errs = append(errs, obj.Err)
		}
	}
	select {
	case err := <-listErrCh:
		errs = append(errs, err)
	default:
	}
	return cliutil.AggregateErrors(errs)
}
