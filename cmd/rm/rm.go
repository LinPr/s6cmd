// Package rm implements the `s6cmd rm` command. The rm command structure
// is expandRmSources -> storage.MultiDelete, and uses cobra +
// aws-sdk-go-v2 + the s6cmd storage aggregate.
//
// The command supports:
//   - --exclude / --include wildcard patterns (repeatable)
//   - --version-id for deleting a specific object version
//   - --all-versions for deleting every version of an object
//   - --raw to disable wildcard expansion (useful for keys with glob chars)
//
// Deletion runs via storage.MultiDelete, which batches keys 1000 at a time
// (the S3 DeleteObjects limit) and returns a per-URL result channel. The
// main goroutine drains that channel and logs each result; errors are
// aggregated into a single errors.Join return so the command surfaces
// every failure instead of just the first.
package rm

import (
	"context"
	"fmt"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NewRmCmd creates the `rm` command. It registers the rm-specific flags
// (--version-id/--all-versions/--raw/--exclude/--include) plus the dry-run
// and recursive flags kept for backwards compatibility with prior s6cmd
// releases.
func NewRmCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "rm [flags] <s3uri>",
		Short:   "remove S3 objects",
		Example: rm_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.S3Uri = args[0]
			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the removal and print one line per object without deleting anything")
	cmd.Flags().BoolVar(&o.Recursive, "recursive", false, "remove objects recursively under a prefix")
	cmd.Flags().BoolVar(&o.AllVersions, "all-versions", false, "list all versions of object(s)")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")
	cmd.Flags().BoolVar(&o.Raw, "raw", false, "disable wildcard operations, useful with filenames that contains glob characters")
	cmd.Flags().StringSliceVar(&o.Exclude, "exclude", nil, "exclude objects with given pattern (repeatable)")
	cmd.Flags().StringSliceVar(&o.Include, "include", nil, "include objects with given pattern (repeatable)")

	return &cmd
}

// Args holds the positional argument.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the rm-specific flags plus the CommonFlags inherited from
// the parent command.
type Flags struct {
	DryRun      bool
	Recursive   bool
	AllVersions bool
	VersionID   string
	Raw         bool
	Exclude     []string
	Include     []string
	cliutil.CommonFlags
}

type Options struct {
	Args
	Flags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command) error {
	o.CommonFlags = cliutil.LoadParentFlags(cmd)
	// Propagate --dry-run into the store constructors: listing and
	// filtering run for real, MultiDelete becomes a per-URL no-op that
	// still reports each key, so the command prints exactly what a real
	// run would delete.
	o.CommonFlags.DryRun = o.DryRun
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	return nil
}

func (o *Options) run(ctx context.Context) error {
	url, err := storage.NewStorageURL(o.S3Uri,
		storage.WithVersion(o.VersionID),
		storage.WithAllVersions(o.AllVersions),
		storage.WithRaw(o.Raw),
	)
	if err != nil {
		return err
	}
	if !url.IsRemote() {
		return fmt.Errorf("rm only supports s3:// URLs")
	}
	if url.Bucket == "" {
		return fmt.Errorf("bucket name is required")
	}
	// rm operates on objects, not buckets. A bare bucket or prefix URL is
	// only valid with --recursive (which expands to every object under
	// the prefix) so we reject it here to surface a clear error.
	if (url.IsBucket() || url.IsPrefix()) && !o.Recursive {
		return fmt.Errorf("refusing to remove bucket/prefix without --recursive")
	}
	// --recursive on a prefix/bucket means "every object under it, across
	// all sub-prefixes". Clear the delimiter so ListObjectsV2 does not
	// collapse sub-prefixes into CommonPrefixes (which would hide nested
	// keys from the deletion set).
	if o.Recursive && (url.IsBucket() || url.IsPrefix()) {
		url.Delimiter = ""
	}

	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Exclude)
	if err != nil {
		return err
	}
	includePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Include)
	if err != nil {
		return err
	}

	// Collect source objects into a slice first so we can drive the
	// MultiDelete channel from a single producer goroutine. The slice is
	// bounded by the number of objects under the prefix.
	objects, err := expandRmSources(ctx, store, url)
	if err != nil {
		return err
	}

	// Split listing failures from deletable URLs on the main goroutine
	// before the producer starts: the errs slice is also appended to by the
	// result-channel drain below, so collecting listing errors here keeps
	// every append on one goroutine. Listing errors join the aggregated
	// exit-code errors — they used to be logged only, which let a failed
	// listing exit 0. AggregateErrors still drops warning sentinels, so a
	// wildcard that matches nothing keeps exiting 0.
	errs := make([]error, 0)
	deletable := make([]*storage.StorageURL, 0, len(objects))
	for _, obj := range objects {
		if obj.Err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}
			log.Error(log.ErrorMessage{Operation: "rm", Err: obj.Err.Error()})
			errs = append(errs, obj.Err)
			continue
		}
		if obj.Type.IsDir() {
			continue
		}
		name := obj.StorageURL.Relative()
		if name == "" {
			name = obj.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}
		deletable = append(deletable, obj.StorageURL)
	}

	// Build the URL channel consumed by MultiDelete. We feed it from a
	// goroutine so MultiDelete's batching goroutine can start draining
	// immediately.
	urlCh := make(chan *storage.StorageURL)
	go func() {
		defer close(urlCh)
		for _, url := range deletable {
			urlCh <- url
		}
	}()

	// MultiDelete returns a per-URL result channel. Drain it on the main
	// goroutine so the log output is ordered and so we can aggregate
	// errors into the final return.
	resultCh := store.MultiDelete(ctx, urlCh)
	for obj := range resultCh {
		if obj.Err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}
			log.Error(log.ErrorMessage{Operation: "rm", Err: obj.Err.Error()})
			errs = append(errs, obj.Err)
			continue
		}
		log.Info(log.InfoMessage{Operation: "rm", Source: obj.String()})
	}

	return cliutil.AggregateErrors(errs)
}

// expandRmSources materializes the list of objects to delete. For a single
// non-prefix URL it returns a one-element slice; otherwise it drains the
// channel returned by storage.List.
//
// The function deliberately returns a slice rather than a channel so the
// caller can iterate it without spawning a second consumer goroutine,
// which would race with the result-channel drain on the errs slice.
//
// When --recursive is set on a prefix/bucket URL, the URL's Delimiter is
// cleared so ListObjectsV2 returns every object under the prefix rather
// than collapsing sub-prefixes into CommonPrefixes.
func expandRmSources(ctx context.Context, store *storage.Storage, src *storage.StorageURL) ([]*storage.Object, error) {
	single := !src.IsWildcard() && !src.IsBucket() && !src.IsPrefix()

	// Single object: stat first so we surface a clear "not found" error
	// before invoking MultiDelete. With --all-versions we must list
	// instead: Stat only sees the current version (and 404s when the
	// latest entry is a delete marker), while ListObjectVersions returns
	// every version plus the markers.
	if single && !src.AllVersions {
		obj, err := store.Stat(ctx, src)
		if err != nil {
			return nil, err
		}
		return []*storage.Object{obj}, nil
	}

	out := make([]*storage.Object, 0, 64)
	for obj := range store.List(ctx, src, false) {
		// ListObjectVersions is prefix-based, so for a single-key
		// --all-versions URL it also returns sibling keys that share the
		// prefix (e.g. "logs" matches "logs2"). Keep only exact key
		// matches in that case.
		if single && obj.Err == nil && obj.StorageURL != nil && obj.StorageURL.Path != src.Path {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}
