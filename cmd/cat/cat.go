// Package cat implements the `s6cmd cat` command. It uses cobra + the
// s6cmd storage aggregate + orderedwriter.
//
// A single remote object is streamed to stdout via storage.Get, wrapping
// os.Stdout in an orderedwriter so the multipart downloader's out-of-order
// WriteAt calls are flushed in offset order. Wildcard / prefix / bucket
// sources are enumerated via storage.list and each matching object is
// printed in turn.
package cat

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/orderedwriter"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NewCatCmd creates the `cat` command.
func NewCatCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "cat [flags] <source>",
		Short:   "print remote object content",
		Example: cat_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx, os.Stdout)
		},
	}

	cmd.Flags().BoolVar(&o.Raw, "raw", false, "disable wildcard operations, useful with filenames that contain glob characters")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")
	cmd.Flags().IntVar(&o.Concurrency, "concurrency", cliutil.DefaultCopyConcurrency, "number of concurrent parts transferred between host and remote server")
	cmd.Flags().IntVar(&o.PartSizeMiB, "part-size", cliutil.DefaultPartSizeMiB, "size of each part transferred between host and remote server, in MiB")

	return &cmd
}

// Args holds the positional arguments.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the cat-specific flags.
type Flags struct {
	Raw         bool
	VersionID   string
	Concurrency int
	PartSizeMiB int
}

// Options is the closure of Args + Flags + CommonFlags.
type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	o.S3Uri = args[0]
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID), storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}
	if !src.IsRemote() {
		return errors.New("source must be a remote object")
	}
	if (src.IsWildcard() || src.IsPrefix() || src.IsBucket()) && o.VersionID != "" {
		return errors.New("wildcard/prefix operations are disabled with --version-id flag")
	}
	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}

	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID), storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}

	partSize := cliutil.PartSizeBytesFromMiB(o.PartSizeMiB)
	concurrency := o.Concurrency
	if concurrency <= 0 {
		concurrency = cliutil.DefaultCopyConcurrency
	}

	if src.IsWildcard() || src.IsPrefix() || src.IsBucket() {
		return o.processObjects(ctx, store, src, out, concurrency, partSize)
	}

	// Single-object path: stat first so a missing object surfaces as a
	// clear not-found error rather than an opaque GetObject NoSuchKey.
	if _, err := store.Stat(ctx, src); err != nil {
		return err
	}
	return o.processSingleObject(ctx, store, src, out, concurrency, partSize)
}

// processObjects drains the listing channel and prints each matching object
// in turn. Directory entries (CommonPrefixes) are skipped. The first error
// short-circuits the command.
func (o *Options) processObjects(ctx context.Context, store *storage.Storage, src *storage.StorageURL, out io.Writer, concurrency int, partSize int64) error {
	for obj := range store.List(ctx, src, false) {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.Type.IsDir() {
			continue
		}
		if err := o.processSingleObject(ctx, store, obj.StorageURL, out, concurrency, partSize); err != nil {
			if errorpkg.IsCancelation(err) {
				continue
			}
			return err
		}
	}
	return nil
}

// processSingleObject streams a single remote object to out. os.Stdout is
// wrapped in orderedwriter.New so the multipart downloader's out-of-order
// WriteAt calls are flushed in offset order; without it the chunks would be
// written wherever the downloader happens to land them, producing jumbled
// output on stdout.
func (o *Options) processSingleObject(ctx context.Context, store *storage.Storage, src *storage.StorageURL, out io.Writer, concurrency int, partSize int64) error {
	buf := orderedwriter.New(out)
	if _, err := store.Get(ctx, src, buf, concurrency, partSize); err != nil {
		if errorpkg.IsWarning(err) {
			log.Debug(log.DebugMessage{Operation: "cat", Err: err.Error()})
			return nil
		}
		return err
	}
	return nil
}
