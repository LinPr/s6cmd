package put

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewPutCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "put [flags] <local-file> <s3uri>",
		Short:   "upload an object to S3 bucket",
		Example: put_examples,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.localFile = args[0]
			o.S3Uri = args[1]

			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the upload and print one line per file without uploading anything")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "upload objects recursively")
	cmd.Flags().IntVarP(&o.Jobs, "jobs", "j", 1, "number of concurrent operations")
	// Same names/semantics as cp's SharedFlags: per-object multipart tuning
	// (as opposed to --jobs, which bounds how many files transfer at once).
	cmd.Flags().IntVar(&o.Concurrency, "concurrency", cliutil.DefaultCopyConcurrency, "number of concurrent parts transferred per file")
	cmd.Flags().IntVar(&o.PartSizeMiB, "part-size", cliutil.DefaultPartSizeMiB, "size of each part transferred per file, in MiB")

	return &cmd
}

type Args struct {
	localFile string `validate:"omitempty"`
	S3Uri     string `validate:"omitempty"`
}
type Flags struct {
	DryRun    bool `json:"DryRun" yaml:"DryRun"`
	Recursive bool
	Jobs      int
	// Concurrency/PartSizeMiB mirror cp's SharedFlags: per-object multipart
	// tuning, converted to bytes via cliutil.PartSizeBytesFromMiB.
	Concurrency int
	PartSizeMiB int
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
	// Propagate --dry-run into the store constructors: file listing runs
	// for real, the Put itself becomes a no-op.
	o.common.DryRun = o.DryRun
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}

	return nil
}

func (o *Options) run(ctx context.Context) error {
	if o.localFile == "-" {
		if o.Recursive {
			return fmt.Errorf("cannot use --recursive with stdin")
		}
		parsedDest, err := storage.NewStorageURL(o.S3Uri)
		if err != nil {
			return err
		}
		if !parsedDest.IsRemote() {
			return fmt.Errorf("destination must be s3:// when using stdin")
		}

		store, err := cliutil.NewStorage(ctx, o.common)
		if err != nil {
			return err
		}
		if _, err := store.UploadFromStdin(ctx, parsedDest.Bucket, parsedDest.Path, o.Concurrency, cliutil.PartSizeBytesFromMiB(o.PartSizeMiB)); err != nil {
			return err
		}
		log.Info(log.InfoMessage{Operation: "put", Source: "-", Destination: parsedDest.String()})
		return nil
	}

	srcURL, err := storage.NewStorageURL(o.localFile)
	if err != nil {
		return err
	}
	destURL, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if !destURL.IsRemote() {
		return fmt.Errorf("put destination must be s3://")
	}

	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}

	return uploadLocalToS3(ctx, store, srcURL, destURL, o.Recursive, o.Jobs, o.Concurrency, cliutil.PartSizeBytesFromMiB(o.PartSizeMiB))
}

func isLocalDir(path string) (bool, error) {
	return cliutil.IsLocalDir(path)
}

func listLocalFiles(src string, recursive bool) ([]string, error) {
	return cliutil.ListLocalFiles(src, recursive)
}

func uploadLocalToS3(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, recursive bool, jobs, concurrency int, partSize int64) error {
	files, err := listLocalFiles(src.Path, recursive)
	if err != nil {
		return err
	}

	if len(files) > 1 && !(dest.IsPrefix() || dest.IsBucket()) {
		return fmt.Errorf("destination must be a prefix when uploading multiple sources")
	}

	srcBase := src.Path
	if strings.ContainsAny(src.Path, "?*") {
		srcBase = cliutil.WildcardBasePath(src.Path)
	}

	destPrefix := dest.Path
	if dest.IsPrefix() || dest.IsBucket() {
		destPrefix = cliutil.NormalizeRemotePrefix(destPrefix)
	}

	tasks := make([]func() error, 0, len(files))
	for _, filePath := range files {
		var destKey string
		if dest.IsPrefix() || dest.IsBucket() || len(files) > 1 {
			rel, err := filepath.Rel(srcBase, filePath)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			destKey = destPrefix + rel
		} else {
			destKey = dest.Path
		}
		uploadPath := filePath
		uploadKey := destKey
		tasks = append(tasks, func() error {
			if _, err := store.UploadFile(ctx, uploadPath, dest.Bucket, uploadKey, concurrency, partSize); err != nil {
				return err
			}
			log.Info(log.InfoMessage{Operation: "put", Source: uploadPath, Destination: "s3://" + dest.Bucket + "/" + uploadKey})
			return nil
		})
	}

	return cliutil.RunTasks(jobs, tasks)
}
