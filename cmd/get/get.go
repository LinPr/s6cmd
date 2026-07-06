package get

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewGetCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "get [flags] <source> <dest>",
		Short:   "download objects from S3 to local",
		Example: get_examples,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.S3Uri = args[0]
			o.FsPath = args[1]
			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "plan the download and print one line per object without writing any local file")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "download objects recursively")
	cmd.Flags().IntVarP(&o.Jobs, "jobs", "j", 1, "number of concurrent operations")
	// Same names/semantics as cp's SharedFlags: per-object multipart tuning
	// (as opposed to --jobs, which bounds how many objects transfer at once).
	cmd.Flags().IntVar(&o.Concurrency, "concurrency", cliutil.DefaultCopyConcurrency, "number of concurrent parts transferred per object")
	cmd.Flags().IntVar(&o.PartSizeMiB, "part-size", cliutil.DefaultPartSizeMiB, "size of each part transferred per object, in MiB")

	return &cmd
}

type Args struct {
	S3Uri  string `validate:"omitempty"`
	FsPath string `validate:"omitempty"`
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
	// Propagate --dry-run into the store constructors: listing runs for
	// real, DownloadFile becomes a no-op that never creates local files.
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
	srcURL, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	destURL, err := storage.NewStorageURL(o.FsPath)
	if err != nil {
		return err
	}

	if !srcURL.IsRemote() {
		return fmt.Errorf("get source must be s3://")
	}

	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}

	return downloadS3ToLocal(ctx, store, srcURL, destURL, o.Recursive, o.Jobs, o.Concurrency, cliutil.PartSizeBytesFromMiB(o.PartSizeMiB))
}

func listS3KeysForGet(ctx context.Context, store *storage.Storage, src *storage.StorageURL, recursive bool) ([]string, string, error) {
	if src.IsWildcard() {
		keys, err := store.ListS3Keys(ctx, src.Bucket, src.Prefix)
		if err != nil {
			return nil, "", err
		}
		filtered := make([]string, 0, len(keys))
		for _, key := range keys {
			if src.Match(key) {
				filtered = append(filtered, key)
			}
		}
		return filtered, src.Prefix, nil
	}

	if src.IsBucket() || src.IsPrefix() {
		if !recursive {
			return nil, "", fmt.Errorf("source is a prefix (use --recursive)")
		}
		prefix := cliutil.NormalizeRemotePrefix(src.Path)
		keys, err := store.ListS3Keys(ctx, src.Bucket, prefix)
		if err != nil {
			return nil, "", err
		}
		return keys, prefix, nil
	}

	return []string{src.Path}, src.Path, nil
}

func downloadS3ToLocal(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, recursive bool, jobs, concurrency int, partSize int64) error {
	keys, srcPrefix, err := listS3KeysForGet(ctx, store, src, recursive)
	if err != nil {
		return err
	}

	if src.IsPrefix() || src.IsBucket() || src.IsWildcard() {
		isDir, err := cliutil.IsLocalDir(dest.Path)
		if err != nil {
			return err
		}
		if !isDir {
			return fmt.Errorf("destination must be a directory when source is a prefix or wildcard")
		}
	}

	tasks := make([]func() error, 0, len(keys))
	for _, key := range keys {
		var destPath string
		switch {
		case src.IsWildcard():
			if src.Match(key) {
				rel := src.Relative()
				if err := storage.EnsureLocalRelPath(key, rel); err != nil {
					return err
				}
				destPath = filepath.Join(dest.Path, filepath.FromSlash(rel))
			}
		case src.IsPrefix() || src.IsBucket():
			rel := strings.TrimPrefix(key, srcPrefix)
			rel = strings.TrimPrefix(rel, "/")
			if err := storage.EnsureLocalRelPath(key, rel); err != nil {
				return err
			}
			destPath = filepath.Join(dest.Path, filepath.FromSlash(rel))
		default:
			isDir, err := cliutil.IsLocalDir(dest.Path)
			if err != nil {
				return err
			}
			if isDir {
				base := path.Base(key)
				if err := storage.EnsureLocalRelPath(key, base); err != nil {
					return err
				}
				destPath = filepath.Join(dest.Path, base)
			} else {
				destPath = dest.Path
			}
		}
		if destPath == "" {
			continue
		}
		dlKey := key
		dlPath := destPath
		tasks = append(tasks, func() error {
			if err := store.DownloadFile(ctx, src.Bucket, dlKey, dlPath, concurrency, partSize); err != nil {
				return err
			}
			log.Info(log.InfoMessage{Operation: "get", Source: "s3://" + src.Bucket + "/" + dlKey, Destination: dlPath})
			return nil
		})
	}

	return cliutil.RunTasks(jobs, tasks)
}
