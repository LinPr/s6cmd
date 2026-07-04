package get

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: get %s %s\n", o.S3Uri, o.FsPath)
				return nil
			}
			return o.run(ctx)
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "download objects recursively")
	cmd.Flags().IntVarP(&o.Jobs, "jobs", "j", 1, "number of concurrent operations")

	return &cmd
}

type Args struct {
	S3Uri  string `validate:"omitempty"`
	FsPath string `validate:"omitempty"`
}
type Flags struct {
	DryRun       bool `json:"DryRun" yaml:"DryRun"`
	EndpointUrl  string
	NoVerifySSL  bool
	NoPaginate   bool
	Output       string
	Profile      string
	Region       string
	PathStyle    bool
	Recursive    bool
	Jobs         int
}

type Options struct {
	Args
	Flags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command) error {
	flags := cliutil.LoadParentFlags(cmd)
	o.EndpointUrl = flags.EndpointURL
	o.NoVerifySSL = flags.NoVerifySSL
	o.NoPaginate = flags.NoPaginate
	o.Output = flags.Output
	o.Profile = flags.Profile
	o.Region = flags.Region
	o.PathStyle = flags.PathStyle
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

	store, err := cliutil.NewStorage(ctx, cliutil.CommonFlags{
		EndpointURL: o.EndpointUrl,
		NoVerifySSL: o.NoVerifySSL,
		NoPaginate:  o.NoPaginate,
		Output:      o.Output,
		Profile:     o.Profile,
		Region:      o.Region,
		PathStyle:   o.PathStyle,
	})
	if err != nil {
		return err
	}

	return downloadS3ToLocal(ctx, store, srcURL, destURL, o.Recursive, o.Jobs)
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

func downloadS3ToLocal(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, recursive bool, jobs int) error {
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
				destPath = filepath.Join(dest.Path, filepath.FromSlash(src.Relative()))
			}
		case src.IsPrefix() || src.IsBucket():
			rel := strings.TrimPrefix(key, srcPrefix)
			rel = strings.TrimPrefix(rel, "/")
			destPath = filepath.Join(dest.Path, filepath.FromSlash(rel))
		default:
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
		if destPath == "" {
			continue
		}
		dlKey := key
		dlPath := destPath
		tasks = append(tasks, func() error {
			return store.DownloadFile(ctx, src.Bucket, dlKey, dlPath)
		})
	}

	return cliutil.RunTasks(jobs, tasks)
}
