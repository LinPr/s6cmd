package put

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: put %s %s\n", o.localFile, o.S3Uri)
				return nil
			}
			return o.run(ctx)
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "upload objects recursively")
	cmd.Flags().IntVarP(&o.Jobs, "jobs", "j", 1, "number of concurrent operations")

	return &cmd
}

type Args struct {
	localFile string `validate:"omitempty"`
	S3Uri     string `validate:"omitempty"`
}
type Flags struct {
	DryRun      bool `json:"DryRun" yaml:"DryRun"`
	EndpointUrl string
	NoVerifySSL bool
	NoPaginate  bool
	Output      string
	Profile     string
	Region      string
	PathStyle   bool
	Recursive   bool
	Jobs        int
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
		_, err = store.UploadFromStdin(ctx, parsedDest.Bucket, parsedDest.Path)
		return err
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

	return uploadLocalToS3(ctx, store, srcURL, destURL, o.Recursive, o.Jobs)
}

func isLocalDir(path string) (bool, error) {
	return cliutil.IsLocalDir(path)
}

func listLocalFiles(src string, recursive bool) ([]string, error) {
	return cliutil.ListLocalFiles(src, recursive)
}

func uploadLocalToS3(ctx context.Context, store *storage.Storage, src, dest *storage.StorageURL, recursive bool, jobs int) error {
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
			_, err := store.UploadFile(ctx, uploadPath, dest.Bucket, uploadKey)
			return err
		})
	}

	return cliutil.RunTasks(jobs, tasks)
}
