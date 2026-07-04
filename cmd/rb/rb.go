package rb

import (
	"context"
	"fmt"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: would remove bucket %s\n", o.S3Uri)
				return nil
			}
			return o.run(ctx)
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be removed")
	cmd.Flags().BoolVarP(&o.Force, "force", "f", false, "empty bucket before removal")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"required"`
}

type Flags struct {
	EndpointUrl string
	NoVerifySSL bool
	NoPaginate  bool
	Output      string
	Profile     string
	Region      string
	PathStyle   bool
	Force       bool
	DryRun      bool
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

	if o.Force {
		keys, err := store.ListS3Keys(ctx, url.Bucket, "")
		if err != nil {
			return err
		}
		if err := cliutil.DeleteS3KeysInBatches(ctx, store, url.Bucket, keys); err != nil {
			return err
		}
	}

	return store.DeleteBucket(ctx, url.Bucket)
}
