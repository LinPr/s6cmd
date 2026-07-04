package mb

import (
	"context"
	"fmt"
	"io"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewMbCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "mb <s3uri>",
		Short:   "Creates an S3 bucket.",
		Example: mb_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: would create bucket %s\n", o.S3Uri)
				return nil
			}
			return o.run(ctx, cmd.OutOrStdout())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")

	return &cmd
}

type Args struct {
	S3Uri string
}
type Flags struct {
	DryRun bool
}

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
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	s3uri, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if s3uri.Bucket == "" {
		return fmt.Errorf("bucket name is required in s3uri")
	}

	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	opt := s3store.S3Option{
		UsePathStyle: o.common.PathStyle,
		Region:       o.common.Region,
		Profile:      o.common.Profile,
		Endpoint:     o.common.EndpointURL,
		NoVerifySSL:  o.common.NoVerifySSL,
	}
	cli, err := s3store.NewS3Client(ctx, opt)
	if err != nil {
		return err
	}

	parsedUri, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}

	exist, err := cli.BucketExists(ctx, parsedUri.Bucket)
	if err != nil {
		return err
	}
	if exist {
		fmt.Fprintf(out, "Bucket %s already exists.\n", parsedUri.Bucket)
		return nil
	}

	// CreateBucket honours the us-east-1 special case at the storage layer
	// (omitting CreateBucketConfiguration). The region passed here comes
	// either from --region or the SDK default; the storage layer decides
	// whether to include the LocationConstraint.
	if err := cli.CreateBucket(ctx, parsedUri.Bucket, o.common.Region); err != nil {
		return err
	}
	fmt.Fprintf(out, "make_bucket: %s\n", o.S3Uri)
	return nil
}
