package stat

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewStatCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "stat [flags] <target>",
		Short:   "get object or bucket metadata",
		Example: stat_examples,
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
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: stat %s\n", o.S3Uri)
				return nil
			}
			return o.run(ctx, cmd.OutOrStdout())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"omitempty"`
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

func (o *Options) complete(cmd *cobra.Command) error {
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
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

	if parsedUri.Bucket == "" {
		return fmt.Errorf("bucket name is required")
	}

	if parsedUri.Path == "" {
		return getBucketMetadata(ctx, cli, parsedUri.Bucket, out)
	}

	return getObjectMetadata(ctx, cli, parsedUri.Bucket, parsedUri.Path, out)
}

func getObjectMetadata(ctx context.Context, cli *s3store.S3Store, bucket, key string, out io.Writer) error {
	output, err := cli.HeadObjectOutput(ctx, bucket, key)
	if err != nil {
		return err
	}
	if output == nil {
		return fmt.Errorf("object not found: s3://%s/%s", bucket, key)
	}

	// Canonical key for display.
	fmt.Fprintf(out, "Key: s3://%s/%s\n", bucket, key)
	fmt.Fprintf(out, "ContentLength: %d\n", aws.ToInt64(output.ContentLength))
	if output.LastModified != nil {
		fmt.Fprintf(out, "LastModified: %s\n", output.LastModified.Format(time.RFC3339))
	}
	fmt.Fprintf(out, "ETag: %s\n", trimQuotes(aws.ToString(output.ETag)))
	fmt.Fprintf(out, "ContentType: %s\n", aws.ToString(output.ContentType))
	fmt.Fprintf(out, "StorageClass: %s\n", nonEmpty(string(output.StorageClass), "STANDARD"))
	fmt.Fprintf(out, "VersionID: %s\n", aws.ToString(output.VersionId))
	fmt.Fprintf(out, "CacheControl: %s\n", aws.ToString(output.CacheControl))
	fmt.Fprintf(out, "ContentEncoding: %s\n", aws.ToString(output.ContentEncoding))
	fmt.Fprintf(out, "ContentDisposition: %s\n", aws.ToString(output.ContentDisposition))
	fmt.Fprintf(out, "ServerSideEncryption: %s\n", string(output.ServerSideEncryption))
	if aws.ToString(output.SSEKMSKeyId) != "" {
		fmt.Fprintf(out, "SSEKMSKeyId: %s\n", aws.ToString(output.SSEKMSKeyId))
	}
	if output.Restore != nil {
		fmt.Fprintf(out, "Restore: %s\n", aws.ToString(output.Restore))
	}
	if len(output.Metadata) > 0 {
		fmt.Fprintln(out, "Metadata:")
		for k, v := range output.Metadata {
			fmt.Fprintf(out, "  %s: %s\n", k, v)
		}
	}
	return nil
}

func getBucketMetadata(ctx context.Context, cli *s3store.S3Store, bucket string, out io.Writer) error {
	output, err := cli.HeadBucketOutput(ctx, bucket)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Bucket: s3://%s\n", bucket)
	if output != nil {
		// HeadBucketOutput primarily carries the bucket region via the
		// response's BucketRegion header (exposed on the SDK type as
		// BucketRegion in newer versions). We print whatever the SDK exposes.
		if region := aws.ToString(output.BucketRegion); region != "" {
			fmt.Fprintf(out, "Region: %s\n", region)
		}
	}
	return nil
}

func nonEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func trimQuotes(v string) string {
	for len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		v = v[1 : len(v)-1]
	}
	return v
}
