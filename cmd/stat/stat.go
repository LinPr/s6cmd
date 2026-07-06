package stat

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/strutil"
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
			return o.run(cmd.Context(), cmd.OutOrStdout())
		},
	}

	// stat is read-only; --dry-run is accepted for interface consistency
	// with the mutating commands but has no effect.
	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "no effect: stat is read-only (accepted for consistency)")

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
	cli, err := cliutil.NewS3Client(ctx, o.common)
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

	jsonOutput := o.common.Output == "json"
	if parsedUri.Path == "" {
		return getBucketMetadata(ctx, cli, parsedUri.Bucket, jsonOutput, out)
	}

	return getObjectMetadata(ctx, cli, parsedUri.Bucket, parsedUri.Path, jsonOutput, out)
}

func getObjectMetadata(ctx context.Context, cli *s3store.S3Store, bucket, key string, jsonOutput bool, out io.Writer) error {
	output, err := cli.HeadObjectOutput(ctx, bucket, key)
	if err != nil {
		return err
	}
	if output == nil {
		return fmt.Errorf("object not found: s3://%s/%s", bucket, key)
	}

	if jsonOutput {
		msg := statObjectMessage{
			Key:                  fmt.Sprintf("s3://%s/%s", bucket, key),
			ContentLength:        aws.ToInt64(output.ContentLength),
			LastModified:         output.LastModified,
			ETag:                 strutil.TrimQuotes(aws.ToString(output.ETag)),
			ContentType:          aws.ToString(output.ContentType),
			StorageClass:         nonEmpty(string(output.StorageClass), "STANDARD"),
			VersionID:            aws.ToString(output.VersionId),
			CacheControl:         aws.ToString(output.CacheControl),
			ContentEncoding:      aws.ToString(output.ContentEncoding),
			ContentDisposition:   aws.ToString(output.ContentDisposition),
			ServerSideEncryption: string(output.ServerSideEncryption),
			SSEKMSKeyID:          aws.ToString(output.SSEKMSKeyId),
			Restore:              aws.ToString(output.Restore),
			Metadata:             output.Metadata,
		}
		fmt.Fprintln(out, msg.JSON())
		return nil
	}

	// Canonical key for display.
	fmt.Fprintf(out, "Key: s3://%s/%s\n", bucket, key)
	fmt.Fprintf(out, "ContentLength: %d\n", aws.ToInt64(output.ContentLength))
	if output.LastModified != nil {
		fmt.Fprintf(out, "LastModified: %s\n", output.LastModified.Format(time.RFC3339))
	}
	fmt.Fprintf(out, "ETag: %s\n", strutil.TrimQuotes(aws.ToString(output.ETag)))
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

func getBucketMetadata(ctx context.Context, cli *s3store.S3Store, bucket string, jsonOutput bool, out io.Writer) error {
	output, err := cli.HeadBucketOutput(ctx, bucket)
	if err != nil {
		return err
	}
	region := ""
	if output != nil {
		// HeadBucketOutput primarily carries the bucket region via the
		// response's BucketRegion header (exposed on the SDK type as
		// BucketRegion in newer versions). We print whatever the SDK exposes.
		region = aws.ToString(output.BucketRegion)
	}
	if jsonOutput {
		fmt.Fprintln(out, statBucketMessage{Bucket: "s3://" + bucket, Region: region}.JSON())
		return nil
	}
	fmt.Fprintf(out, "Bucket: s3://%s\n", bucket)
	if region != "" {
		fmt.Fprintf(out, "Region: %s\n", region)
	}
	return nil
}

// statObjectMessage is the JSON payload for object metadata when
// --output json is set.
type statObjectMessage struct {
	Key                  string            `json:"key"`
	ContentLength        int64             `json:"size"`
	LastModified         *time.Time        `json:"last_modified,omitempty"`
	ETag                 string            `json:"etag,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	StorageClass         string            `json:"storage_class,omitempty"`
	VersionID            string            `json:"version_id,omitempty"`
	CacheControl         string            `json:"cache_control,omitempty"`
	ContentEncoding      string            `json:"content_encoding,omitempty"`
	ContentDisposition   string            `json:"content_disposition,omitempty"`
	ServerSideEncryption string            `json:"server_side_encryption,omitempty"`
	SSEKMSKeyID          string            `json:"sse_kms_key_id,omitempty"`
	Restore              string            `json:"restore,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty"`
}

func (m statObjectMessage) String() string { return m.JSON() }
func (m statObjectMessage) JSON() string   { return strutil.JSON(m) }

// statBucketMessage is the JSON payload for bucket metadata when
// --output json is set.
type statBucketMessage struct {
	Bucket string `json:"bucket"`
	Region string `json:"region,omitempty"`
}

func (m statBucketMessage) String() string { return m.JSON() }
func (m statBucketMessage) JSON() string   { return strutil.JSON(m) }

func nonEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
