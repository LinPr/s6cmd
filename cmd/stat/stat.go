package stat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/storage/uri"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewStatCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "stat [flags] <target>",
		Short: "get object metadata",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			o.S3Uri = args[0]

			if err := o.complete(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
			if err := o.validate(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
			if err := o.run(); err != nil {
				fmt.Fprintf(os.Stderr, "err: %v\n", err)
				return
			}
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"omitempty"`
}
type Flags struct {
	DryRun bool `json:"DryRun" yaml:"DryRun"`
}

type Options struct {
	Args
	Flags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete() error {
	// 使用 viper 获取到最终生效的配置 flag > env > config > default
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}

	return nil
}

func (o *Options) run() error {
	j, _ := json.Marshal(o)
	fmt.Fprintf(os.Stdout, "options: %s\n", string(j))
	// return nil

	cli, err := s3store.NewS3Client(context.TODO(), s3store.S3Option{})
	if err != nil {
		return err
	}

	parsedUri, err := uri.ParseS3Uri(o.S3Uri)
	if err != nil {
		return err
	}

	if parsedUri.GetBucket() == "" {
		return fmt.Errorf("bucket name is required")
	}

	if parsedUri.GetKey() == "" {
		return getBucketMetadata(cli, parsedUri.GetBucket())
	}

	return getObjectMetadata(cli, parsedUri.GetBucket(), parsedUri.GetKey())
}

func getObjectMetadata(cli *s3store.S3Store, bucket, key string) error {

	headOutput, err := cli.HeadObject(context.TODO(), bucket, key)
	if err != nil {
		return err
	}

	j, _ := json.Marshal(headOutput)
	fmt.Fprintf(os.Stdout, "object metadata: %s\n", string(j))
	return nil
}

func getBucketMetadata(cli *s3store.S3Store, bucket string) error {
	bucketInfo, err := cli.HeadBucket(context.TODO(), bucket)
	if err != nil {
		return err
	}

	j, _ := json.Marshal(bucketInfo)
	fmt.Fprintf(os.Stdout, "bucket metadata: %s\n", string(j))
	return nil
}
