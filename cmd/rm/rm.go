package rm

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

func NewRmCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "rm [flags] <bucket-name>",
		Short: "remove S3 bucket",
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
	cmd.Flags().StringVarP(&o.Region, "region", "r", "", "specify the region to create the bucket in")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"required"`
}
type Flags struct {
	DryRun bool   `json:"DryRun" yaml:"DryRun"`
	Region string `json:"Region" yaml:"Region"`
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
	fmt.Println("rb called")
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

	if parsedUri.GetKey() == "" {
		return deleteBucket(cli, parsedUri.GetBucket())
	}

	// o.Region = "cn-east-3"
	return deleteObjects(cli, parsedUri.GetBucket(), parsedUri.GetKey())
}

func deleteBucket(ccli *s3store.S3Store, bucket string) error {
	return ccli.DeleteBucket(context.TODO(), bucket)
}

func deleteObjects(ccli *s3store.S3Store, bucket string, key string) error {
	return ccli.DeleteObjects(context.TODO(), bucket, []string{key})
}
