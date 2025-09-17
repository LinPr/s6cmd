package du

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

func NewDuCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "du [flags] <target>",
		Short: "calculate total size of bucket or objects",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				o.S3Uri = args[0]
			}
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
	DryRun bool    `json:"DryRun" yaml:"DryRun"`
	Region *string `json:"Region" yaml:"Region"`
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

	opt := s3store.S3Option{
		Region: *o.Region,
	}

	cli, err := s3store.NewS3Client(context.TODO(), opt)
	if err != nil {
		return err
	}

	parsedUri, err := uri.ParseS3Uri(o.S3Uri)
	if err != nil {
		return err
	}
	if parsedUri.GetBucket() == "" {
		return fmt.Errorf("bucket is required")
	}

	return listObjects(cli, parsedUri.GetBucket(), parsedUri.GetKey())
}

func listObjects(cli *s3store.S3Store, bucket, key string) error {
	objs, err := cli.ListObjectsWithPagination(context.TODO(), bucket, key)
	if err != nil {
		return err
	}
	var size int64

	for _, obj := range objs {
		size += *obj.Size
	}
	unit := "bytes"
	if size > 1024 {
		size = size / 1024
		unit = "KB"
	}
	if size > 1024 {
		size = size / 1024
		unit = "MB"
	}
	if size > 1024 {
		size = size / 1024
		unit = "GB"
	}
	fmt.Printf("Total size: %d %s\n", size, unit)
	return nil
}
