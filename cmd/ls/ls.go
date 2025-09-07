package ls

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

func NewLsCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "ls [flags] <target>",
		Short: "list buckets and objects",
		// Args:  cobra.ExactArgs(1),
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

	cli, err := s3store.NewS3Client(context.TODO())
	if err != nil {
		return err
	}

	if o.S3Uri == "" {
		return listBuckets(cli)
	}

	parsedUri, err := uri.ParseS3Url(o.S3Uri)
	if err != nil {
		return err
	}
	if parsedUri.GetBucket() == "" {
		return listBuckets(cli)
	}

	return listObjects(cli, o.S3Uri)
}

func listBuckets(cli *s3store.S3Store) error {
	buckets, err := cli.ListBuckets(context.TODO())
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		fmt.Printf("\t%v\n", *bucket.Name)
	}
	return nil
}

func listObjects(cli *s3store.S3Store, s3uri string) error {
	objs, err := cli.ListObjects(context.TODO(), s3uri)
	if err != nil {
		return err
	}
	for _, obj := range objs.Contents {
		fmt.Printf("\t%v\n", *obj.Key)
	}
	return nil
}
