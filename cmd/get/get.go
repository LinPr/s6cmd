package get

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/LinPr/s6cmd/storage"
	"github.com/LinPr/s6cmd/storage/uri"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewGetCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "get [flags] <target>",
		Short: "get buckets and objects",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				o.S3Uri = args[0]
				o.FsPath = args[1]
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
	S3Uri  string `validate:"omitempty"`
	FsPath string `validate:"omitempty"`
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

	parsedUri, err := uri.ParseS3Uri(o.S3Uri)
	if err != nil {
		return err
	}
	opt := storage.StorageOption{}
	store, err := storage.NewStorage(context.TODO(), opt)
	if err != nil {
		return err
	}

	return store.DownloadFile(context.TODO(), parsedUri.GetBucket(), parsedUri.GetKey(), o.FsPath)
}
