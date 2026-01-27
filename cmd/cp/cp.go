package cp

import (
	"context"
	"fmt"
	"os"

	"github.com/LinPr/s6cmd/storage"
	fsstore "github.com/LinPr/s6cmd/storage/fs"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/storage/uri"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewCpCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "cp [flags] source destination",
		Short: "copy file or files from source to destination",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if err := o.complete(cmd, args); err != nil {
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

	return &cmd
}

type Args struct {
	SrcS3Uri  string `validate:"required"`
	DestS3Uri string `validate:"required"`
}
type Flags struct {
	//TODO: 全局flag 需要移动到 root 中
	Debug         bool
	EndpointUrl   string
	NoVerifySSL   bool
	NoPaginate    bool
	Output        string
	Profile       string
	Region        string
	Recursive     bool
	Summarize     string
	HumanReadable bool
	PageSize      int32
	PathStyle     bool
}

type Options struct {
	Args
	Flags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	if len(args) >= 2 {
		o.SrcS3Uri = args[0]
		o.DestS3Uri = args[1]
	}
	// Get persistent flags from parent command
	if cmd.Parent() != nil {
		parentFlags := cmd.Parent().PersistentFlags()

		if parentFlags.Lookup("debug") != nil {
			o.Debug, _ = parentFlags.GetBool("debug")
		}
		if parentFlags.Lookup("endpoint-url") != nil {
			o.EndpointUrl, _ = parentFlags.GetString("endpoint-url")
		}
		if parentFlags.Lookup("no-verify-ssl") != nil {
			o.NoVerifySSL, _ = parentFlags.GetBool("no-verify-ssl")
		}
		if parentFlags.Lookup("no-paginate") != nil {
			o.NoPaginate, _ = parentFlags.GetBool("no-paginate")
		}
		if parentFlags.Lookup("output") != nil {
			o.Output, _ = parentFlags.GetString("output")
		}
		if parentFlags.Lookup("profile") != nil {
			o.Profile, _ = parentFlags.GetString("profile")
		}
		if parentFlags.Lookup("region") != nil {
			o.Region, _ = parentFlags.GetString("region")
		}
		if parentFlags.Lookup("path-style") != nil {
			o.PathStyle, _ = parentFlags.GetBool("path-style")
		}

	}
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}

	return nil
}

func (o *Options) run() error {

	s3Opt := s3store.S3Option{
		UsePathStyle: o.PathStyle,
		Region:       o.Region,
		Profile:      o.Profile,

		NoVerifySSL: o.NoVerifySSL,
	}

	storageOpt := storage.NewStorageOption(s3Opt, fsstore.LocalOption{})

	s, err := storage.NewStorage(context.TODO(), *storageOpt)
	if err != nil {
		return err
	}

	parsedSrcS3Uri, err := uri.ParseS3Uri(o.SrcS3Uri)
	if err != nil {
		return err
	}

	parsedDestS3Uri, err := uri.ParseS3Uri(o.DestS3Uri)
	if err != nil {
		return err
	}

	if er := s.CopyS3Objects(context.TODO(), parsedSrcS3Uri, parsedDestS3Uri); err != nil {
		return er
	}
	return nil
}
