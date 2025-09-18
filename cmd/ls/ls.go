package ls

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/storage/uri"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NOTE:refer to https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html#description
func NewLsCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:   "ls [flags] <target>",
		Short: "list buckets and objects",
		// Args:  cobra.ExactArgs(1),
		Example: ls_examples,
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

	// cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"omitempty"`
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
	if len(args) > 0 {
		o.S3Uri = args[0]
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
	j, _ := json.Marshal(o)
	fmt.Fprintf(os.Stdout, "options: %s\n", string(j))
	// return nil
	opt := s3store.S3Option{
		UsePathStyle: o.PathStyle,
	}

	cli, err := s3store.NewS3Client(context.TODO(), opt)
	if err != nil {
		return err
	}

	if o.S3Uri == "" {
		return listBuckets(cli)
	}

	parsedUri, err := uri.ParseS3Uri(o.S3Uri)
	if err != nil {
		return err
	}
	if parsedUri.GetBucket() == "" {
		return listBuckets(cli)
	}

	return listObjects(cli, parsedUri.GetBucket(), parsedUri.GetKey())
}

func listBuckets(cli *s3store.S3Store) error {
	buckets, err := cli.ListBuckets(context.TODO())
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		fmt.Printf("%s\t%s\n", bucket.CreationDate.Format(time.DateTime), *bucket.Name)
	}
	return nil
}

func listObjects(cli *s3store.S3Store, bucket, key string) error {
	_, err := cli.ListObjectsWithPagination(context.TODO(), bucket, key)
	if err != nil {
		return err
	}
	// for _, obj := range objs.Contents {
	// 	fmt.Printf("\t%v\n", *obj.Key)
	// }
	return nil
}
