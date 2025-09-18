package mb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/storage/uri"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewMbCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "mb <s3uri>",
		Short:   "Creates an S3 bucket.",
		Args:    cobra.ExactArgs(1),
		Example: mb_examples,
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

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be transferred")
	cmd.Flags().StringVarP(&o.Region, "region", "r", "", "specify the region to create the bucket in")

	return &cmd
}

type Args struct {
	S3Uri string
}
type Flags struct {
	DryRun        bool
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
	o.S3Uri = args[0]

	parentFlags := cmd.Parent().PersistentFlags()
	if parentFlags != nil {
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
	s3uri, err := uri.ParseS3Uri(o.S3Uri)
	if err != nil {
		return err
	}
	if s3uri.GetBucket() == "" {
		return fmt.Errorf("bucket name is required in s3uri")
	}

	return nil
}

func (o *Options) run() error {
	fmt.Println("mb called")
	j, _ := json.Marshal(o)
	fmt.Fprintf(os.Stdout, "options: %s\n", string(j))
	// return nil

	opt := s3store.S3Option{
		UsePathStyle: o.PathStyle,
		Region:       o.Region,
	}
	cli, err := s3store.NewS3Client(context.TODO(), opt)
	if err != nil {
		return err
	}

	parsedUri, _ := uri.ParseS3Uri(o.S3Uri)

	exist, err := cli.BucketExists(context.TODO(), parsedUri.GetBucket())
	if err != nil {
		return err
	}
	if exist {
		fmt.Printf("Bucket %s already exists.\n", parsedUri.GetBucket())
	}

	if err := cli.CreateBucket(context.TODO(), parsedUri.GetBucket(), o.Region); err != nil {
		log.Println("Create bucket error:", err)
	}

	return nil
}
