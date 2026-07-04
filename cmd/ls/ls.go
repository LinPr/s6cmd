package ls

import (
	"context"
	"fmt"
	"io"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/LinPr/s6cmd/strutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NOTE:refer to https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html#description
func NewLsCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "ls [flags] <target>",
		Short:   "list buckets and objects",
		Example: ls_examples,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			if err := o.run(ctx, cmd.OutOrStdout()); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().BoolVarP(&o.Recursive, "recursive", "r", false, "list objects recursively")
	cmd.Flags().BoolVarP(&o.Humanize, "humanize", "H", false, "human-readable object sizes")
	cmd.Flags().BoolVarP(&o.Summarize, "summarize", "", false, "print total objects and size at the end")
	cmd.Flags().Int32VarP(&o.PageSize, "page-size", "", 0, "number of objects per page (0 = SDK default)")
	cmd.Flags().BoolVarP(&o.Etag, "etag", "e", false, "show ETag in output")
	cmd.Flags().BoolVarP(&o.StorageClass, "storage-class", "s", false, "show storage class in output")
	cmd.Flags().BoolVarP(&o.ShowFullPath, "show-fullpath", "", false, "show absolute s3:// URLs instead of relative keys")
	cmd.Flags().BoolVarP(&o.AllVersions, "all-versions", "", false, "list all object versions")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"omitempty"`
}
type Flags struct {
	Recursive    bool
	Summarize    bool
	Humanize     bool
	PageSize     int32
	Etag         bool
	StorageClass bool
	ShowFullPath bool
	AllVersions  bool
}

type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		o.S3Uri = args[0]
	}
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

	if o.S3Uri == "" {
		return listBuckets(ctx, cli, out)
	}

	parsedUri, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if parsedUri.Bucket == "" {
		return listBuckets(ctx, cli, out)
	}

	return listObjects(ctx, cli, parsedUri.Bucket, parsedUri.Path, o.Flags, out)
}

const lsDateFormat = "2006-01-02 15:04:05"

func listBuckets(ctx context.Context, cli *s3store.S3Store, out io.Writer) error {
	buckets, err := cli.ListBucketsRaw(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		name := aws.ToString(bucket.Name)
		fmt.Fprintf(out, "%s %s\n", bucket.CreationDate.Format(lsDateFormat), name)
	}
	return nil
}

func listObjects(ctx context.Context, cli *s3store.S3Store, bucket, key string, f Flags, out io.Writer) error {
	delimiter := "/"
	if f.Recursive {
		delimiter = ""
	}
	objects, prefixes, err := cli.ListObjectsWithPagination(ctx, bucket, key, delimiter)
	if err != nil {
		return err
	}

	// Non-recursive listing prints CommonPrefixes first (aws s3 ls style).
	if !f.Recursive {
		for _, p := range prefixes {
			prefix := aws.ToString(p.Prefix)
			fmt.Fprintf(out, "%s %s\n", formatDirColumn(), prefix)
		}
	}

	var (
		totalSize int64
		totalCnt  int64
	)
	for _, obj := range objects {
		if obj.Key == nil {
			continue
		}
		// S3 sometimes returns the prefix itself as a "directory" object; skip
		// zero-size keys that end with "/".
		keyName := aws.ToString(obj.Key)
		if keyName == key || (obj.Size != nil && *obj.Size == 0 && keyName == key+"/") {
			continue
		}
		size := aws.ToInt64(obj.Size)
		totalSize += size
		totalCnt++
		fmt.Fprintln(out, formatObjectLine(obj, f, bucket))
	}

	if f.Summarize {
		sizeStr := fmt.Sprintf("%d", totalSize)
		if f.Humanize {
			sizeStr = strutil.HumanizeBytes(totalSize)
		}
		fmt.Fprintf(out, "\nTotal Objects: %d\n   Total Size: %s\n", totalCnt, sizeStr)
	}
	return nil
}

// formatDirColumn renders the leading columns for a directory (CommonPrefix)
// row in non-recursive aws s3 ls output: a fixed-width date placeholder and a
// "PRE" marker.
func formatDirColumn() string {
	// aws s3 ls prints `                             PRE prefix/`
	return fmt.Sprintf("%-20s %10s", "", "PRE")
}

// formatObjectLine builds a single aws s3 ls style line for one object.
func formatObjectLine(obj types.Object, f Flags, bucket string) string {
	date := ""
	if obj.LastModified != nil {
		date = obj.LastModified.Format(lsDateFormat)
	}

	var sizeStr string
	if f.Humanize {
		sizeStr = strutil.HumanizeBytes(aws.ToInt64(obj.Size))
	} else {
		sizeStr = fmt.Sprintf("%d", aws.ToInt64(obj.Size))
	}

	key := aws.ToString(obj.Key)
	if f.ShowFullPath {
		key = "s3://" + bucket + "/" + key
	}

	// Optional extra columns between size and key: etag, storage class.
	var extra string
	if f.Etag {
		extra += " " + trimEtag(aws.ToString(obj.ETag))
	}
	if f.StorageClass {
		extra += " " + string(obj.StorageClass)
	}

	return fmt.Sprintf("%s %10s%s %s", date, sizeStr, extra, key)
}

// trimEtag strips the surrounding quotes S3 wraps around ETag values. We
// redeclare it here (rather than importing storage/s3's unexported helper) to
// keep cmd/ls decoupled from the concrete store package's internals.
func trimEtag(v string) string {
	for len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		v = v[1 : len(v)-1]
	}
	return v
}
