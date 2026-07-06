package ls

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
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
	cmd.Flags().Int32VarP(&o.PageSize, "page-size", "", 0, "number of objects per page (0 = SDK default; ignored with --all-versions)")
	cmd.Flags().BoolVarP(&o.Etag, "etag", "e", false, "show ETag in output")
	cmd.Flags().BoolVarP(&o.StorageClass, "storage-class", "s", false, "show storage class in output")
	cmd.Flags().BoolVarP(&o.ShowFullPath, "show-fullpath", "", false, "show absolute s3:// URLs instead of relative keys")
	cmd.Flags().BoolVarP(&o.AllVersions, "all-versions", "", false, "list all object versions and delete markers with their version IDs")

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

// jsonOutput reports whether --output json is in effect.
func (o *Options) jsonOutput() bool {
	return o.common.Output == "json"
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	cli, err := cliutil.NewS3Client(ctx, o.common)
	if err != nil {
		return err
	}

	if o.S3Uri == "" {
		return o.listBuckets(ctx, cli, out)
	}

	parsedUri, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if parsedUri.Bucket == "" {
		return o.listBuckets(ctx, cli, out)
	}

	if o.AllVersions {
		return o.listAllVersions(ctx, cli, out)
	}
	return o.listObjects(ctx, cli, parsedUri.Bucket, parsedUri.Path, out)
}

const lsDateFormat = "2006-01-02 15:04:05"

func (o *Options) listBuckets(ctx context.Context, cli *s3store.S3Store, out io.Writer) error {
	buckets, err := cli.ListBucketsRaw(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		name := aws.ToString(bucket.Name)
		if o.jsonOutput() {
			fmt.Fprintln(out, lsBucketMessage{
				CreationDate: bucket.CreationDate,
				Name:         name,
			}.JSON())
			continue
		}
		fmt.Fprintf(out, "%s %s\n", bucket.CreationDate.Format(lsDateFormat), name)
	}
	return nil
}

func (o *Options) listObjects(ctx context.Context, cli *s3store.S3Store, bucket, key string, out io.Writer) error {
	delimiter := "/"
	if o.Recursive {
		delimiter = ""
	}
	objects, prefixes, err := cli.ListObjectsWithPagination(ctx, bucket, key, delimiter, o.PageSize, o.common.NoPaginate)
	if err != nil {
		return err
	}

	// Non-recursive listing prints CommonPrefixes first (aws s3 ls style).
	if !o.Recursive {
		for _, p := range prefixes {
			prefix := aws.ToString(p.Prefix)
			if o.jsonOutput() {
				fmt.Fprintln(out, lsObjectMessage{
					Key:  "s3://" + bucket + "/" + prefix,
					Type: "directory",
				}.JSON())
				continue
			}
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
		if o.jsonOutput() {
			fmt.Fprintln(out, lsObjectMessage{
				Key:          "s3://" + bucket + "/" + keyName,
				Type:         "file",
				Etag:         strutil.TrimQuotes(aws.ToString(obj.ETag)),
				LastModified: obj.LastModified,
				Size:         size,
				StorageClass: string(obj.StorageClass),
			}.JSON())
			continue
		}
		fmt.Fprintln(out, formatObjectLine(obj, o.Flags, bucket))
	}

	if o.Summarize && !o.jsonOutput() {
		sizeStr := fmt.Sprintf("%d", totalSize)
		if o.Humanize {
			sizeStr = strutil.HumanizeBytes(totalSize)
		}
		fmt.Fprintf(out, "\nTotal Objects: %d\n   Total Size: %s\n", totalCnt, sizeStr)
	}
	return nil
}

// listAllVersions lists every object version and delete marker under the
// target via the store's ListObjectVersions path (the same one rm
// --all-versions uses), printing the version ID as an extra column.
//
// --page-size and --no-paginate are not honoured here: the version listing
// streams through the shared storage.List channel which paginates fully.
func (o *Options) listAllVersions(ctx context.Context, cli *s3store.S3Store, out io.Writer) error {
	listURL, err := storage.NewStorageURL(o.S3Uri, storage.WithAllVersions(true))
	if err != nil {
		return err
	}
	if o.Recursive {
		// Clear the delimiter so the version listing returns every key
		// under the prefix rather than collapsing sub-prefixes.
		listURL.Delimiter = ""
	}

	for obj := range cli.List(ctx, listURL, false) {
		if obj.Err != nil {
			// An empty prefix is not an error for ls; print nothing.
			if errors.Is(obj.Err, errorpkg.ErrNoObjectFound) {
				return nil
			}
			return obj.Err
		}
		if obj.Type.IsDir() {
			if o.jsonOutput() {
				fmt.Fprintln(out, lsObjectMessage{
					Key:  obj.StorageURL.Absolute(),
					Type: "directory",
				}.JSON())
				continue
			}
			fmt.Fprintf(out, "%s %s\n", formatDirColumn(), obj.StorageURL.Path)
			continue
		}
		if o.jsonOutput() {
			fmt.Fprintln(out, lsObjectMessage{
				Key:            obj.StorageURL.Absolute(),
				Type:           "file",
				Etag:           obj.Etag,
				LastModified:   obj.ModTime,
				Size:           obj.Size,
				StorageClass:   string(obj.StorageClass),
				VersionID:      obj.VersionID,
				IsDeleteMarker: obj.IsDeleteMarker,
			}.JSON())
			continue
		}
		fmt.Fprintln(out, o.formatVersionLine(obj))
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
		extra += " " + strutil.TrimQuotes(aws.ToString(obj.ETag))
	}
	if f.StorageClass {
		extra += " " + string(obj.StorageClass)
	}

	return fmt.Sprintf("%s %10s%s %s", date, sizeStr, extra, key)
}

// formatVersionLine builds a single line for one object version (or delete
// marker) in --all-versions output. The version ID is always printed as an
// extra column between size and key; delete markers are flagged.
func (o *Options) formatVersionLine(obj *storage.Object) string {
	date := ""
	if obj.ModTime != nil {
		date = obj.ModTime.Format(lsDateFormat)
	}

	var sizeStr string
	if o.Humanize {
		sizeStr = strutil.HumanizeBytes(obj.Size)
	} else {
		sizeStr = fmt.Sprintf("%d", obj.Size)
	}

	key := obj.StorageURL.Path
	if o.ShowFullPath {
		key = obj.StorageURL.Absolute()
	}

	var extra string
	if o.Etag {
		extra += " " + obj.Etag
	}
	if o.StorageClass {
		extra += " " + string(obj.StorageClass)
	}
	versionID := obj.VersionID
	if versionID == "" {
		versionID = "null"
	}
	marker := ""
	if obj.IsDeleteMarker {
		marker = " (delete-marker)"
	}

	return fmt.Sprintf("%s %10s%s %s %s%s", date, sizeStr, extra, versionID, key, marker)
}

// lsObjectMessage is the per-line JSON payload for object rows when
// --output json is set.
type lsObjectMessage struct {
	Key            string     `json:"key"`
	Type           string     `json:"type,omitempty"`
	Etag           string     `json:"etag,omitempty"`
	LastModified   *time.Time `json:"last_modified,omitempty"`
	Size           int64      `json:"size"`
	StorageClass   string     `json:"storage_class,omitempty"`
	VersionID      string     `json:"version_id,omitempty"`
	IsDeleteMarker bool       `json:"is_delete_marker,omitempty"`
}

func (m lsObjectMessage) String() string { return m.JSON() }
func (m lsObjectMessage) JSON() string   { return strutil.JSON(m) }

// lsBucketMessage is the per-line JSON payload for bucket rows when
// --output json is set.
type lsBucketMessage struct {
	CreationDate *time.Time `json:"created_at,omitempty"`
	Name         string     `json:"name"`
}

func (m lsBucketMessage) String() string { return m.JSON() }
func (m lsBucketMessage) JSON() string   { return strutil.JSON(m) }
