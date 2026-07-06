package du

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	"github.com/LinPr/s6cmd/strutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewDuCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "du [flags] <target>",
		Short:   "calculate total size of bucket or objects",
		Example: du_examples,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				o.S3Uri = args[0]
			}
			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), cmd.OutOrStdout())
		},
	}

	// du is read-only; --dry-run is accepted for interface consistency
	// with the mutating commands but has no effect.
	cmd.Flags().BoolVarP(&o.DryRun, "dry-run", "n", false, "no effect: du is read-only (accepted for consistency)")
	cmd.Flags().BoolVarP(&o.Humanize, "humanize", "H", false, "human-readable sizes")
	cmd.Flags().BoolVarP(&o.GroupByClass, "group", "g", false, "group sizes by storage class")
	cmd.Flags().StringSliceVarP(&o.Exclude, "exclude", "", nil, "exclude objects matching the given wildcard pattern (repeatable)")

	return &cmd
}

type Args struct {
	S3Uri string `validate:"omitempty"`
}
type Flags struct {
	DryRun       bool
	Humanize     bool
	GroupByClass bool
	Exclude      []string
}

type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command) error {
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	return nil
}

type sizeAndCount struct {
	size  int64
	count int64
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	url, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if !url.IsRemote() {
		return fmt.Errorf("du only supports s3:// URLs")
	}
	if url.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	cli, err := cliutil.NewS3Client(ctx, o.common)
	if err != nil {
		return err
	}

	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Exclude)
	if err != nil {
		return err
	}

	storageTotal := map[string]sizeAndCount{}
	total := sizeAndCount{}

	prefix := url.Path
	paginator := s3.NewListObjectsV2Paginator(cli.Client(), &s3.ListObjectsV2Input{
		Bucket: aws.String(url.Bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			key := aws.ToString(obj.Key)
			if key == prefix {
				continue
			}
			if cliutil.MatchAnyPattern(excludePatterns, key) {
				continue
			}
			size := aws.ToInt64(obj.Size)
			cls := string(obj.StorageClass)
			if cls == "" {
				cls = "STANDARD"
			}

			s := storageTotal[cls]
			s.size += size
			s.count++
			storageTotal[cls] = s

			total.size += size
			total.count++
		}
	}

	jsonOutput := o.common.Output == "json"

	if !o.GroupByClass {
		if jsonOutput {
			fmt.Fprintln(out, duMessage{Source: o.S3Uri, Count: total.count, Size: total.size}.JSON())
			return nil
		}
		fmt.Fprintln(out, formatSizeLine(o.S3Uri, "", total, o.Humanize))
		return nil
	}

	classes := make([]string, 0, len(storageTotal))
	for c := range storageTotal {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		if jsonOutput {
			fmt.Fprintln(out, duMessage{
				Source:       o.S3Uri,
				Count:        storageTotal[c].count,
				Size:         storageTotal[c].size,
				StorageClass: c,
			}.JSON())
			continue
		}
		fmt.Fprintln(out, formatSizeLine(o.S3Uri, c, storageTotal[c], o.Humanize))
	}
	return nil
}

// duMessage is the per-line JSON payload for du when --output json is set.
type duMessage struct {
	Source       string `json:"source"`
	Count        int64  `json:"count"`
	Size         int64  `json:"size"`
	StorageClass string `json:"storage_class,omitempty"`
}

func (m duMessage) String() string { return m.JSON() }
func (m duMessage) JSON() string   { return strutil.JSON(m) }

func formatSizeLine(source, class string, sc sizeAndCount, humanize bool) string {
	var sizeStr string
	if humanize {
		sizeStr = strutil.HumanizeBytes(sc.size)
	} else {
		sizeStr = fmt.Sprintf("%d", sc.size)
	}
	if class != "" {
		return fmt.Sprintf("%s bytes in %d objects: %s [%s]", sizeStr, sc.count, source, class)
	}
	return fmt.Sprintf("%s bytes in %d objects: %s", sizeStr, sc.count, source)
}
