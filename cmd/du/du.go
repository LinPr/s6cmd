package du

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	s3store "github.com/LinPr/s6cmd/storage/s3"
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
			ctx := cmd.Context()
			if o.DryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRYRUN: du %s\n", o.S3Uri)
				return nil
			}
			return o.run(ctx, cmd.OutOrStdout())
		},
	}

	cmd.Flags().BoolVarP(&o.DryRun, "dryRun", "n", false, "show what would be computed")
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

	excludePatterns, err := compileWildcards(o.Exclude)
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
			if matchAny(excludePatterns, key) {
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

	if !o.GroupByClass {
		fmt.Fprintln(out, formatSizeLine(o.S3Uri, "", total, o.Humanize))
		return nil
	}

	classes := make([]string, 0, len(storageTotal))
	for c := range storageTotal {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	for _, c := range classes {
		fmt.Fprintln(out, formatSizeLine(o.S3Uri, c, storageTotal[c], o.Humanize))
	}
	return nil
}

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

// compileWildcards converts a list of shell wildcard patterns to regex
// strings. An empty input returns nil with no error.
func compileWildcards(patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		out = append(out, strutil.WildCardToRegexp(p))
	}
	return out, nil
}

// matchAny reports whether key matches any of the precompiled wildcard regex
// strings. Patterns are anchored to the full key.
func matchAny(patterns []string, key string) bool {
	for _, p := range patterns {
		// re-use strutil helpers for full-match semantics
		full := strutil.MatchFromStartToEnd(p)
		if strings.TrimSpace(full) == "" {
			continue
		}
		// We use the simple WildCardToRegexp output; check by regex match.
		// Avoid importing regexp here — strutil already wraps QuoteMeta.
		if wildcardMatch(p, key) {
			return true
		}
	}
	return false
}

// wildcardMatch does a glob-style match (supports ? and *) without regex.
// It is a simple iterative matcher sufficient for --exclude patterns.
func wildcardMatch(pattern, s string) bool {
	pi, si := 0, 0
	star := -1
	ss := 0
	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			star = pi
			ss = si
			pi++
		} else if star != -1 {
			pi = star + 1
			ss++
			si = ss
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}
