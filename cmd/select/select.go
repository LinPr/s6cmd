// Package selectcmd implements the `s6cmd select` command. The select
// command structure is csv/json/parquet subcommands + a default fallback
// that treats the source as JSON-Lines, and uses cobra + aws-sdk-go-v2 +
// the s6cmd parallel.Manager framework.
//
// The command expands the source URL (wildcard/prefix/single object) into
// an object channel via storage.List, then for each object schedules a
// parallel.Run task that calls storage.Select. Select streams decoded
// records onto a resultCh of json.RawMessage; a single consumer goroutine
// drains resultCh and writes each record to stdout, cancelling the
// context on EPIPE so a closed downstream pipe does not leave the workers
// hanging.
package selectcmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NewSelectCmd creates the `select` command with csv/json/parquet
// subcommands and a default fallback that treats the source as JSON-Lines.
//
// The subcommands exist purely to scope format-specific flags
// (--delimiter/--use-header for csv, --structure for json); the Run
// pipeline is shared and lives in Options.run.
func NewSelectCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "select [format] [flags] <source>",
		Short:   "run SQL queries on objects",
		Example: select_examples,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Default fallback: no subcommand given. Treat the source as
			// JSON-Lines.
			o.inputFormat = "json"
			o.inputStructure = "lines"
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}

	// Top-level flags (the fallback path). They are a subset of the
	// subcommand flags so the help text stays consistent.
	addSharedSelectFlags(&cmd, o)
	cmd.Flags().StringVar(&o.CompressionType, "compression", "", "input compression format (GZIP, BZIP2, NONE)")

	cmd.AddCommand(newSelectCSVCmd(o))
	cmd.AddCommand(newSelectJSONCmd(o))
	cmd.AddCommand(newSelectParquetCmd(o))

	return &cmd
}

// newSelectCSVCmd builds the `select csv` subcommand.
func newSelectCSVCmd(root *Options) *cobra.Command {
	o := newOptions()
	o.inputFormat = "csv"
	cmd := cobra.Command{
		Use:   "csv [flags] <source>",
		Short: "run SQL queries on CSV objects",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}
	cmd.Flags().StringVar(&o.Delimiter, "delimiter", ",", "field delimiter for the CSV file (use \"\\t\" for tab)")
	cmd.Flags().StringVar(&o.FileHeaderInfo, "use-header", "NONE", "treat the first line as a header: USE, IGNORE, or NONE")
	cmd.Flags().StringVar(&o.CompressionType, "compression", "", "input compression format (GZIP, BZIP2, NONE)")
	addSharedSelectFlags(&cmd, o)
	return &cmd
}

// newSelectJSONCmd builds the `select json` subcommand.
func newSelectJSONCmd(root *Options) *cobra.Command {
	o := newOptions()
	o.inputFormat = "json"
	o.inputStructure = "lines"
	cmd := cobra.Command{
		Use:   "json [flags] <source>",
		Short: "run SQL queries on JSON objects",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}
	cmd.Flags().StringVar(&o.inputStructure, "structure", "lines", "how objects are aligned in the JSON file: lines or document")
	cmd.Flags().StringVar(&o.CompressionType, "compression", "", "input compression format (GZIP, BZIP2, NONE)")
	addSharedSelectFlags(&cmd, o)
	return &cmd
}

// newSelectParquetCmd builds the `select parquet` subcommand. Parquet is
// self-describing so there are no format-specific flags; only the shared
// flags are registered.
func newSelectParquetCmd(root *Options) *cobra.Command {
	o := newOptions()
	o.inputFormat = "parquet"
	cmd := cobra.Command{
		Use:   "parquet [flags] <source>",
		Short: "run SQL queries on Parquet objects",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}
	addSharedSelectFlags(&cmd, o)
	return &cmd
}

// addSharedSelectFlags registers the flags common to the top-level select
// command and all its subcommands.
func addSharedSelectFlags(cmd *cobra.Command, o *Options) {
	cmd.Flags().StringVarP(&o.Query, "query", "e", "", "SQL expression to use to select from the objects")
	cmd.Flags().StringVar(&o.OutputFormat, "output-format", "", "output format of the result (json, csv)")
	cmd.Flags().StringSliceVar(&o.Exclude, "exclude", nil, "exclude objects with given pattern (repeatable)")
	cmd.Flags().StringSliceVar(&o.Include, "include", nil, "include objects with given pattern (repeatable)")
	cmd.Flags().BoolVar(&o.Raw, "raw", false, "disable wildcard operations, useful with filenames that contain glob characters")
	cmd.Flags().BoolVar(&o.AllVersions, "all-versions", false, "list all versions of object(s)")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of the object")
	cmd.Flags().BoolVar(&o.ForceGlacierTransfer, "force-glacier-transfer", false, "force transfer of glacier objects whether they are restored or not")
	cmd.Flags().BoolVar(&o.IgnoreGlacierWarnings, "ignore-glacier-warnings", false, "turns off glacier warnings")
}

// Args holds the positional argument.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the select-specific flags plus the CommonFlags inherited
// from the parent command.
type Flags struct {
	// Query is the SQL expression. It is required.
	Query string
	// OutputFormat is "json" or "csv". Empty means "use the input format".
	OutputFormat string
	// CompressionType is "GZIP"/"BZIP2"/"NONE"/"".
	CompressionType string
	// Exclude / Include are repeatable wildcard patterns.
	Exclude []string
	Include []string
	// Raw disables wildcard expansion on the source URL.
	Raw bool
	// AllVersions / VersionID select a specific version of the object.
	AllVersions bool
	VersionID   string
	// ForceGlacierTransfer / IgnoreGlacierWarnings control Glacier handling.
	ForceGlacierTransfer  bool
	IgnoreGlacierWarnings bool

	// CSV-specific.
	Delimiter      string
	FileHeaderInfo string

	// JSON-specific.
	// inputStructure is "lines" or "document".
	inputStructure string

	// inputFormat is "csv"/"json"/"parquet". It is set by the subcommand
	// constructor, not by a flag.
	inputFormat string

	cliutil.CommonFlags
}

// Options is the closure of Args + Flags. It is the single value passed
// through complete -> validate -> run.
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
	o.CommonFlags = cliutil.LoadParentFlags(cmd)
	if o.inputFormat == "" {
		// Default fallback path: treat as JSON-Lines.
		o.inputFormat = "json"
		if o.inputStructure == "" {
			o.inputStructure = "lines"
		}
	}
	// For CSV the "structure" field carries the delimiter; if the user
	// did not pass --delimiter, default to ",".
	if o.inputFormat == "csv" && o.Delimiter == "" {
		o.Delimiter = ","
	}
	// Unquote the delimiter so escape sequences like "\t" are honoured.
	// strconv.Unquote expects a literal with surrounding quotes; wrap the
	// raw value so single characters like "\t" become a real tab.
	if d, err := unquoteDelimiter(o.Delimiter); err == nil {
		o.Delimiter = d
	}
	return nil
}

// unquoteDelimiter expands Go-style escape sequences in the user-supplied
// delimiter so --delimiter "\t" produces a real tab. strconv.Unquote
// requires surrounding quotes; we add them and fall back to the original
// on any error (so arbitrary single characters like "," keep working).
func unquoteDelimiter(s string) (string, error) {
	if s == "" {
		return s, nil
	}
	return strconvUnquote(`"` + s + `"`)
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	if o.Query == "" {
		return errors.New("query must be non-empty (use --query)")
	}
	src, err := storage.NewStorageURL(o.S3Uri,
		storage.WithVersion(o.VersionID),
		storage.WithAllVersions(o.AllVersions),
		storage.WithRaw(o.Raw),
	)
	if err != nil {
		return err
	}
	if !src.IsRemote() {
		return errors.New("source must be a remote object")
	}
	if (src.IsWildcard() || src.IsPrefix() || src.IsBucket()) && o.VersionID != "" {
		return errors.New("wildcard/prefix operations are disabled with --version-id flag")
	}
	switch o.inputFormat {
	case "csv", "json", "parquet":
	default:
		return fmt.Errorf("select: unsupported input format %q", o.inputFormat)
	}
	return nil
}

// run is the shared entry point for the top-level select command and all
// its subcommands. The flow is:
//
//  1. expandSource -> objch (channel of *storage.Object)
//  2. parallel.NewWaiter + resultCh := make(chan json.RawMessage, 128)
//  3. goroutine A drains waiter.Err() into a cliutil.ErrorCollector
//  4. goroutine B drains resultCh into stdout, cancelling ctx on EPIPE
//  5. for each object in objch: parallel.Run(prepareTask) where
//     prepareTask calls storage.Select(ctx, url, query, resultCh)
//  6. waiter.Wait(); close(resultCh); drainDone(); <-writeDoneCh
func (o *Options) run(ctx context.Context) error {
	store, err := cliutil.NewStorage(ctx, o.CommonFlags)
	if err != nil {
		return err
	}

	src, err := storage.NewStorageURL(o.S3Uri,
		storage.WithVersion(o.VersionID),
		storage.WithAllVersions(o.AllVersions),
		storage.WithRaw(o.Raw),
	)
	if err != nil {
		return err
	}

	// Pre-compile exclude/include patterns once; isObjectExcluded uses
	// them per object.
	excludePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Exclude)
	if err != nil {
		return err
	}
	includePatterns, err := cliutil.CompileExcludeIncludePatterns(o.Include)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// expandSource returns a slice (matching cp/rm behaviour) so we can
	// drive the task-submission loop on the main goroutine without racing
	// the result-writer goroutine on the errs slice.
	objects, err := expandSelectSource(ctx, store, src)
	if err != nil {
		return err
	}

	// goroutine A: the collector's drain consumes waiter.Err(). The
	// collector serializes appends from the drain goroutine and the
	// submission loop below, which used to race on a shared errs slice.
	waiter := parallel.NewWaiter()
	resultCh := make(chan json.RawMessage, 128)
	writeDoneCh := make(chan struct{})
	ec := cliutil.NewErrorCollector("select")
	drainDone := ec.Drain(waiter)

	// goroutine B: drain resultCh into stdout. On EPIPE (broken pipe —
	// typically because the user piped the output to head and closed it)
	// cancel the context so workers stop fetching.
	go func() {
		defer close(writeDoneCh)
		for record := range resultCh {
			select {
			case <-ctx.Done():
				// Drain the channel without writing so senders do not
				// block; the cancel already happened.
				continue
			default:
			}
			if _, err := os.Stdout.Write(append(record, '\n')); err != nil {
				if isEPipeError(err) {
					cancel()
					return
				}
				log.Error(log.ErrorMessage{Operation: "select", Err: err.Error()})
				cancel()
				return
			}
		}
	}()

	for _, object := range objects {
		if object.Err != nil {
			ec.Collect(object.Err)
			continue
		}
		if object.Type.IsDir() {
			continue
		}
		if !object.Type.IsRegular() && object.Type.String() != "" {
			// Skip non-file entries (e.g. symlinks on the local backend,
			// which should not occur for S3 sources but guard anyway).
			continue
		}
		// Glacier check: S3 Select cannot query Glacier/Deep Archive
		// objects. Skip with a warning unless --force-glacier-transfer
		// is set.
		if isGlacierStorageClass(object.StorageClass) && !o.ForceGlacierTransfer {
			if !o.IgnoreGlacierWarnings {
				ec.Collect(fmt.Errorf("object '%v' is on Glacier storage", object))
			}
			continue
		}
		// Exclude/include filtering uses the relative path so patterns
		// behave the same way for prefix and wildcard sources.
		name := object.StorageURL.Relative()
		if name == "" {
			name = object.StorageURL.Absolute()
		}
		if cliutil.IsObjectExcluded(name, excludePatterns, includePatterns) {
			continue
		}

		task := o.prepareTask(ctx, store, object.StorageURL, resultCh)
		parallel.Run(task, waiter)
	}

	waiter.Wait()
	close(resultCh)
	drainDone()
	<-writeDoneCh

	return ec.Aggregate()
}

// prepareTask builds the per-object task that calls storage.Select. The
// task is run on the global parallel.Manager; its result is streamed
// onto resultCh as json.RawMessage records.
func (o *Options) prepareTask(ctx context.Context, store *storage.Storage, url *storage.StorageURL, resultCh chan<- json.RawMessage) parallel.Task {
	return func() error {
		query := &storage.SelectQuery{
			ExpressionType:        "SQL",
			Expression:            o.Query,
			InputFormat:           o.inputFormat,
			InputContentStructure: o.inputStructureForQuery(),
			FileHeaderInfo:        o.FileHeaderInfo,
			OutputFormat:          o.OutputFormat,
			CompressionType:       o.CompressionType,
		}
		if err := store.Select(ctx, url, query, resultCh); err != nil {
			return &errorpkg.Error{Op: "select", Src: url.String(), Err: err}
		}
		return nil
	}
}

// inputStructureForQuery returns the per-format "content structure" value
// that buildInputSerialization expects. For JSON it is the structure
// ("lines"/"document"); for CSV it is the delimiter; for parquet it is
// ignored.
func (o *Options) inputStructureForQuery() string {
	switch o.inputFormat {
	case "json":
		return o.inputStructure
	case "csv":
		return o.Delimiter
	}
	return ""
}

// expandSelectSource materializes the list of source objects. For a single
// non-wildcard, non-prefix URL it returns a one-element slice via Stat;
// otherwise it drains the channel returned by storage.List.
//
// The function returns a slice (not a channel) so the caller can iterate
// it without spawning a second consumer goroutine, which would race with
// the result-writer goroutine on the errs slice.
func expandSelectSource(ctx context.Context, store *storage.Storage, src *storage.StorageURL) ([]*storage.Object, error) {
	if !src.IsWildcard() && !src.IsBucket() && !src.IsPrefix() {
		obj, err := store.Stat(ctx, src)
		if err != nil {
			return nil, err
		}
		return []*storage.Object{obj}, nil
	}
	out := make([]*storage.Object, 0, 64)
	for obj := range store.List(ctx, src, false) {
		if obj.Err != nil && errorpkg.IsCancelation(obj.Err) {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}

// isGlacierStorageClass reports whether the storage class indicates a
// Glacier/Deep Archive tier that S3 Select cannot query directly.
func isGlacierStorageClass(s storage.StorageClass) bool {
	switch string(s) {
	case "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR":
		return true
	}
	return false
}

// isEPipeError reports whether err is a broken-pipe error from os.Stdout.
// On Linux a closed downstream pipe yields syscall.EPIPE; we string-match
// so the check does not pull in syscall on every platform.
func isEPipeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "EPIPE")
}

// strconvUnquote is a tiny indirection so the test suite can stub
// strconv.Unquote without importing strconv at the package's top level
// (select already imports it indirectly via cliutil). It is a function
// variable so the test can replace it.
var strconvUnquote = func(s string) (string, error) {
	return strconvUnquoteReal(s)
}
