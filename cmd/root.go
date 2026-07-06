/*
Copyright © 2025 LinQinyi
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/LinPr/s6cmd/cmd/bucketversion"
	"github.com/LinPr/s6cmd/cmd/cat"
	"github.com/LinPr/s6cmd/cmd/cp"
	"github.com/LinPr/s6cmd/cmd/du"
	"github.com/LinPr/s6cmd/cmd/get"
	"github.com/LinPr/s6cmd/cmd/head"
	"github.com/LinPr/s6cmd/cmd/ls"
	"github.com/LinPr/s6cmd/cmd/mb"
	"github.com/LinPr/s6cmd/cmd/mv"
	"github.com/LinPr/s6cmd/cmd/pipe"
	"github.com/LinPr/s6cmd/cmd/presign"
	"github.com/LinPr/s6cmd/cmd/put"
	"github.com/LinPr/s6cmd/cmd/rb"
	"github.com/LinPr/s6cmd/cmd/rm"
	runCmd "github.com/LinPr/s6cmd/cmd/run"
	selectCmd "github.com/LinPr/s6cmd/cmd/select"
	"github.com/LinPr/s6cmd/cmd/stat"
	syncCmd "github.com/LinPr/s6cmd/cmd/sync"
	"github.com/LinPr/s6cmd/cmd/tree"
	"github.com/LinPr/s6cmd/cmd/version"
	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/log"
	logstat "github.com/LinPr/s6cmd/log/stat"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var ConfigFile string

type Options struct {
	EndpointUrl string
	NoVerifySSL bool
	NoPaginate  bool
	Output      string
	Profile     string
	Region      string
	PathStyle   bool
	// RetryCount mirrors --retry-count. <=0 leaves the SDK default.
	RetryCount int
	// NoSuchUploadRetryCount mirrors --no-such-upload-retry-count (hidden).
	// <=0 disables the Put retry-on-NoSuchUpload path.
	NoSuchUploadRetryCount int
	// CredentialsFile mirrors --credentials-file.
	CredentialsFile string
	// NoSignRequest mirrors --no-sign-request. When true the S3 client uses
	// anonymous credentials and Profile/CredentialsFile are rejected.
	NoSignRequest bool
	// UseListObjectsV1 mirrors --use-list-objects-v1 (hidden). When true the
	// legacy ListObjects API is used instead of ListObjectsV2, for
	// S3-compatible services that do not implement V2.
	UseListObjectsV1 bool
	// LogLevel mirrors --log. One of trace, debug, info, error.
	LogLevel string
	// Stat mirrors --stat. When true, per-operation success/error counters
	// are collected and a summary table is printed after the command
	// completes.
	Stat bool

	// profileFlagChanged / credentialsFileFlagChanged record whether the
	// user passed --profile / --credentials-file on the command line (as
	// opposed to the value being resolved from env or the config file).
	// The --no-sign-request mutex only fires for explicit flags: an
	// ambient AWS_PROFILE or AWS_SHARED_CREDENTIALS_FILE simply loses to
	// an explicit --no-sign-request (which ignores credentials entirely),
	// matching the AWS CLI behaviour.
	profileFlagChanged         bool
	credentialsFileFlagChanged bool
}

func NewOptions() *Options {
	return &Options{}
}

// complete reconciles config-file / env / flag values into the Options struct.
//
// Viper's find() already orders sources as: flag (changed) > env > config file
// > flag default. Because root binds each persistent flag with BindPFlag and
// registers the matching env names with BindEnv, viper.Get(key) returns the
// effective value for each option. The flag's own default is the last resort
// via flagDefault, so we only overwrite Options when the flag was NOT changed
// on the command line — explicit --flag wins, env/config file come next, and
// the cobra default applies otherwise.
func (o *Options) complete(root *cobra.Command) error {
	if root == nil {
		return nil
	}
	cliutil.ResolveFlags(root.PersistentFlags(), []cliutil.FlagBinding{
		{Name: "endpoint-url", String: &o.EndpointUrl},
		{Name: "output", String: &o.Output},
		{Name: "log", String: &o.LogLevel},
		{Name: "profile", String: &o.Profile},
		{Name: "region", String: &o.Region},
		{Name: "credentials-file", String: &o.CredentialsFile},
		{Name: "no-verify-ssl", Bool: &o.NoVerifySSL},
		{Name: "no-paginate", Bool: &o.NoPaginate},
		{Name: "path-style", Bool: &o.PathStyle},
		{Name: "no-sign-request", Bool: &o.NoSignRequest},
		{Name: "use-list-objects-v1", Bool: &o.UseListObjectsV1},
		{Name: "stat", Bool: &o.Stat},
		{Name: "retry-count", Int: &o.RetryCount},
		{Name: "no-such-upload-retry-count", Int: &o.NoSuchUploadRetryCount},
	})
	o.profileFlagChanged = root.PersistentFlags().Changed("profile")
	o.credentialsFileFlagChanged = root.PersistentFlags().Changed("credentials-file")
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	// "table" was advertised in older help text but nothing implements
	// it, so only json/text are accepted.
	switch o.Output {
	case "json", "text":
	default:
		return fmt.Errorf(`--output must be "json" or "text", got %q`, o.Output)
	}
	if _, ok := log.LevelFromString(o.LogLevel); !ok {
		return fmt.Errorf(`--log must be one of "trace", "debug", "info", "error", got %q`, o.LogLevel)
	}
	if o.RetryCount < 0 {
		return fmt.Errorf("retry-count cannot be a negative value")
	}
	if o.NoSuchUploadRetryCount < 0 {
		return fmt.Errorf("no-such-upload-retry-count cannot be a negative value")
	}
	// --no-sign-request is mutually exclusive with --profile and
	// --credentials-file because it disables credential loading entirely.
	// The mutex only fires when the conflicting flag was passed EXPLICITLY
	// on the command line: values resolved from env (AWS_PROFILE,
	// AWS_SHARED_CREDENTIALS_FILE) or the config file simply lose to an
	// explicit --no-sign-request, which ignores them like the AWS CLI does.
	// Rejecting the resolved values used to make --no-sign-request unusable
	// on any machine with an ambient AWS_PROFILE.
	if o.NoSignRequest {
		if o.profileFlagChanged && o.Profile != "" {
			return fmt.Errorf(`"no-sign-request" and "profile" flags cannot be used together`)
		}
		if o.credentialsFileFlagChanged && o.CredentialsFile != "" {
			return fmt.Errorf(`"no-sign-request" and "credentials-file" flags cannot be used together`)
		}
	}
	return nil
}

// RootCmd represents the base command when called without any subcommands
func NewRootCmd() *cobra.Command {
	o := NewOptions()
	cmd := &cobra.Command{
		Use:     "s6cmd [command] [arguments...]",
		Short:   "S6cmd is a tool for managing objects in Amazon S3 storage",
		Example: "Find more information at README.md",
		// PersistentPreRunE runs for every subcommand. It is the single
		// place where root-level flag reconciliation and validation
		// (mutex checks, retry-count >= 0, ...) happens, so subcommands
		// do not have to repeat it. cobra calls the nearest
		// PersistentPreRunE in the chain; because no subcommand defines
		// its own PreRunE, this one always runs.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// initConfig (run via cobra.OnInitialize) cannot return an
			// error, so it records fatal config problems here. A corrupt
			// or explicitly requested but unreadable config file must
			// abort instead of silently running with defaults.
			if configLoadErr != nil {
				return configLoadErr
			}
			if err := o.complete(cmd.Root()); err != nil {
				return err
			}
			// Root-level validation failures are flag-usage problems
			// (invalid --output value, mutually exclusive flags, ...);
			// wrap them so Execute maps them to ExitCodeUsage.
			if err := o.validate(); err != nil {
				return &usageError{err}
			}
			// The logger configuration is only known after flag
			// reconciliation, so Init happens here rather than in main.
			// Anything logged earlier went through the log package's
			// lazily-created default logger and was flushed by Init.
			level, _ := log.LevelFromString(o.LogLevel)
			log.Init(level, o.Output == "json")
			// --stat opts in to per-operation counters; execute prints
			// the summary table after the command completes.
			if o.Stat {
				logstat.InitStat()
			}
			if used := viper.ConfigFileUsed(); used != "" {
				log.Debug(log.DebugMessage{Err: fmt.Sprintf("using config file: %v", used)})
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// complete/validate already ran in PersistentPreRunE. The bare
			// root command has no action of its own; print the help text.
			return cmd.Help()
		},
	}
	// initConfig searches $HOME for "s6cmd.<ext>" when --config is not
	// given; keep the help text in sync with that behaviour.
	homeDir, _ := os.UserHomeDir()
	defaultConfigPath := fmt.Sprintf("%s/s6cmd.yaml", homeDir)
	cmd.PersistentFlags().StringVar(&ConfigFile, "config", "", "path to a YAML config file (default search: "+defaultConfigPath+")")

	cmd.PersistentFlags().StringVar(&o.EndpointUrl, "endpoint-url", "", "Override the default endpoint URL (or use AWS_ENDPOINT_URL_S3 environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.NoVerifySSL, "no-verify-ssl", "", false, "Disable SSL certificate verification (or use AWS_NO_VERIFY_SSL environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.NoPaginate, "no-paginate", "", false, "Disable automatic pagination and return only the first page of results; currently honoured by ls object listings, other commands always paginate (or use AWS_NO_PAGINATE environment variable)")
	cmd.PersistentFlags().StringVarP(&o.Output, "output", "o", "text", "Set output format. One of: json, text (or use AWS_OUTPUT environment variable)")
	cmd.PersistentFlags().StringVar(&o.LogLevel, "log", "info", "Set log level. One of: trace, debug, info, error (or use S6CMD_LOG environment variable)")
	cmd.PersistentFlags().StringVarP(&o.Profile, "profile", "p", "", "Use a specific profile from your credential file (or use AWS_PROFILE environment variable)")
	cmd.PersistentFlags().StringVar(&o.Region, "region", "", "The region to use. Overrides config/env settings (or use AWS_REGION environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.PathStyle, "path-style", "", false, "Use path-style addressing (https://endpoint/bucket/key). Defaults to true when --endpoint-url is set (MinIO, OSS, COS, GCS); pass --path-style=false to keep virtual-host addressing with a custom endpoint. Env: S6CMD_USE_PATH_STYLE.")
	cmd.PersistentFlags().IntVar(&o.RetryCount, "retry-count", 0, "maximum number of attempts for a failed request; 0 keeps the SDK default resolution (AWS_MAX_ATTEMPTS, AWS_RETRY_MODE, shared-config max_attempts, falling back to 3 attempts) (or use AWS_RETRY_COUNT environment variable)")
	cmd.PersistentFlags().IntVar(&o.NoSuchUploadRetryCount, "no-such-upload-retry-count", 0, "number of times that a request will be retried on NoSuchUpload error; you should not use this unless you really know what you're doing (or use AWS_NO_SUCH_UPLOAD_RETRY_COUNT environment variable)")
	if f := cmd.PersistentFlags().Lookup("no-such-upload-retry-count"); f != nil {
		f.Hidden = true
	}
	cmd.PersistentFlags().StringVar(&o.CredentialsFile, "credentials-file", "", "use the specified credentials file instead of the default credentials file (or use AWS_SHARED_CREDENTIALS_FILE environment variable)")
	cmd.PersistentFlags().BoolVar(&o.NoSignRequest, "no-sign-request", false, "do not sign requests: credentials will not be loaded if --no-sign-request is provided (or use AWS_ANON_BOOL environment variable)")
	cmd.PersistentFlags().BoolVar(&o.UseListObjectsV1, "use-list-objects-v1", false, "use the legacy ListObjects API instead of ListObjectsV2, for S3-compatible services that do not implement V2 (or use S6CMD_USE_LIST_OBJECTS_V1 environment variable)")
	if f := cmd.PersistentFlags().Lookup("use-list-objects-v1"); f != nil {
		f.Hidden = true
	}
	cmd.PersistentFlags().BoolVar(&o.Stat, "stat", false, "collect statistics of program execution and print a summary at the end (or use S6CMD_STAT environment variable)")

	// Bind persistent flags to viper so that config file / env values flow
	// through viper.Get(key). BindPFlag keeps the flag pointer; when the flag
	// was changed by the user, cobra's parsed value wins. Otherwise viper
	// falls through to env (BoundEnv below) and then the config file.
	// Failures here are programmer errors (a name not registered above), so
	// panicking at init is fine.
	for _, name := range []string{
		"config", "endpoint-url", "no-verify-ssl", "no-paginate", "output",
		"log", "profile", "region", "path-style", "retry-count",
		"no-such-upload-retry-count", "credentials-file", "no-sign-request",
		"use-list-objects-v1", "stat",
	} {
		if err := viper.BindPFlag(name, cmd.PersistentFlags().Lookup(name)); err != nil {
			panic(err)
		}
	}

	// Bind env vars. Keys without an explicit BindEnv fall back to
	// AutomaticEnv (configured with the S6CMD prefix in initConfig, so
	// S6CMD_<KEY> works for every flag), but the AWS_* names below are the
	// ones the AWS SDK and CLI already use, so bind them explicitly.
	// Note viper checks the AutomaticEnv (S6CMD_*) name before the explicit
	// bindings, so S6CMD_OUTPUT beats AWS_OUTPUT when both are set.
	for _, kv := range [][2]string{
		{"endpoint-url", "AWS_ENDPOINT_URL_S3"},
		{"no-verify-ssl", "AWS_NO_VERIFY_SSL"},
		{"no-paginate", "AWS_NO_PAGINATE"},
		{"output", "AWS_OUTPUT"},
		{"profile", "AWS_PROFILE"},
		{"region", "AWS_REGION"},
		{"path-style", "S6CMD_USE_PATH_STYLE"},
		{"config", "S6CMD_CONFIG"},
		{"log", "S6CMD_LOG"},
		{"retry-count", "AWS_RETRY_COUNT"},
		{"no-such-upload-retry-count", "AWS_NO_SUCH_UPLOAD_RETRY_COUNT"},
		{"credentials-file", "AWS_SHARED_CREDENTIALS_FILE"},
		{"no-sign-request", "AWS_ANON_BOOL"},
		{"use-list-objects-v1", "S6CMD_USE_LIST_OBJECTS_V1"},
		{"stat", "S6CMD_STAT"},
	} {
		if err := viper.BindEnv(kv[0], kv[1]); err != nil {
			panic(err)
		}
	}

	// Accept deprecated flag spellings on every subcommand. Must be set
	// before registerSubCommands so cobra propagates it to the children.
	cmd.SetGlobalNormalizationFunc(normalizeFlagName)

	registerSubCommands(cmd)
	return cmd
}

// normalizeFlagName maps deprecated flag spellings onto their canonical
// names so legacy scripts keep working without the alias showing up in
// help output. --dryRun predates the kebab-case flag convention; it is a
// hidden alias for --dry-run.
func normalizeFlagName(f *pflag.FlagSet, name string) pflag.NormalizedName {
	if name == "dryRun" {
		name = "dry-run"
	}
	return pflag.NormalizedName(name)
}

// Exit codes returned by Execute, following common CLI conventions.
const (
	// ExitCodeSuccess means the command completed without error.
	ExitCodeSuccess = 0
	// ExitCodeError means one or more operations failed.
	ExitCodeError = 1
	// ExitCodeUsage means cobra rejected the invocation (unknown
	// command, bad flag, wrong argument count) before any command ran.
	ExitCodeUsage = 2
	// ExitCodeCanceled (128 + SIGINT) means the run was interrupted by a
	// signal canceling the root context.
	ExitCodeCanceled = 130
)

// initConfigOnce guards initConfig so nested/repeated Execute calls (tests)
// only load the config once per process.
var initConfigOnce sync.Once

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main() with the signal-aware root
// context and returns the process exit code; main flushes the logger and
// calls os.Exit with it.
//
// Execute uses cobra's RunE contract: every subcommand returns its error
// rather than printing-and-exiting, so the classification here is the only
// exit-code path. Subcommands that previously used `Run` with
// `fmt.Fprintf(os.Stderr, ...)` have been converted to `RunE` so their
// errors propagate here.
func Execute(ctx context.Context) int {
	return execute(ctx, nil)
}

// execute is the testable core of Execute. A nil args means "use os.Args".
func execute(ctx context.Context, args []string) int {
	cobra.OnInitialize(func() {
		initConfigOnce.Do(func() { initConfig(ConfigFile) })
	})

	rootCmd := NewRootCmd()
	if args != nil {
		rootCmd.SetArgs(args)
	}
	// cobra would otherwise print "Error: ..." plus the full usage text
	// on every failure; silence both so the classification below fully
	// controls what reaches stderr (log.Error already reported
	// per-object failures).
	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	// cobra only reaches PersistentPreRunE after flag parsing and
	// argument validation succeeded, so the hook doubles as the marker
	// separating usage errors (exit 2) from operation failures (exit 1).
	parsed := false
	preRun := rootCmd.PersistentPreRunE
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		parsed = true
		return preRun(cmd, args)
	}

	err := rootCmd.ExecuteContext(ctx)

	// With --stat, print the end-of-run summary table before the exit-code
	// classification message. Statistics() returns an empty slice when
	// collection was never enabled, so this is a no-op without the flag.
	if stats := logstat.Statistics(); len(stats) > 0 {
		log.Stat(stats)
	}

	code, msg := classify(err, ctx.Err(), parsed)
	if msg != "" {
		fmt.Fprintf(os.Stderr, "err: %v\n", msg)
	}
	if code == ExitCodeUsage {
		fmt.Fprintf(os.Stderr, "Run '%v --help' for usage.\n", rootCmd.CommandPath())
	}
	return code
}

// usageError marks an error as a command-line usage problem (invalid value
// for a root flag, mutually exclusive flags, ...) so classify maps it to
// ExitCodeUsage like cobra's own parse errors.
type usageError struct{ err error }

func (u *usageError) Error() string { return u.err.Error() }
func (u *usageError) Unwrap() error { return u.err }

// classify maps the error returned by cobra (plus the root context state
// and the parsed marker) to an exit code and the message to print on
// stderr. An empty message means nothing should be printed.
func classify(err, ctxErr error, parsed bool) (int, string) {
	var uerr *usageError
	switch {
	case err == nil && ctxErr == nil:
		return ExitCodeSuccess, ""
	case ctxErr != nil:
		// The signal context was canceled. Batch drain loops deliberately
		// drop per-object cancelation errors, so err may be nil here even
		// though the run was aborted; the context state is authoritative —
		// and it is the ONLY cancelation signal honoured. An error chain
		// that merely wraps context.Canceled while the root context is
		// still live (e.g. errors.Join(realFailure, context.Canceled) from
		// an internal sub-context) is a real failure and must not be
		// reported as exit 130, which would mask it.
		return ExitCodeCanceled, "operation canceled"
	case !parsed || errors.As(err, &uerr):
		return ExitCodeUsage, err.Error()
	default:
		// errors.Join'ed batch errors were already logged per object via
		// log.Error as they happened; print a summary instead of dumping
		// every message a second time.
		var joined interface{ Unwrap() []error }
		if errors.As(err, &joined) && len(joined.Unwrap()) > 1 {
			return ExitCodeError, fmt.Sprintf("%d operations failed", len(joined.Unwrap()))
		}
		return ExitCodeError, err.Error()
	}
}

// configLoadErr records a fatal problem found while loading the config
// file. cobra.OnInitialize callbacks cannot return errors, so initConfig
// stores it here and the root PersistentPreRunE surfaces it before any
// command logic runs.
var configLoadErr error

// initConfig reads in config file and ENV variables if set.
//
// It is registered via cobra.OnInitialize, so it runs once at the start of
// every command's Execute — including subcommands. The once.Do guard makes
// the body idempotent across nested invocations. ConfigFile is the value of
// --config (or S6CMD_CONFIG via viper.BindEnv); when empty, viper falls back
// to searching $HOME/s6cmd.yaml. The current working directory is
// deliberately NOT searched: a production tool must not pick up an
// attacker-provided s6cmd.yaml from whatever directory it happens to run in.
func initConfig(configFile string) {
	// explicit tracks whether the user pointed at a specific file; a
	// missing explicit file is fatal, a missing search-path file is not.
	explicit := false
	// --config / S6CMD_CONFIG explicitly points at a file. viper.SetConfigFile
	// honours the extension to pick the parser, so .yaml/.yml/.json all work.
	if configFile != "" {
		viper.SetConfigFile(configFile)
		explicit = true
	} else if v := viper.GetString("config"); v != "" {
		viper.SetConfigFile(v)
		explicit = true
	} else if home, err := os.UserHomeDir(); err == nil {
		viper.AddConfigPath(home)
		// No SetConfigType here: with a forced type viper also matches
		// the bare, extensionless name ("$HOME/s6cmd" — e.g. the s6cmd
		// binary itself) and would try to parse it as YAML. Restricting
		// the search to s6cmd.<known-config-extension> avoids that.
		viper.SetConfigName("s6cmd")
	}
	// If the home directory cannot be resolved there is simply nowhere
	// to search; config is optional, so that is not an error.

	// Replace "."/"-" in env keys so AWS_ENDPOINT_URL_S3 maps to
	// "endpoint-url" rather than "AWS.ENDPOINT.URL.S3".
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	// AutomaticEnv consults the environment for every viper key. Without a
	// prefix it would read bare OUTPUT/LOG/PROFILE/REGION/CONFIG/STAT vars —
	// names generic enough to be set by unrelated software — and those
	// lookups happen BEFORE the explicit BindEnv names, so a stray
	// OUTPUT=... would hijack --output for every command. The S6CMD prefix
	// namespaces the automatic lookups (S6CMD_OUTPUT, S6CMD_REGION, ...)
	// while the explicit AWS_*/S6CMD_* BindEnv entries in NewRootCmd keep
	// working unprefixed.
	viper.SetEnvPrefix("S6CMD")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) && !explicit {
			// No config anywhere in the search path: config is
			// optional, stay silent.
			return
		}
		// An explicitly requested but unreadable file, or a config file
		// that exists but does not parse, is fatal: silently running
		// with settings the user did not intend is worse than aborting.
		configLoadErr = fmt.Errorf("read config file: %w", err)
	}
}

func registerSubCommands(cmd *cobra.Command) {
	cmd.AddCommand(ls.NewLsCmd())
	cmd.AddCommand(mb.NewMbCmd())
	cmd.AddCommand(rm.NewRmCmd())
	cmd.AddCommand(get.NewGetCmd())
	cmd.AddCommand(put.NewPutCmd())
	cmd.AddCommand(stat.NewStatCmd())
	cmd.AddCommand(du.NewDuCmd())
	cmd.AddCommand(cp.NewCpCmd())
	cmd.AddCommand(syncCmd.NewSyncCmd())
	cmd.AddCommand(mv.NewMvCmd())
	cmd.AddCommand(rb.NewRbCmd())
	cmd.AddCommand(tree.NewTreeCmd())

	// Additional commands: cat/presign/head/version/
	// bucket-version/pipe, adapted to cobra + the s6cmd storage
	// aggregate.
	cmd.AddCommand(cat.NewCatCmd())
	cmd.AddCommand(presign.NewPresignCmd())
	cmd.AddCommand(head.NewHeadCmd())
	cmd.AddCommand(version.NewVersionCmd())
	cmd.AddCommand(bucketversion.NewBucketVersionCmd())
	cmd.AddCommand(pipe.NewPipeCmd())

	// select provides csv/json/parquet subcommands + a JSON-Lines
	// fallback, backed by the v2 SelectObjectContent EventStream.
	cmd.AddCommand(selectCmd.NewSelectCmd())

	// run reads commands from a file (or stdin) and dispatches each line
	// as a forked s6cmd child process, bounded by --numworkers.
	cmd.AddCommand(runCmd.NewRunCmd())
}
