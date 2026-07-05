/*
Copyright © 2025 LinQinyi
*/
package cmd

import (
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
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
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
	pf := root.PersistentFlags()
	setString := func(name, key string, dst *string) {
		if pf.Changed(name) {
			return // explicit --flag wins; flag value is already in *dst.
		}
		if v := viper.GetString(key); v != "" {
			*dst = v
		}
	}
	setBool := func(name, key string, dst *bool) {
		if pf.Changed(name) {
			return
		}
		if viper.IsSet(key) {
			*dst = viper.GetBool(key)
		}
	}

	setString("endpoint-url", "endpoint-url", &o.EndpointUrl)
	setString("output", "output", &o.Output)
	setString("profile", "profile", &o.Profile)
	setString("region", "region", &o.Region)
	setString("credentials-file", "credentials-file", &o.CredentialsFile)
	setBool("no-verify-ssl", "no-verify-ssl", &o.NoVerifySSL)
	setBool("no-paginate", "no-paginate", &o.NoPaginate)
	setBool("path-style", "path-style", &o.PathStyle)
	setBool("no-sign-request", "no-sign-request", &o.NoSignRequest)
	setInt := func(name, key string, dst *int) {
		if pf.Changed(name) {
			return
		}
		if viper.IsSet(key) {
			*dst = viper.GetInt(key)
		}
	}
	setInt("retry-count", "retry-count", &o.RetryCount)
	setInt("no-such-upload-retry-count", "no-such-upload-retry-count", &o.NoSuchUploadRetryCount)

	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	if o.RetryCount < 0 {
		return fmt.Errorf("retry-count cannot be a negative value")
	}
	if o.NoSuchUploadRetryCount < 0 {
		return fmt.Errorf("no-such-upload-retry-count cannot be a negative value")
	}
	// --no-sign-request is mutually exclusive with --profile and
	// --credentials-file because it disables credential loading entirely.
	// Surfacing this here (rather than after the S3 client fails to load
	// credentials) gives the user a clear error.
	if o.NoSignRequest {
		if o.Profile != "" {
			return fmt.Errorf(`"no-sign-request" and "profile" flags cannot be used together`)
		}
		if o.CredentialsFile != "" {
			return fmt.Errorf(`"no-sign-request" and "credentials-file" flags cannot be used together`)
		}
	}
	return nil
}

func (o *Options) run() error {
	return nil
}

// RootCmd represents the base command when called without any subcommands
func NewRootCmd() *cobra.Command {
	o := NewOptions()
	cmd := &cobra.Command{
		Use:     "s6cmd [command] [arguments...]",
		Short:   "S6cmd is a tool for managing objects in Amazon S3 storage",
		Example: "Find more infomation at README.md",
		// Long:  `Find more infomation at README.md`,
		// SuggestFor: ,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		// PersistentPreRunE runs for every subcommand. It is the single
		// place where root-level flag reconciliation and validation
		// (mutex checks, retry-count >= 0, ...) happens, so subcommands
		// do not have to repeat it. cobra calls the nearest
		// PersistentPreRunE in the chain; because no subcommand defines
		// its own PreRunE, this one always runs.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd.Root()); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// complete/validate already ran in PersistentPreRunE.
			if err := o.run(); err != nil {
				return err
			}
			return cmd.Help()
		},
	}
	homeDir, _ := os.UserHomeDir()
	defaultConfigPath := fmt.Sprintf("%s/.s6cmd.yaml", homeDir)
	cmd.PersistentFlags().StringVar(&ConfigFile, "config", "", "default to "+defaultConfigPath)

	cmd.PersistentFlags().StringVar(&o.EndpointUrl, "endpoint-url", "", "Override the default endpoint URL (or use AWS_ENDPOINT_URL_S3 environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.NoVerifySSL, "no-verify-ssl", "", false, "Disable SSL certificate verification (or use AWS_NO_VERIFY_SSL environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.NoPaginate, "no-paginate", "", false, "Disable automatic pagination of responses (or use AWS_NO_PAGINATE environment variable)")
	cmd.PersistentFlags().StringVarP(&o.Output, "output", "o", "text", "Set output format. One of: json, text, table (or use AWS_OUTPUT environment variable)")
	cmd.PersistentFlags().StringVarP(&o.Profile, "profile", "p", "", "Use a specific profile from your credential file (or use AWS_PROFILE environment variable)")
	cmd.PersistentFlags().StringVar(&o.Region, "region", "", "The region to use. Overrides config/env settings (or use AWS_REGION environment variable)")
	cmd.PersistentFlags().BoolVarP(&o.PathStyle, "path-style", "", false, "Force path-style addressing (https://endpoint/bucket/key). Required for MinIO, OSS, COS, GCS. Env: S6CMD_USE_PATH_STYLE.")
	cmd.PersistentFlags().IntVar(&o.RetryCount, "retry-count", 10, "number of times that a request will be retried for failures (or use AWS_RETRY_COUNT environment variable)")
	cmd.PersistentFlags().IntVar(&o.NoSuchUploadRetryCount, "no-such-upload-retry-count", 0, "number of times that a request will be retried on NoSuchUpload error; you should not use this unless you really know what you're doing (or use AWS_NO_SUCH_UPLOAD_RETRY_COUNT environment variable)")
	_ = cmd.PersistentFlags().Lookup("no-such-upload-retry-count")
	if f := cmd.PersistentFlags().Lookup("no-such-upload-retry-count"); f != nil {
		f.Hidden = true
	}
	cmd.PersistentFlags().StringVar(&o.CredentialsFile, "credentials-file", "", "use the specified credentials file instead of the default credentials file (or use AWS_SHARED_CREDENTIALS_FILE environment variable)")
	cmd.PersistentFlags().BoolVar(&o.NoSignRequest, "no-sign-request", false, "do not sign requests: credentials will not be loaded if --no-sign-request is provided (or use AWS_ANON_BOOL environment variable)")

	// Bind persistent flags to viper so that config file / env values flow
	// through viper.Get(key). BindPFlag keeps the flag pointer; when the flag
	// was changed by the user, cobra's parsed value wins. Otherwise viper
	// falls through to env (BoundEnv below) and then the config file.
	if err := viper.BindPFlag("config", cmd.PersistentFlags().Lookup("config")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("endpoint-url", cmd.PersistentFlags().Lookup("endpoint-url")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("no-verify-ssl", cmd.PersistentFlags().Lookup("no-verify-ssl")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("no-paginate", cmd.PersistentFlags().Lookup("no-paginate")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("output", cmd.PersistentFlags().Lookup("output")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("profile", cmd.PersistentFlags().Lookup("profile")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("region", cmd.PersistentFlags().Lookup("region")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("path-style", cmd.PersistentFlags().Lookup("path-style")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("retry-count", cmd.PersistentFlags().Lookup("retry-count")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("no-such-upload-retry-count", cmd.PersistentFlags().Lookup("no-such-upload-retry-count")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("credentials-file", cmd.PersistentFlags().Lookup("credentials-file")); err != nil {
		panic(err)
	}
	if err := viper.BindPFlag("no-sign-request", cmd.PersistentFlags().Lookup("no-sign-request")); err != nil {
		panic(err)
	}

	// Bind env vars. Keys without an explicit BindEnv fall back to
	// AutomaticEnv below (S6CMD_<KEY>), but the AWS_* names below are the
	// ones the AWS SDK and CLI already use, so bind them explicitly.
	for _, kv := range [][2]string{
		{"endpoint-url", "AWS_ENDPOINT_URL_S3"},
		{"no-verify-ssl", "AWS_NO_VERIFY_SSL"},
		{"no-paginate", "AWS_NO_PAGINATE"},
		{"output", "AWS_OUTPUT"},
		{"profile", "AWS_PROFILE"},
		{"region", "AWS_REGION"},
		{"path-style", "S6CMD_USE_PATH_STYLE"},
		{"config", "S6CMD_CONFIG"},
		{"retry-count", "AWS_RETRY_COUNT"},
		{"no-such-upload-retry-count", "AWS_NO_SUCH_UPLOAD_RETRY_COUNT"},
		{"credentials-file", "AWS_SHARED_CREDENTIALS_FILE"},
		{"no-sign-request", "AWS_ANON_BOOL"},
	} {
		if err := viper.BindEnv(kv[0], kv[1]); err != nil {
			panic(err)
		}
	}

	registerSubCommands(cmd)
	return cmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
//
// Execute uses cobra's RunE contract: every subcommand returns its error
// rather than printing-and-exiting, so the single os.Exit(1) here is the only
// exit-code path. Subcommands that previously used `Run` with `fmt.Fprintf(os.Stderr, ...)`
// have been converted to `RunE` so their errors propagate here.
func Execute() {
	once := sync.Once{}
	cobra.OnInitialize(func() {
		once.Do(func() { initConfig(ConfigFile) })
	})

	rootCmd := NewRootCmd()

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	}
}

// initConfig reads in config file and ENV variables if set.
//
// It is registered via cobra.OnInitialize, so it runs once at the start of
// every command's Execute — including subcommands. The once.Do guard makes
// the body idempotent across nested invocations. ConfigFile is the value of
// --config (or S6CMD_CONFIG via viper.BindEnv); when empty, viper falls back
// to searching ~/.s6cmd.yaml.
func initConfig(configFile string) {
	// --config / S6CMD_CONFIG explicitly points at a file. viper.SetConfigFile
	// honours the extension to pick the parser, so .yaml/.yml/.json all work.
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else if v := viper.GetString("config"); v != "" {
		viper.SetConfigFile(v)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		// Also search the local config/ directory so a repo-shipped
		// config/s6cmd.yaml is picked up by `go run .` and friends.
		viper.AddConfigPath("config")
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName("s6cmd")
	}
	// Replace "."/"-" in env keys so AWS_ENDPOINT_URL_S3 maps to
	// "endpoint-url" rather than "AWS.ENDPOINT.URL.S3".
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		// Config is optional; only print the error, do not abort execution.
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}
	fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
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
