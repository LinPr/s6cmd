package cliutil

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type CommonFlags struct {
	EndpointURL string
	NoVerifySSL bool
	NoPaginate  bool
	Output      string
	Profile     string
	Region      string
	PathStyle   bool
	// PathStyleSet reports that --path-style was set explicitly (flag,
	// env or config file). When false and --endpoint-url is set, the S3
	// client defaults to path-style addressing; an explicit
	// --path-style=false keeps virtual-host addressing.
	PathStyleSet bool
	// RetryCount is the --retry-count value (AWS_RETRY_COUNT env): the
	// maximum number of attempts per request. When non-positive the SDK's
	// own retry resolution (AWS_MAX_ATTEMPTS / AWS_RETRY_MODE /
	// shared-config max_attempts) is kept.
	RetryCount int
	// NoSuchUploadRetryCount is the --no-such-upload-retry-count value
	// (AWS_NO_SUCH_UPLOAD_RETRY_COUNT env). When non-positive Put does
	// not retry on NoSuchUpload.
	NoSuchUploadRetryCount int
	// CredentialsFile is the --credentials-file value
	// (AWS_SHARED_CREDENTIALS_FILE env). When empty the SDK default
	// (~/.aws/credentials + ~/.aws/config) is used.
	CredentialsFile string
	// NoSignRequest is the --no-sign-request value (AWS_ANON_BOOL env).
	// When true the S3 client uses anonymous credentials.
	NoSignRequest bool
	// UseListObjectsV1 is the --use-list-objects-v1 value
	// (S6CMD_USE_LIST_OBJECTS_V1 env). When true the legacy ListObjects API
	// is used instead of ListObjectsV2.
	UseListObjectsV1 bool
	// DryRun makes the stores built from these flags no-op every mutating
	// operation (Put/Copy/Delete/...). Unlike the fields above it is NOT a
	// root persistent flag: each command owns its own --dry-run flag and
	// copies the value here before calling NewStorage/NewS3Client, so
	// LoadParentFlags never sets it.
	DryRun bool
}

// LoadParentFlags reads the shared persistent flags from the root command
// and reconciles them with viper so that config-file and env values are
// honoured when the user did not pass an explicit --flag.
//
// Priority (highest to lowest):
//
//  1. Explicit --flag on the command line (cobra's Changed bit)
//  2. Environment variable (bound via viper.BindEnv in cmd/root.go)
//  3. Config file value (loaded by viper.ReadInConfig)
//  4. cobra flag default
//
// Viper's find() already orders these sources, but only for keys bound with
// BindPFlag. cmd/root.go binds every persistent flag, so viper.GetString(key)
// returns the effective value. For string flags we skip the viper lookup when
// the result is empty (so the cobra default — e.g. "text" for --output — is
// preserved when nothing else applies). For bool flags we use IsSet to avoid
// treating a missing key as false.
func LoadParentFlags(cmd *cobra.Command) CommonFlags {
	var flags CommonFlags
	if cmd == nil {
		return flags
	}

	// The shared flags live on the ROOT command's PersistentFlags set.
	// cmd.Parent() is wrong for nested subcommands (`s6cmd select csv ...`
	// has parent `select`, whose own PersistentFlags set is empty), which
	// used to silently drop --endpoint-url/--profile/... and send requests
	// to real AWS. Root() walks up to the top of the tree regardless of
	// nesting depth.
	parentFlags := cmd.Root().PersistentFlags()
	// Root PersistentFlags is the same set viper is bound to, so passing
	// parentFlags.Lookup(...) values to viper would be redundant; we read
	// the resolved value via viper.Get* instead.

	ResolveFlags(parentFlags, []FlagBinding{
		{Name: "endpoint-url", String: &flags.EndpointURL},
		{Name: "output", String: &flags.Output},
		{Name: "profile", String: &flags.Profile},
		{Name: "region", String: &flags.Region},
		{Name: "credentials-file", String: &flags.CredentialsFile},
		{Name: "no-verify-ssl", Bool: &flags.NoVerifySSL},
		{Name: "no-paginate", Bool: &flags.NoPaginate},
		{Name: "path-style", Bool: &flags.PathStyle, Explicit: &flags.PathStyleSet},
		{Name: "no-sign-request", Bool: &flags.NoSignRequest},
		{Name: "use-list-objects-v1", Bool: &flags.UseListObjectsV1},
		{Name: "retry-count", Int: &flags.RetryCount},
		{Name: "no-such-upload-retry-count", Int: &flags.NoSuchUploadRetryCount},
	})

	return flags
}

// FlagBinding names a flag and the destination its reconciled value should
// be written to. Exactly one of String/Bool/Int must be set, matching the
// flag's type.
type FlagBinding struct {
	Name   string
	String *string
	Bool   *bool
	Int    *int
	// Explicit, when non-nil, receives whether the user set the flag
	// explicitly via any source (command line, environment variable or
	// config file) as opposed to the cobra default applying. Callers use
	// it for flags whose default depends on other flags (e.g. --path-style
	// defaults to true only when --endpoint-url is set).
	Explicit *bool
}

// ResolveFlags reconciles each binding's flag with viper and writes the
// effective value to its destination. Priority per flag (highest first):
//
//  1. Explicit --flag on the command line (cobra's Changed bit)
//  2. Environment variable (bound via viper.BindEnv in cmd/root.go)
//  3. Config file value (loaded by viper.ReadInConfig)
//  4. cobra flag default
//
// For string flags an empty viper result falls through to the flag value
// (so the cobra default — e.g. "text" for --output — is preserved when
// nothing else applies). For bool/int flags viper.IsSet distinguishes "not
// configured" from a configured zero value. Flags missing from fs leave
// their destination untouched.
func ResolveFlags(fs *pflag.FlagSet, bindings []FlagBinding) {
	for _, b := range bindings {
		flag := fs.Lookup(b.Name)
		if flag == nil {
			continue
		}
		if b.Explicit != nil {
			// The flag was set explicitly when either cobra parsed it from
			// the command line or viper found it in the environment or the
			// config file. viper.IsSet skips flag defaults, so an untouched
			// flag reports false here.
			*b.Explicit = flag.Changed || viper.IsSet(b.Name)
		}
		switch {
		case b.String != nil:
			if v := viper.GetString(b.Name); !flag.Changed && v != "" {
				*b.String = v
			} else {
				*b.String, _ = fs.GetString(b.Name)
			}
		case b.Bool != nil:
			if !flag.Changed && viper.IsSet(b.Name) {
				*b.Bool = viper.GetBool(b.Name)
			} else {
				*b.Bool, _ = fs.GetBool(b.Name)
			}
		case b.Int != nil:
			if !flag.Changed && viper.IsSet(b.Name) {
				*b.Int = viper.GetInt(b.Name)
			} else {
				*b.Int, _ = fs.GetInt(b.Name)
			}
		}
	}
}
