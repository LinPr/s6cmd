package cliutil

import (
	"github.com/spf13/cobra"
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
	// RetryCount is the --retry-count value (AWS_RETRY_COUNT env). When
	// non-positive the SDK default retryer is used.
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
}

// LoadParentFlags reads the shared persistent flags from the parent command
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
	if cmd == nil || cmd.Parent() == nil {
		return flags
	}

	parentFlags := cmd.Parent().PersistentFlags()
	// Root PersistentFlags is the same set viper is bound to, so passing
	// parentFlags.Lookup(...) values to viper would be redundant; we read
	// the resolved value via viper.Get* instead.

	if flag := parentFlags.Lookup("endpoint-url"); flag != nil {
		if flag.Changed {
			flags.EndpointURL, _ = parentFlags.GetString("endpoint-url")
		} else {
			flags.EndpointURL = viper.GetString("endpoint-url")
		}
	}
	if flag := parentFlags.Lookup("no-verify-ssl"); flag != nil {
		if flag.Changed {
			flags.NoVerifySSL, _ = parentFlags.GetBool("no-verify-ssl")
		} else if viper.IsSet("no-verify-ssl") {
			flags.NoVerifySSL = viper.GetBool("no-verify-ssl")
		}
	}
	if flag := parentFlags.Lookup("no-paginate"); flag != nil {
		if flag.Changed {
			flags.NoPaginate, _ = parentFlags.GetBool("no-paginate")
		} else if viper.IsSet("no-paginate") {
			flags.NoPaginate = viper.GetBool("no-paginate")
		}
	}
	if flag := parentFlags.Lookup("output"); flag != nil {
		if flag.Changed {
			flags.Output, _ = parentFlags.GetString("output")
		} else if v := viper.GetString("output"); v != "" {
			flags.Output = v
		} else {
			flags.Output, _ = parentFlags.GetString("output")
		}
	}
	if flag := parentFlags.Lookup("profile"); flag != nil {
		if flag.Changed {
			flags.Profile, _ = parentFlags.GetString("profile")
		} else {
			flags.Profile = viper.GetString("profile")
		}
	}
	if flag := parentFlags.Lookup("region"); flag != nil {
		if flag.Changed {
			flags.Region, _ = parentFlags.GetString("region")
		} else {
			flags.Region = viper.GetString("region")
		}
	}
	if flag := parentFlags.Lookup("path-style"); flag != nil {
		if flag.Changed {
			flags.PathStyle, _ = parentFlags.GetBool("path-style")
		} else if viper.IsSet("path-style") {
			flags.PathStyle = viper.GetBool("path-style")
		}
	}
	if flag := parentFlags.Lookup("retry-count"); flag != nil {
		if flag.Changed {
			flags.RetryCount, _ = parentFlags.GetInt("retry-count")
		} else if viper.IsSet("retry-count") {
			flags.RetryCount = viper.GetInt("retry-count")
		}
	}
	if flag := parentFlags.Lookup("no-such-upload-retry-count"); flag != nil {
		if flag.Changed {
			flags.NoSuchUploadRetryCount, _ = parentFlags.GetInt("no-such-upload-retry-count")
		} else if viper.IsSet("no-such-upload-retry-count") {
			flags.NoSuchUploadRetryCount = viper.GetInt("no-such-upload-retry-count")
		}
	}
	if flag := parentFlags.Lookup("credentials-file"); flag != nil {
		if flag.Changed {
			flags.CredentialsFile, _ = parentFlags.GetString("credentials-file")
		} else {
			flags.CredentialsFile = viper.GetString("credentials-file")
		}
	}
	if flag := parentFlags.Lookup("no-sign-request"); flag != nil {
		if flag.Changed {
			flags.NoSignRequest, _ = parentFlags.GetBool("no-sign-request")
		} else if viper.IsSet("no-sign-request") {
			flags.NoSignRequest = viper.GetBool("no-sign-request")
		}
	}

	return flags
}
