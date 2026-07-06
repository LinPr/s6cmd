package cliutil

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// newFlagTestRoot builds a minimal command tree that mirrors the production
// layout: a root command carrying the shared persistent flags, a `select`
// middle command with NO persistent flags of its own, and a `csv` leaf. The
// leaf's RunE captures the CommonFlags LoadParentFlags resolves.
func newFlagTestRoot(captured *CommonFlags) *cobra.Command {
	root := &cobra.Command{Use: "s6cmd"}
	root.PersistentFlags().String("endpoint-url", "", "")
	root.PersistentFlags().Bool("no-verify-ssl", false, "")
	root.PersistentFlags().Bool("no-paginate", false, "")
	root.PersistentFlags().StringP("output", "o", "text", "")
	root.PersistentFlags().StringP("profile", "p", "", "")
	root.PersistentFlags().String("region", "", "")
	root.PersistentFlags().Bool("path-style", false, "")
	root.PersistentFlags().Int("retry-count", 0, "")
	root.PersistentFlags().Int("no-such-upload-retry-count", 0, "")
	root.PersistentFlags().String("credentials-file", "", "")
	root.PersistentFlags().Bool("no-sign-request", false, "")
	root.PersistentFlags().Bool("use-list-objects-v1", false, "")

	middle := &cobra.Command{Use: "select"}
	leaf := &cobra.Command{
		Use: "csv",
		RunE: func(cmd *cobra.Command, args []string) error {
			*captured = LoadParentFlags(cmd)
			return nil
		},
	}
	middle.AddCommand(leaf)
	root.AddCommand(middle)
	return root
}

// TestLoadParentFlagsNestedSubcommand is the regression test for the P0
// where LoadParentFlags read cmd.Parent().PersistentFlags(): for a nested
// subcommand (`s6cmd select csv ...`) the parent is `select`, whose own
// PersistentFlags set is empty, so --endpoint-url/--profile/--region/
// --path-style/--no-sign-request were ALL silently dropped and requests
// went to real AWS with default credentials. The shared flags must resolve
// from the ROOT command regardless of nesting depth.
func TestLoadParentFlagsNestedSubcommand(t *testing.T) {
	var got CommonFlags
	root := newFlagTestRoot(&got)
	root.SetArgs([]string{
		"--endpoint-url", "http://127.0.0.1:9000",
		"--profile", "e2e",
		"--region", "eu-west-1",
		"--path-style",
		"--no-sign-request",
		"--retry-count", "7",
		"--credentials-file", "/tmp/creds",
		"select", "csv",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got.EndpointURL != "http://127.0.0.1:9000" {
		t.Errorf("EndpointURL = %q, want %q", got.EndpointURL, "http://127.0.0.1:9000")
	}
	if got.Profile != "e2e" {
		t.Errorf("Profile = %q, want %q", got.Profile, "e2e")
	}
	if got.Region != "eu-west-1" {
		t.Errorf("Region = %q, want %q", got.Region, "eu-west-1")
	}
	if !got.PathStyle {
		t.Error("PathStyle = false, want true")
	}
	if !got.PathStyleSet {
		t.Error("PathStyleSet = false, want true for an explicit --path-style")
	}
	if !got.NoSignRequest {
		t.Error("NoSignRequest = false, want true")
	}
	if got.RetryCount != 7 {
		t.Errorf("RetryCount = %d, want 7", got.RetryCount)
	}
	if got.CredentialsFile != "/tmp/creds" {
		t.Errorf("CredentialsFile = %q, want %q", got.CredentialsFile, "/tmp/creds")
	}
	// The cobra default must survive for flags that were not passed.
	if got.Output != "text" {
		t.Errorf("Output = %q, want cobra default %q", got.Output, "text")
	}
}

// TestLoadParentFlagsFirstLevelSubcommand pins that the Root() lookup also
// covers the common single-level case (`s6cmd ls ...`).
func TestLoadParentFlagsFirstLevelSubcommand(t *testing.T) {
	var got CommonFlags
	root := newFlagTestRoot(&got)
	// Reuse the tree but capture from the middle command instead.
	middle, _, err := root.Find([]string{"select"})
	if err != nil {
		t.Fatalf("Find(select): %v", err)
	}
	middle.RunE = func(cmd *cobra.Command, args []string) error {
		got = LoadParentFlags(cmd)
		return nil
	}
	root.SetArgs([]string{"--endpoint-url", "http://127.0.0.1:9001", "select"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.EndpointURL != "http://127.0.0.1:9001" {
		t.Errorf("EndpointURL = %q, want %q", got.EndpointURL, "http://127.0.0.1:9001")
	}
	// --path-style was not passed anywhere, so the explicit bit must be
	// false: with a custom endpoint the store then defaults to path-style.
	if got.PathStyleSet {
		t.Error("PathStyleSet = true, want false when --path-style is untouched")
	}
}

// TestResolveFlags covers the table-driven resolver directly: explicit
// flag > viper (env/config) > cobra default, per flag type. Notably the
// cobra default must be written for int flags that are neither changed nor
// configured (the per-flag copies this replaced silently left such
// destinations at zero).
func TestResolveFlags(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("output", "text", "")
	fs.String("region", "", "")
	fs.Bool("path-style", false, "")
	fs.Int("retry-count", 10, "")
	fs.Int("jobs", 4, "")
	if err := fs.Parse([]string{"--region", "eu-west-1", "--jobs", "8"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Simulate config/env values via viper. "region" is also changed on
	// the command line, so the flag must win over viper.
	viper.Set("region", "us-east-2")
	viper.Set("path-style", true)
	t.Cleanup(viper.Reset)

	var (
		output    string
		region    string
		pathStyle bool
		retry     int
		jobs      int
	)
	ResolveFlags(fs, []FlagBinding{
		{Name: "output", String: &output},
		{Name: "region", String: &region},
		{Name: "path-style", Bool: &pathStyle},
		{Name: "retry-count", Int: &retry},
		{Name: "jobs", Int: &jobs},
		{Name: "does-not-exist", Int: &retry}, // must be ignored, not panic
	})

	if output != "text" {
		t.Errorf("output = %q, want cobra default %q", output, "text")
	}
	if region != "eu-west-1" {
		t.Errorf("region = %q, want explicit flag value %q", region, "eu-west-1")
	}
	if !pathStyle {
		t.Error("path-style = false, want true from viper")
	}
	if retry != 10 {
		t.Errorf("retry-count = %d, want cobra default 10", retry)
	}
	if jobs != 8 {
		t.Errorf("jobs = %d, want explicit flag value 8", jobs)
	}
}

// TestResolveFlagsExplicitBit pins the Explicit destination: it must be
// true when the flag was set on the command line (even to its default
// value, e.g. --path-style=false) or via viper (env/config file), and false
// when only the cobra default applies. The addressing policy depends on
// this distinction: --path-style=false with a custom endpoint means
// "virtual-host on purpose", while an untouched flag means "default to
// path-style".
func TestResolveFlagsExplicitBit(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Bool("path-style", false, "")
	fs.Bool("untouched", false, "")
	fs.String("region", "", "")
	if err := fs.Parse([]string{"--path-style=false"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	viper.Set("region", "eu-west-1")
	t.Cleanup(viper.Reset)

	var (
		pathStyle, pathStyleSet bool
		untouched, untouchedSet bool
		region                  string
		regionSet               bool
	)
	ResolveFlags(fs, []FlagBinding{
		{Name: "path-style", Bool: &pathStyle, Explicit: &pathStyleSet},
		{Name: "untouched", Bool: &untouched, Explicit: &untouchedSet},
		{Name: "region", String: &region, Explicit: &regionSet},
	})

	if pathStyle {
		t.Error("path-style value: want false (explicitly set to false)")
	}
	if !pathStyleSet {
		t.Error("path-style explicit bit: want true for --path-style=false on the command line")
	}
	if untouchedSet {
		t.Error("untouched explicit bit: want false when neither flag nor viper set it")
	}
	if !regionSet {
		t.Error("region explicit bit: want true when viper (env/config) provides the value")
	}
	if region != "eu-west-1" {
		t.Errorf("region = %q, want viper value %q", region, "eu-west-1")
	}
}
