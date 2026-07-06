package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/LinPr/s6cmd/internal/errorpkg"
)

// TestClassify verifies the error → exit-code mapping used by Execute.
func TestClassify(t *testing.T) {
	joined := errors.Join(errors.New("a failed"), errors.New("b failed"), errors.New("c failed"))
	cases := []struct {
		name     string
		err      error
		ctxErr   error
		parsed   bool
		wantCode int
		wantMsg  string
	}{
		{"success", nil, nil, true, ExitCodeSuccess, ""},
		{"operation failure", errors.New("boom"), nil, true, ExitCodeError, "boom"},
		{"usage: before pre-run", errors.New("unknown flag"), nil, false, ExitCodeUsage, "unknown flag"},
		{"usage: wrapped usageError", &usageError{errors.New("bad --output")}, nil, true, ExitCodeUsage, "bad --output"},
		{"canceled context, nil error", nil, context.Canceled, true, ExitCodeCanceled, "operation canceled"},
		{"canceled context with error", fmt.Errorf("cp: %w", context.Canceled), context.Canceled, true, ExitCodeCanceled, "operation canceled"},
		// Only the ROOT context state selects exit 130. An error chain that
		// merely wraps context.Canceled while the root context is still live
		// (an internal sub-context was canceled) is a real failure; mapping
		// it to 130 used to mask the failure that came with it.
		{"cancelation error, live context", fmt.Errorf("cp: %w", context.Canceled), nil, true, ExitCodeError, "cp: " + context.Canceled.Error()},
		{"deadline is a failure", context.DeadlineExceeded, nil, true, ExitCodeError, context.DeadlineExceeded.Error()},
		{"joined batch summary", joined, nil, true, ExitCodeError, "3 operations failed"},
		{"single joined error", errors.Join(errors.New("only")), nil, true, ExitCodeError, "only"},
		{"joined with cancelation, live context", errors.Join(errors.New("x"), context.Canceled), nil, true, ExitCodeError, "2 operations failed"},
		{"joined with cancelation, canceled context", errors.Join(errors.New("x"), context.Canceled), context.Canceled, true, ExitCodeCanceled, "operation canceled"},
	}
	for _, c := range cases {
		code, msg := classify(c.err, c.ctxErr, c.parsed)
		if code != c.wantCode || msg != c.wantMsg {
			t.Errorf("%s: classify() = (%d, %q), want (%d, %q)", c.name, code, msg, c.wantCode, c.wantMsg)
		}
	}
	// Sanity: warnings never reach classify as cancelations.
	if errorpkg.IsCancelation(errorpkg.ErrObjectExists) {
		t.Error("IsCancelation(ErrObjectExists) = true, want false")
	}
}

// TestExecuteExitCodes drives the real cobra tree offline and checks the
// process exit code for usage errors vs successes. HOME is pointed at an
// empty temp dir so no developer config file leaks into the run.
func TestExecuteExitCodes(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()

	cases := []struct {
		name string
		args []string
		want int
	}{
		{"success", []string{"version"}, ExitCodeSuccess},
		{"unknown command", []string{"definitely-not-a-command"}, ExitCodeUsage},
		{"unknown flag", []string{"--definitely-not-a-flag"}, ExitCodeUsage},
		{"missing positional args", []string{"mb"}, ExitCodeUsage},
		{"invalid --output value", []string{"version", "--output", "table"}, ExitCodeUsage},
		{"invalid --log value", []string{"version", "--log", "loud"}, ExitCodeUsage},
		{"negative retry-count", []string{"version", "--retry-count=-3"}, ExitCodeUsage},
	}
	for _, c := range cases {
		if got := execute(ctx, c.args); got != c.want {
			t.Errorf("%s: execute(%v) = %d, want %d", c.name, c.args, got, c.want)
		}
	}
}

// TestDryRunFlagAlias verifies that the canonical --dry-run flag exists on
// the mutating subcommands with the -n shorthand, and that the deprecated
// --dryRun spelling is normalized onto it (hidden alias via the global
// normalization func) without executing anything.
//
// pipe is deliberately absent from the shorthand list: -n historically
// meant --no-clobber there, so pipe carries NO -n shorthand at all (see
// TestPipeHasNoShorthandN).
func TestDryRunFlagAlias(t *testing.T) {
	root := NewRootCmd()
	for _, name := range []string{"cp", "mv", "sync", "rm", "rb", "get", "put", "mb"} {
		sub, _, err := root.Find([]string{name})
		if err != nil || sub == nil {
			t.Fatalf("Find(%q): %v", name, err)
		}
		if err := sub.ParseFlags([]string{"--dryRun"}); err != nil {
			t.Errorf("%s: --dryRun alias not accepted: %v", name, err)
			continue
		}
		f := sub.Flags().Lookup("dry-run")
		if f == nil {
			t.Errorf("%s: no --dry-run flag registered", name)
			continue
		}
		if !f.Changed || f.Value.String() != "true" {
			t.Errorf("%s: --dryRun did not set --dry-run (changed=%v value=%v)", name, f.Changed, f.Value)
		}
		if f.Shorthand != "n" {
			t.Errorf("%s: --dry-run shorthand = %q, want %q", name, f.Shorthand, "n")
		}
	}

	// The shorthand collisions were resolved: pipe's --no-clobber and cp's
	// --flatten must not own a shorthand anymore.
	pipeCmd, _, _ := root.Find([]string{"pipe"})
	if f := pipeCmd.Flags().Lookup("no-clobber"); f == nil || f.Shorthand != "" {
		t.Errorf("pipe --no-clobber shorthand should be empty, got %+v", f)
	}
	cpCmd, _, _ := root.Find([]string{"cp"})
	if f := cpCmd.Flags().Lookup("flatten"); f == nil || f.Shorthand != "" {
		t.Errorf("cp --flatten shorthand should be empty, got %+v", f)
	}
	rbCmd, _, _ := root.Find([]string{"rb"})
	if f := rbCmd.Flags().Lookup("force"); f == nil || f.Shorthand != "f" {
		t.Errorf("rb --force should keep the -f shorthand, got %+v", f)
	}
}

// TestPipeHasNoShorthandN pins that pipe rejects -n outright. The
// shorthand historically meant --no-clobber for pipe; briefly re-pointing
// it at --dry-run silently changed a legacy `pipe -n` from "don't
// overwrite" into "upload NOTHING and exit 0". Neither meaning is safe to
// keep, so pipe has no -n at all: legacy usage fails loudly with exit 2.
func TestPipeHasNoShorthandN(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	root := NewRootCmd()
	pipeCmd, _, _ := root.Find([]string{"pipe"})
	if f := pipeCmd.Flags().Lookup("dry-run"); f == nil || f.Shorthand != "" {
		t.Errorf("pipe --dry-run shorthand should be empty, got %+v", f)
	}
	if f := pipeCmd.Flags().ShorthandLookup("n"); f != nil {
		t.Errorf("pipe must not own a -n shorthand, got %q", f.Name)
	}

	// End-to-end: a legacy `pipe -n` invocation is a usage error.
	if got := execute(context.Background(), []string{"pipe", "-n", "s3://bucket/key"}); got != ExitCodeUsage {
		t.Errorf("execute(pipe -n) = %d, want %d", got, ExitCodeUsage)
	}
}

// TestEnvPrefixIsolation verifies that AutomaticEnv is namespaced with the
// S6CMD prefix: bare OUTPUT/LOG env vars — generic names any unrelated
// software may set — must NOT be consulted (they used to hijack --output
// and --log for every command and beat the documented AWS_* bindings),
// while the prefixed S6CMD_* names must work.
func TestEnvPrefixIsolation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()

	// Bare env vars are ignored: the command must succeed with defaults.
	t.Setenv("OUTPUT", "bogus")
	t.Setenv("LOG", "bogus")
	if got := execute(ctx, []string{"version"}); got != ExitCodeSuccess {
		t.Errorf("execute(version) with bare OUTPUT/LOG in env = %d, want %d (bare env vars must not be consulted)", got, ExitCodeSuccess)
	}

	// Prefixed env vars are consulted: an invalid S6CMD_OUTPUT is a usage
	// error, proving the prefixed name reaches the --output validation...
	t.Setenv("S6CMD_OUTPUT", "bogus")
	if got := execute(ctx, []string{"version"}); got != ExitCodeUsage {
		t.Errorf("execute(version) with S6CMD_OUTPUT=bogus = %d, want %d (prefixed env vars must be consulted)", got, ExitCodeUsage)
	}
	// ...and a valid one switches the output format successfully.
	t.Setenv("S6CMD_OUTPUT", "json")
	if got := execute(ctx, []string{"version"}); got != ExitCodeSuccess {
		t.Errorf("execute(version) with S6CMD_OUTPUT=json = %d, want %d", got, ExitCodeSuccess)
	}
	os.Unsetenv("S6CMD_OUTPUT")

	// The documented AWS-style explicit binding still works alongside the
	// prefixed automatic lookup.
	t.Setenv("AWS_OUTPUT", "bogus")
	if got := execute(ctx, []string{"version"}); got != ExitCodeUsage {
		t.Errorf("execute(version) with AWS_OUTPUT=bogus = %d, want %d (explicit AWS_* binding must be kept)", got, ExitCodeUsage)
	}
}

// TestNoSignRequestMutex pins the --no-sign-request mutual-exclusion rule:
// only an EXPLICIT --profile/--credentials-file flag conflicts with it.
// Values resolved from the environment (an ambient AWS_PROFILE /
// AWS_SHARED_CREDENTIALS_FILE, present on most developer machines) lose to
// an explicit --no-sign-request instead of making it unusable with exit 2.
func TestNoSignRequestMutex(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx := context.Background()

	// Ambient env-resolved values must not trigger the mutex. `version`
	// never builds an S3 client, so this exercises exactly the root
	// validation path that used to reject the flags.
	t.Setenv("AWS_PROFILE", "some-ambient-profile")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials")
	if got := execute(ctx, []string{"version", "--no-sign-request"}); got != ExitCodeSuccess {
		t.Errorf("--no-sign-request with ambient AWS_PROFILE/AWS_SHARED_CREDENTIALS_FILE = %d, want %d", got, ExitCodeSuccess)
	}

	// Explicit flags still conflict.
	if got := execute(ctx, []string{"version", "--no-sign-request", "--profile", "p"}); got != ExitCodeUsage {
		t.Errorf("--no-sign-request --profile = %d, want %d", got, ExitCodeUsage)
	}
	if got := execute(ctx, []string{"version", "--no-sign-request", "--credentials-file", "/tmp/creds"}); got != ExitCodeUsage {
		t.Errorf("--no-sign-request --credentials-file = %d, want %d", got, ExitCodeUsage)
	}
}

// TestExecuteCanceledContext verifies that a canceled root context (the
// signal path) yields exit code 130 even when the command itself did not
// return an error.
func TestExecuteCanceledContext(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := execute(ctx, []string{"version"}); got != ExitCodeCanceled {
		t.Errorf("execute(canceled ctx) = %d, want %d", got, ExitCodeCanceled)
	}
}
