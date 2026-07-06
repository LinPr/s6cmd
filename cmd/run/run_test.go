package run

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
)

// syncedWriter serializes concurrent writes to an underlying writer. The
// run command streams child process output to the same writer from
// multiple worker goroutines; os.Stdout is safe for concurrent writes at
// the OS level (each Write is one syscall), but a *bytes.Buffer is not.
// Tests inject a syncedWriter wrapping a bytes.Buffer so concurrent
// children do not race on the buffer's internal state.
type syncedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func newSyncedWriter(w io.Writer) *syncedWriter { return &syncedWriter{w: w} }
func (s *syncedWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// TestRun_DispatchFromBuffer builds the s6cmd binary, writes a small
// commands file, and verifies that `s6cmd run` dispatches each line as
// a child process. It uses the `version` subcommand (which prints a
// fixed string and exits 0) so the test does not depend on any S3
// endpoint.
//
// The test pins the binary path via S6CMD_RUN_BINARY so the run command's
// execChild forks the freshly built binary rather than relying on
// os.Executable() (which would point at the test binary, not s6cmd).
func TestRun_DispatchFromBuffer(t *testing.T) {
	// Not parallel: t.Setenv cannot be combined with t.Parallel.
	binary := buildS6cmd(t)
	defer os.Remove(binary)

	// Write a small commands file: two `version` lines + a comment + a
	// blank line. The run command should fork s6cmd twice, once per
	// line, and each fork should print the version string.
	cmdFile := filepath.Join(t.TempDir(), "commands.txt")
	commands := "# comment line\nversion\n\nversion\n"
	if err := os.WriteFile(cmdFile, []byte(commands), 0o644); err != nil {
		t.Fatalf("write commands file: %v", err)
	}

	t.Setenv("S6CMD_RUN_BINARY", binary)
	cmd := NewRunCmd()
	cmd.SetArgs([]string{cmdFile})
	var buf bytes.Buffer
	cmd.SetOut(newSyncedWriter(&buf))
	// Silence cobra's own error output; the run command surfaces errors
	// via RunE.
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Each `version` invocation prints one line; the comment and blank
	// line are skipped. We expect exactly two non-empty output lines.
	got := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(got) != 2 {
		t.Fatalf("output lines: want 2, got %d (%q)", len(got), buf.String())
	}
	for _, line := range got {
		if strings.TrimSpace(line) == "" {
			t.Fatalf("unexpected empty line in output: %q", buf.String())
		}
	}
}

// TestRun_RejectsNestedRun verifies that a line starting with `run` is
// rejected and surfaced as an error rather than forked.
func TestRun_RejectsNestedRun(t *testing.T) {
	// Not parallel: t.Setenv cannot be combined with t.Parallel.
	binary := buildS6cmd(t)
	defer os.Remove(binary)

	t.Setenv("S6CMD_RUN_BINARY", binary)
	cmdFile := filepath.Join(t.TempDir(), "commands.txt")
	if err := os.WriteFile(cmdFile, []byte("run\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cmd := NewRunCmd()
	cmd.SetArgs([]string{cmdFile})
	var buf bytes.Buffer
	cmd.SetErr(&buf)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

	err := cmd.ExecuteContext(context.Background())
	if err == nil {
		t.Fatalf("expected error for nested run, got nil")
	}
	if !strings.Contains(err.Error(), "not permitted in run-mode") {
		t.Errorf("error: want substring %q, got %q", "not permitted in run-mode", err.Error())
	}
}

// TestRun_Stdin verifies that the run command reads from stdin when no
// file argument is given.
func TestRun_Stdin(t *testing.T) {
	// Not parallel: t.Setenv + os.Stdin mutation cannot be combined with
	// t.Parallel.
	binary := buildS6cmd(t)
	defer os.Remove(binary)

	t.Setenv("S6CMD_RUN_BINARY", binary)
	cmd := NewRunCmd()
	cmd.SetArgs([]string{}) // no file → read from stdin
	var buf bytes.Buffer
	cmd.SetOut(newSyncedWriter(&buf))
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

	// Inject stdin by setting cmd's in-or-stdin. cobra does not expose
	// a direct SetIn, but Run reads from os.Stdin via the options struct.
	// We override os.Stdin for the duration of the test.
	tmp, err := os.CreateTemp("", "run-stdin-*.txt")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString("version\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	tmp.Close()

	orig := os.Stdin
	defer func() { os.Stdin = orig }()
	f, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	os.Stdin = f

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got == "" {
		t.Errorf("expected non-empty version output, got %q", buf.String())
	}
}

// TestRun_CommentAndBlankLinesSkipped verifies that comment lines and
// blank lines do not produce any child invocations.
func TestRun_CommentAndBlankLinesSkipped(t *testing.T) {
	// Not parallel: t.Setenv cannot be combined with t.Parallel.
	binary := buildS6cmd(t)
	defer os.Remove(binary)

	t.Setenv("S6CMD_RUN_BINARY", binary)
	cmdFile := filepath.Join(t.TempDir(), "commands.txt")
	commands := "# only a comment\n\n   \n# another\n"
	if err := os.WriteFile(cmdFile, []byte(commands), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cmd := NewRunCmd()
	cmd.SetArgs([]string{cmdFile})
	var buf bytes.Buffer
	cmd.SetOut(newSyncedWriter(&buf))
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		// No commands ran, so there should be no error.
		t.Fatalf("run: %v", err)
	}
	if got := buf.String(); got != "" {
		t.Errorf("expected empty output, got %q", got)
	}
}

// TestRun_NumWorkersFlag verifies that --numworkers is parsed and
// rejects non-positive values.
func TestRun_NumWorkersFlag(t *testing.T) {
	// Not parallel: t.Setenv cannot be combined with t.Parallel.
	t.Setenv("S6CMD_RUN_BINARY", os.Args[0])

	cmd := NewRunCmd()
	cmd.SetArgs([]string{"--numworkers", "0", "/dev/null"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "numworkers") {
		t.Fatalf("expected numworkers error, got %v", err)
	}
}

// buildS6cmd compiles the s6cmd binary into a temp file and returns its
// path. The caller is responsible for removing it. We use -mod=vendor so
// the build does not need network access.
func buildS6cmd(t *testing.T) string {
	t.Helper()
	workdir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// Walk up until we find go.mod; the test runs from cmd/run/, so the
	// project root is two levels up.
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(workdir, "go.mod")); err == nil {
			break
		}
		workdir = filepath.Dir(workdir)
	}
	binary := filepath.Join(t.TempDir(), "s6cmd-test")
	if err := exec.Command("go", "build", "-mod=vendor", "-o", binary,
		filepath.Join(workdir, "main.go")).Run(); err != nil {
		t.Fatalf("build s6cmd: %v", err)
	}
	return binary
}

// TestGlobalFlagArgsPathStyleTriState pins the --path-style forwarding
// semantics of globalFlagArgs: an unset flag is not forwarded (the child
// re-derives the endpoint-dependent default itself), while an explicitly
// set value — true or false — is forwarded verbatim so the child cannot
// override an explicit --path-style=false with the path-style default for
// custom endpoints.
func TestGlobalFlagArgsPathStyleTriState(t *testing.T) {
	t.Parallel()

	// Unset: no --path-style argument at all.
	args := globalFlagArgs(cliutil.CommonFlags{}, rootForward{})
	for _, a := range args {
		if strings.HasPrefix(a, "--path-style") {
			t.Errorf("unset --path-style must not be forwarded, got %q", args)
		}
	}

	// Explicit true.
	args = globalFlagArgs(cliutil.CommonFlags{PathStyle: true, PathStyleSet: true}, rootForward{})
	if !containsArg(args, "--path-style=true") {
		t.Errorf("explicit --path-style must be forwarded as --path-style=true, got %q", args)
	}

	// Explicit false.
	args = globalFlagArgs(cliutil.CommonFlags{PathStyle: false, PathStyleSet: true}, rootForward{})
	if !containsArg(args, "--path-style=false") {
		t.Errorf("explicit --path-style=false must be forwarded verbatim, got %q", args)
	}
}

// TestGlobalFlagArgsForwarding pins which global flags reach the child
// processes. --use-list-objects-v1 (CommonFlags) and the root-only --log /
// --config / --stat used to be silently dropped, so children fell back to
// their own defaults despite the package doc claiming the parent's global
// flags are inherited.
func TestGlobalFlagArgsForwarding(t *testing.T) {
	t.Parallel()

	// Defaults: nothing is forwarded.
	if args := globalFlagArgs(cliutil.CommonFlags{}, rootForward{LogLevel: "info"}); len(args) != 0 {
		t.Errorf("default flags must not be forwarded, got %q", args)
	}

	args := globalFlagArgs(cliutil.CommonFlags{
		EndpointURL:      "http://127.0.0.1:9000",
		UseListObjectsV1: true,
	}, rootForward{
		LogLevel: "debug",
		Config:   "/path/to/s6cmd.yaml",
		Stat:     true,
	})

	for _, want := range []string{"--use-list-objects-v1", "--stat"} {
		if !containsArg(args, want) {
			t.Errorf("args %q should contain %q", args, want)
		}
	}
	for _, pair := range [][2]string{
		{"--endpoint-url", "http://127.0.0.1:9000"},
		{"--log", "debug"},
		{"--config", "/path/to/s6cmd.yaml"},
	} {
		if !containsArgPair(args, pair[0], pair[1]) {
			t.Errorf("args %q should contain %q %q", args, pair[0], pair[1])
		}
	}
}

// containsArgPair reports whether args contains name immediately followed
// by value.
func containsArgPair(args []string, name, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name && args[i+1] == value {
			return true
		}
	}
	return false
}

// TestLineReaderCancelUnblocksBlockedRead verifies that canceling the
// context interrupts a lineReader that is BLOCKED in a read. The read loop
// used to check ctx only between reads, so `s6cmd run` on an idle
// stdin/fifo sat in bufio.ReadString forever, immune to Ctrl-C, and had to
// be SIGKILLed.
func TestLineReaderCancelUnblocksBlockedRead(t *testing.T) {
	t.Parallel()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer w.Close() // keep the write end open: the read must stay blocked

	ctx, cancel := context.WithCancel(context.Background())
	lr := newLineReader(ctx, r)

	// Feed one line to prove the reader works, then cancel while it is
	// blocked waiting for the next line.
	if _, err := w.WriteString("version\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case line := <-lr.lines():
		if strings.TrimSpace(line) != "version" {
			t.Fatalf("line = %q, want %q", line, "version")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first line")
	}

	cancel()

	// The lines channel must close promptly even though no further input
	// (and no EOF) ever arrives on the pipe.
	select {
	case _, ok := <-lr.lines():
		if ok {
			t.Fatal("unexpected extra line after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lineReader did not unblock after context cancelation")
	}
	if err := lr.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Err() = %v, want context.Canceled", err)
	}
}

// TestLineReaderCleanEOF verifies the normal termination path is
// unaffected: EOF closes the channel with a nil Err and does not close
// the underlying reader (the caller owns it).
func TestLineReaderCleanEOF(t *testing.T) {
	t.Parallel()

	lr := newLineReader(context.Background(), strings.NewReader("a\nb\n"))
	var lines []string
	for line := range lr.lines() {
		lines = append(lines, strings.TrimSpace(line))
	}
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("lines = %q, want [a b]", lines)
	}
	if err := lr.Err(); err != nil {
		t.Errorf("Err() after clean EOF = %v, want nil", err)
	}
}

// containsArg reports whether args contains exactly the given argument.
func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}
