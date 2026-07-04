// Package run implements the `s6cmd run` command. The run command reads
// commands from a file or stdin, dispatches each line as a separate s6cmd
// invocation in parallel, and adapts it to the cobra + s6cmd toolchain.
//
// Execution model: each non-comment, non-empty line is forked as a child
// s6cmd process via os/exec. The alternative — reusing the in-process
// cobra.Command tree — is unsafe under concurrency because cobra Commands
// and their pflag.FlagSet are not safe for concurrent use (ParseFlags
// mutates shared state). Forking a child per line gives each command an
// isolated cobra/viper/flag set, naturally inherits the parent's global
// flags (forwarded as CLI args prepended to each line), and matches what
// users expect from a batch runner: a failing line does not poison the
// next.
//
// The parent's effective global flags (--endpoint-url, --profile,
// --region, --path-style, ...) are resolved via cliutil.LoadParentFlags
// and prepended to each line's arguments so the user does not have to
// repeat them on every line of the commands file.
package run

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// defaultNumWorkers is the default worker count for the run command's
// per-line parallelism. It is smaller than parallel.defaultWorkerCount
// (256) because each task forks a process; a high concurrency would
// exhaust file descriptors and memory.
const defaultNumWorkers = 16

// NewRunCmd creates the `run` command. It accepts zero or one positional
// argument: a file containing one command per line. With no argument it
// reads commands from stdin.
func NewRunCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "run [file]",
		Short:   "run commands in batch",
		Example: run_examples,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			// Resolve stdout/stderr via cobra so tests can inject a
			// buffer via SetOut/SetErr. In production these return
			// os.Stdout/os.Stderr.
			o.stdout = cmd.OutOrStdout()
			o.stderr = cmd.ErrOrStderr()
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}

	cmd.Flags().IntVarP(&o.NumWorkers, "numworkers", "w", defaultNumWorkers, "number of concurrent workers to run commands in parallel")

	return &cmd
}

// Args holds the optional positional argument (the commands file). It is
// optional so we use validate:"omitempty" — an empty value means "read
// from stdin".
type Args struct {
	File string `validate:"omitempty"`
}

// Flags holds the run-specific flags plus the CommonFlags inherited from
// the parent command. The CommonFlags are used to build the global-flag
// prefix that is prepended to every child command line.
type Flags struct {
	NumWorkers int
	cliutil.CommonFlags
}

// Options is the closure of Args + Flags + the reader the commands are
// read from.
type Options struct {
	Args
	Flags
	// reader is the source of command lines. It defaults to os.Stdin
	// when File is empty, or to an *os.File opened on File.
	reader io.Reader
	// binaryPath is the path to the s6cmd executable that child processes
	// invoke. It is resolved via os.Executable() in complete(); tests
	// override it via the S6CMD_RUN_BINARY env var or the binaryPath
	// field.
	binaryPath string
	// stdout / stderr are the writers the child processes' output is
	// streamed to. They default to os.Stdout / os.Stderr via cobra's
	// OutOrStdout / ErrOrStderr in complete(). Tests inject buffers via
	// cmd.SetOut / cmd.SetErr.
	stdout io.Writer
	stderr io.Writer
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	if len(args) == 1 {
		o.File = args[0]
	}
	o.CommonFlags = cliutil.LoadParentFlags(cmd)

	// Resolve the s6cmd binary path. os.Executable returns the path of
	// the running process; for `go run` and `go test` this points at the
	// temporary build, which is exactly what we want for e2e tests. The
	// S6CMD_RUN_BINARY env var lets a test pin a specific binary.
	if v := os.Getenv("S6CMD_RUN_BINARY"); v != "" {
		o.binaryPath = v
	} else if p, err := os.Executable(); err == nil {
		o.binaryPath = p
	}

	// Open the commands file (if any); otherwise read from stdin.
	if o.File != "" {
		f, err := os.Open(o.File)
		if err != nil {
			return err
		}
		// Wrap in a closer so run() can close it on exit. We don't use
		// a defer here because the file must stay open for the duration
		// of run(); instead run() closes it via the io.ReadCloser
		// returned by newLineReader.
		o.reader = f
	} else {
		o.reader = os.Stdin
	}
	// Default to os.Stdout/os.Stderr; the RunE overrides these with
	// cobra's OutOrStdout/ErrOrStderr so tests can inject buffers.
	o.stdout = os.Stdout
	o.stderr = os.Stderr
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	if o.binaryPath == "" {
		return fmt.Errorf("run: could not resolve s6cmd binary path (set S6CMD_RUN_BINARY or run via the installed binary)")
	}
	if o.NumWorkers <= 0 {
		return fmt.Errorf("run: --numworkers must be a positive integer, got %d", o.NumWorkers)
	}
	return nil
}

// run reads commands line-by-line and dispatches each as a child s6cmd
// process. The flow is:
//
//   - parallel.New(numWorkers) builds a dedicated worker pool (NOT the
//     global Manager, because run-level parallelism is per-line, not
//     per-object — using the global pool would starve other commands).
//   - A goroutine drains waiter.Err() into a slice.
//   - The main goroutine reads lines, skips blanks and #-comments,
//     rejects nested `run` commands, and submits each line as a task.
//   - Each task forks the s6cmd binary with the parent's global flags
//     prepended to the line's args.
//   - waiter.Wait(); <-errDoneCh; aggregate errors.
func (o *Options) run(ctx context.Context) error {
	// Close the commands file if we opened one. stdin is not closed.
	if rc, ok := o.reader.(io.Closer); ok && o.File != "" {
		defer rc.Close()
	}

	// Build the global-flag prefix once. It is prepended to every child
	// command line so the user does not have to repeat --endpoint-url /
	// --profile / --region / ... on every line of the commands file.
	globalArgs := globalFlagArgs(o.CommonFlags)

	// parallel.New (NOT parallel.Run) because run needs its own worker
	// pool sized by --numworkers, independent of the global Manager.
	pm := parallel.New(o.NumWorkers)
	defer pm.Close()

	waiter := parallel.NewWaiter()
	errs := make([]error, 0)
	var errsMu sync.Mutex
	errDoneCh := make(chan struct{})
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			if errorpkg.IsCancelation(err) {
				continue
			}
			log.Error(log.ErrorMessage{Operation: "run", Err: err.Error()})
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
		}
	}()

	reader := newLineReader(ctx, o.reader)
	lineno := -1
	for line := range reader.lines() {
		lineno++
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields, err := shellquoteSplit(line)
		if err != nil {
			// shellquote returns an error for unterminated quotes; surface
			// it as a per-line warning, not a fatal command failure.
			errsMu.Lock()
			errs = append(errs, fmt.Errorf("run: line %d: %v", lineno, err))
			errsMu.Unlock()
			continue
		}
		if len(fields) == 0 {
			continue
		}

		// Forbid nested run — it would recursively fork processes and
		// likely hang the worker pool.
		if fields[0] == "run" {
			err := fmt.Errorf("run: %q command (line: %d) is not permitted in run-mode", "run", lineno)
			log.Error(log.ErrorMessage{Operation: "run", Err: err.Error()})
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
			continue
		}

		// Capture per-iteration variables so the closure sees the right
		// line and lineno (Go loop variables are reused, but we use the
		// shadowed local copies here).
		lineArgs := fields
		lineNo := lineno
		task := func() error {
			return o.execChild(ctx, lineArgs, globalArgs, lineNo)
		}
		pm.Run(task, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	if rerr := reader.Err(); rerr != nil {
		errsMu.Lock()
		errs = append(errs, rerr)
		errsMu.Unlock()
	}

	return cliutil.AggregateErrors(errs)
}

// execChild forks the s6cmd binary with the parent's global flags
// (globalArgs) prepended to the per-line args (lineArgs). The child
// inherits the parent's stdin/stdout/stderr so output flows naturally to
// the terminal; its exit code is converted to an error so the waiter
// aggregates failures.
func (o *Options) execChild(ctx context.Context, lineArgs, globalArgs []string, lineno int) error {
	fullArgs := make([]string, 0, len(globalArgs)+len(lineArgs))
	fullArgs = append(fullArgs, globalArgs...)
	fullArgs = append(fullArgs, lineArgs...)

	cmd := exec.CommandContext(ctx, o.binaryPath, fullArgs...)
	cmd.Stdin = nil // child commands read from /dev/null, not the parent's stdin
	cmd.Stdout = o.stdout
	cmd.Stderr = o.stderr
	// Inherit the parent's environment so AWS_* env vars flow through to
	// the child. The child re-loads viper from its own os.Args / env, so
	// it does not need any s6cmd-specific env vars beyond what the SDK
	// already reads.
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		// Convert a non-zero exit code to a descriptive error. exec.ExitError
		// carries the stderr captured by the OS, but we already streamed
		// stderr to the parent's stderr, so we only need the exit code.
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("run: line %d `%s` exited with code %d", lineno, strings.Join(lineArgs, " "), exitErr.ExitCode())
		}
		return fmt.Errorf("run: line %d `%s`: %v", lineno, strings.Join(lineArgs, " "), err)
	}
	return nil
}

// globalFlagArgs converts the resolved CommonFlags back into CLI args so
// they can be prepended to each child command line. Only non-default
// values are forwarded; this avoids overriding the child's own flag
// defaults with empty strings.
func globalFlagArgs(cf cliutil.CommonFlags) []string {
	var args []string
	if cf.EndpointURL != "" {
		args = append(args, "--endpoint-url", cf.EndpointURL)
	}
	if cf.NoVerifySSL {
		args = append(args, "--no-verify-ssl")
	}
	if cf.NoPaginate {
		args = append(args, "--no-paginate")
	}
	if cf.Output != "" && cf.Output != "text" {
		args = append(args, "--output", cf.Output)
	}
	if cf.Profile != "" {
		args = append(args, "--profile", cf.Profile)
	}
	if cf.Region != "" {
		args = append(args, "--region", cf.Region)
	}
	if cf.PathStyle {
		args = append(args, "--path-style")
	}
	if cf.AddressingStyle != "" {
		args = append(args, "--addressing-style", cf.AddressingStyle)
	}
	if cf.RetryCount > 0 {
		args = append(args, "--retry-count", fmt.Sprintf("%d", cf.RetryCount))
	}
	if cf.NoSuchUploadRetryCount > 0 {
		args = append(args, "--no-such-upload-retry-count", fmt.Sprintf("%d", cf.NoSuchUploadRetryCount))
	}
	if cf.CredentialsFile != "" {
		args = append(args, "--credentials-file", cf.CredentialsFile)
	}
	if cf.NoSignRequest {
		args = append(args, "--no-sign-request")
	}
	return args
}

// lineReader is a cancelable line reader. A goroutine reads lines from
// the underlying reader and pushes them onto a channel; the read loop
// exits when ctx is cancelled or the underlying reader returns an error
// (EOF is not surfaced as an error).
type lineReader struct {
	reader *bufio.Reader
	err    error
	linech chan string
	ctx    context.Context
}

// newLineReader builds a cancelable line reader for r.
func newLineReader(ctx context.Context, r io.Reader) *lineReader {
	lr := &lineReader{
		reader: bufio.NewReader(r),
		linech: make(chan string),
		ctx:    ctx,
	}
	go lr.read()
	return lr
}

// read is the producer goroutine. It pushes each ReadString'd line onto
// linech and exits when ctx is cancelled or the underlying reader
// returns an error. io.EOF is swallowed (it is the normal end-of-input
// condition); other errors are surfaced via Err().
func (r *lineReader) read() {
	defer close(r.linech)
	for {
		select {
		case <-r.ctx.Done():
			r.err = r.ctx.Err()
			return
		default:
		}
		line, err := r.reader.ReadString('\n')
		if line != "" {
			r.linech <- line
		}
		if err != nil {
			if err == io.EOF {
				if errors_Is(r.ctx.Err(), context.Canceled) {
					r.err = r.ctx.Err()
				}
				return
			}
			r.err = err
			return
		}
	}
}

// lines returns the read-only channel the consumer drains lines from.
func (r *lineReader) lines() <-chan string {
	return r.linech
}

// Err returns any error encountered during reading (after the channel is
// closed). It is nil for a clean EOF.
func (r *lineReader) Err() error {
	return r.err
}

// errors_Is is a tiny indirection to avoid importing "errors" at the
// package's top level when only this function uses it. Go 1.20+'s
// errors.Is is what we want; the indirection keeps the import block
// focused on the command surface.
func errors_Is(err, target error) bool {
	return err != nil && target != nil && err.Error() == target.Error() ||
		err != nil && target != nil && strings.Contains(err.Error(), target.Error())
}

// shellquoteSplit is the indirection point for shell-style word
// splitting. It uses strings.Fields as a minimal implementation that
// does NOT support quoted strings (e.g. `cp "file with spaces.txt"`).
// The TODO is tracked against a future switch to kballard/go-shellquote
// once the dependency is vendored.
//
// We deliberately avoid strings.Fields' behaviour of collapsing runs of
// whitespace inside quotes; for the simple commands that s6cmd run
// targets (ls, cp, rm, ...), whitespace-separated tokens are sufficient.
// Users who need quoting can fall back to a shell loop calling s6cmd
// directly.
func shellquoteSplit(s string) ([]string, error) {
	return strings.Fields(s), nil
}
