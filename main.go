package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/LinPr/s6cmd/cmd"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
)

func main() {
	// Cancel the root context on SIGINT/SIGTERM so in-flight transfers
	// stop cooperatively (temp files cleaned up, multipart uploads
	// aborted) instead of the process being hard-killed mid-write. The
	// context flows to every subcommand via cmd.Context().
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Two-Ctrl-C contract: the FIRST signal cancels the context for a
	// graceful shutdown; once that happened, stop() unregisters the
	// handler and restores the default signal disposition, so a SECOND
	// Ctrl-C hard-kills the process. Without this, signal.NotifyContext
	// keeps swallowing every subsequent SIGINT for the whole process
	// lifetime, and any code path blocked outside the context (a bufio
	// read on stdin/fifo, a terminal confirmation prompt) could only be
	// stopped with SIGKILL.
	go func() {
		<-ctx.Done()
		stop()
	}()

	// Initialize process-wide infrastructure before any command runs.
	// parallel.Init raises the soft RLIMIT_NOFILE and constructs the
	// global Manager so parallel.Run does not panic. The logger itself is
	// configured in the root command's PersistentPreRunE (after flag
	// parsing, so --log/--output are honoured); logging before that is
	// safe because the log package lazily creates a default logger.
	// Per-operation statistics are opt-in via the root --stat flag, which
	// calls stat.InitStat from PersistentPreRunE.
	fdErr := parallel.Init(0)

	// Raising the file-descriptor limit is best-effort; surface a failure
	// at debug level so verbose runs can see why highly concurrent
	// transfers might hit "too many open files".
	if fdErr != nil {
		log.Debug(log.DebugMessage{Err: fmt.Sprintf("could not raise open file limit: %v", fdErr)})
	}

	code := cmd.Execute(ctx)

	// Stop the signal handler and flush queued log lines before exiting:
	// os.Exit does not run deferred functions, so an explicit Close is
	// the only way queued messages are guaranteed to reach stdout/stderr.
	stop()
	log.Close()
	os.Exit(code)
}
