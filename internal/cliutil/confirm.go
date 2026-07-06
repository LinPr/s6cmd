package cliutil

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/term"
)

// ErrConfirmationDeclined is returned by Confirm when an interactive user
// answers the prompt with anything other than y/yes. Callers surface it as
// a command error so a declined destructive operation exits non-zero
// instead of masquerading as success.
var ErrConfirmationDeclined = errors.New("operation not confirmed")

// fdReader is the subset of *os.File Confirm needs to decide whether the
// input is a terminal.
type fdReader interface {
	io.Reader
	Fd() uintptr
}

// Confirm asks the user to approve a destructive operation. It writes
// prompt to out, reads one line from in and returns nil only for an
// explicit y/yes answer.
//
// When in is not a terminal (a pipe, a file, or a non-file reader), Confirm
// refuses to prompt and returns an error telling the user to pass --yes: a
// blind read on piped stdin would silently eat a line of the pipeline's
// data, and an EOF would previously be treated as "no" with a zero exit
// code, making scripts believe the operation ran.
//
// The terminal read runs in a goroutine so a canceled ctx (Ctrl-C on the
// signal-aware root context) aborts the prompt instead of blocking until
// the user types a line: the blocking read used to make the prompt immune
// to SIGINT, forcing a SIGKILL. A cancelation is reported as an error, so
// the destructive operation is declined, never approved by default. The
// reader goroutine itself stays blocked on the terminal until the process
// exits; that is fine for a CLI whose next Ctrl-C hard-kills it.
func Confirm(ctx context.Context, in io.Reader, out io.Writer, prompt string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("confirmation aborted: %w", err)
	}
	f, ok := in.(fdReader)
	if !ok || !term.IsTerminal(int(f.Fd())) {
		return errors.New("refusing to prompt for confirmation: standard input is not a terminal (use --yes to proceed)")
	}
	fmt.Fprintf(out, "%s [y/N]: ", prompt)

	type answer struct {
		line string
		err  error
	}
	// Buffered so the reader goroutine can deliver (and exit) even when
	// the ctx branch won the select.
	ch := make(chan answer, 1)
	go func() {
		line, err := bufio.NewReader(in).ReadString('\n')
		ch <- answer{line, err}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("confirmation aborted: %w", ctx.Err())
	case a := <-ch:
		if a.err != nil && a.line == "" {
			return fmt.Errorf("read confirmation: %w", a.err)
		}
		line := strings.TrimSpace(a.line)
		if strings.EqualFold(line, "y") || strings.EqualFold(line, "yes") {
			return nil
		}
		return ErrConfirmationDeclined
	}
}
