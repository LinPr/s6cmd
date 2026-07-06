package cliutil

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// TestConfirmNonInteractive verifies that Confirm refuses to prompt when
// stdin is not a terminal: a blind ReadString on piped stdin used to eat a
// line of the pipeline's data, and EOF was treated as "no" with a ZERO
// exit code, so scripts believed the destructive operation ran.
func TestConfirmNonInteractive(t *testing.T) {
	var out bytes.Buffer

	// A plain bytes.Reader has no Fd() at all.
	err := Confirm(context.Background(), strings.NewReader("y\n"), &out, "Continue?")
	if err == nil {
		t.Fatalf("Confirm(non-tty reader) = nil, want refusal error")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Errorf("error %q should tell the user to pass --yes", err)
	}
	// The refusal must not consume input or print the prompt.
	if out.Len() != 0 {
		t.Errorf("Confirm wrote %q to out before refusing; want nothing", out.String())
	}
}

// TestConfirmPipeFd verifies the refusal also triggers for a reader that
// HAS an Fd but is not a terminal (the piped-stdin case).
func TestConfirmPipeFd(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer r.Close()
	if _, err := w.WriteString("y\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	w.Close()

	var out bytes.Buffer
	if err := Confirm(context.Background(), r, &out, "Continue?"); err == nil {
		t.Fatalf("Confirm(pipe) = nil, want refusal error")
	}
	// The piped "y" must NOT have been consumed.
	buf := make([]byte, 2)
	n, _ := r.Read(buf)
	if got := string(buf[:n]); got != "y\n" {
		t.Errorf("piped data was consumed by the refused Confirm; residual read = %q, want %q", got, "y\n")
	}
}

// TestConfirmCanceledContext verifies that a canceled context aborts the
// prompt with an error (so the destructive operation is declined, never
// approved by default). The blocking terminal read used to make Confirm
// immune to SIGINT: signal cancelation had no way to interrupt it and the
// process had to be SIGKILLed. The pre-canceled ctx is checked before the
// terminal test, so this holds for any reader.
func TestConfirmCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out bytes.Buffer
	err := Confirm(ctx, strings.NewReader("y\n"), &out, "Continue?")
	if err == nil {
		t.Fatalf("Confirm(canceled ctx) = nil, want abort error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %v should wrap context.Canceled", err)
	}
	if err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Errorf("error %q should say the confirmation was aborted", err)
	}
	// Nothing must have been written or consumed: the prompt never ran.
	if out.Len() != 0 {
		t.Errorf("Confirm wrote %q to out for a canceled ctx; want nothing", out.String())
	}
}
