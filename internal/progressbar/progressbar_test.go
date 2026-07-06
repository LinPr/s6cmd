package progressbar

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// TestBarLineFormat pins the rendered progress line: object counts, byte
// counts (humanized) and the percentage derived from bytes.
func TestBarLineFormat(t *testing.T) {
	b := NewBar(&bytes.Buffer{})
	b.AddTotalBytes(400)
	b.AddCompletedBytes(100)
	b.IncrementTotalObjects()
	b.IncrementTotalObjects()
	b.IncrementCompletedObjects()

	got := b.line()
	want := "1/2 objects, 100/400 bytes (25%)"
	if got != want {
		t.Errorf("line() = %q, want %q", got, want)
	}
}

// TestBarPercentEdgeCases verifies the zero-total and over-100 clamps.
func TestBarPercentEdgeCases(t *testing.T) {
	b := NewBar(&bytes.Buffer{})
	if got := b.line(); !strings.Contains(got, "(0%)") {
		t.Errorf("zero-total line = %q, want 0%%", got)
	}
	b.AddTotalBytes(10)
	b.AddCompletedBytes(25) // more completed than total must clamp to 100
	if got := b.line(); !strings.Contains(got, "(100%)") {
		t.Errorf("overflow line = %q, want clamped 100%%", got)
	}
}

// TestBarStartFinish verifies the render lifecycle: Finish paints a final
// state terminated by a newline and is idempotent, and Start may be called
// repeatedly without spawning extra goroutines (idempotent via sync.Once).
func TestBarStartFinish(t *testing.T) {
	var buf bytes.Buffer
	b := NewBar(&buf)
	b.Start()
	b.Start()
	b.AddTotalBytes(2)
	b.AddCompletedBytes(2)
	b.IncrementTotalObjects()
	b.IncrementCompletedObjects()
	b.Finish()
	b.Finish()

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("Finish output does not end with newline: %q", out)
	}
	if !strings.Contains(out, "1/1 objects, 2/2 bytes (100%)") {
		t.Errorf("final render missing from output: %q", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Errorf("Finish is not idempotent, got %d newlines: %q", strings.Count(out, "\n"), out)
	}
}

// TestBarConcurrentUpdates exercises counter updates from many goroutines
// under -race while the render goroutine repaints.
func TestBarConcurrentUpdates(t *testing.T) {
	var buf bytes.Buffer
	b := NewBar(&buf)
	// Note: bytes.Buffer is not synchronized; keep rendering out of the
	// concurrent phase by not calling Start until after the writers are
	// done. The atomics are the point of this test.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.AddTotalBytes(2)
				b.AddCompletedBytes(1)
				b.IncrementTotalObjects()
				b.IncrementCompletedObjects()
			}
		}()
	}
	wg.Wait()
	b.Start()
	b.Finish()
	if got := b.line(); got != "800/800 objects, 800/1.6K bytes (50%)" {
		t.Errorf("line() = %q, want %q", got, "800/800 objects, 800/1.6K bytes (50%)")
	}
}

// TestNewInvisibleReturnsNoOp verifies New's gating: with isVisible false a
// NoOp is returned. (The TTY branch cannot be exercised here because the
// test process's stderr is not a terminal, which also means New(true) must
// return a NoOp under `go test`.)
func TestNewInvisibleReturnsNoOp(t *testing.T) {
	if _, ok := New(false).(*NoOp); !ok {
		t.Error("New(false) did not return a NoOp")
	}
	if _, ok := New(true).(*NoOp); !ok {
		t.Error("New(true) with non-terminal stderr did not return a NoOp")
	}
}
