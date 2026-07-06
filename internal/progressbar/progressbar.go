// Package progressbar defines the progress-reporting interface used by
// commands and a minimal terminal implementation with no external
// dependencies. New returns the real bar only when progress was requested
// AND stderr is a terminal; otherwise it returns a NoOp so redirected or
// scripted runs never see control characters.
package progressbar

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinPr/s6cmd/strutil"
	"golang.org/x/term"
)

// ProgressBar is the surface commands depend on. It tracks both byte-level
// and object-level progress so callers can report whichever makes sense
// for their operation.
type ProgressBar interface {
	// Start begins progress reporting. It is called once before the
	// first work unit is processed.
	Start()
	// Finish stops progress reporting and flushes any final state.
	Finish()
	// IncrementCompletedObjects advances the count of completed
	// objects by one.
	IncrementCompletedObjects()
	// IncrementTotalObjects advances the count of total objects by
	// one (used when the total is not known up front).
	IncrementTotalObjects()
	// AddCompletedBytes adds n to the count of completed bytes.
	AddCompletedBytes(n int64)
	// AddTotalBytes adds n to the count of total bytes.
	AddTotalBytes(n int64)
}

// NoOp is a ProgressBar that does nothing. It is returned by New when the
// bar would not be visible (flag not set, or stderr is not a terminal).
type NoOp struct{}

// Start implements ProgressBar.
func (pb *NoOp) Start() {}

// Finish implements ProgressBar.
func (pb *NoOp) Finish() {}

// IncrementCompletedObjects implements ProgressBar.
func (pb *NoOp) IncrementCompletedObjects() {}

// IncrementTotalObjects implements ProgressBar.
func (pb *NoOp) IncrementTotalObjects() {}

// AddCompletedBytes implements ProgressBar.
func (pb *NoOp) AddCompletedBytes(n int64) {}

// AddTotalBytes implements ProgressBar.
func (pb *NoOp) AddTotalBytes(n int64) {}

// renderInterval is how often the terminal bar repaints. 5 Hz is smooth
// enough for a transfer display and cheap enough to never matter.
const renderInterval = 200 * time.Millisecond

// Bar is a minimal terminal progress bar. Counter updates are atomic (they
// arrive from many transfer goroutines); a single render goroutine repaints
// the current line on stderr with \r at a fixed interval.
type Bar struct {
	out io.Writer

	totalObjects     atomic.Int64
	completedObjects atomic.Int64
	totalBytes       atomic.Int64
	completedBytes   atomic.Int64

	startOnce  sync.Once
	finishOnce sync.Once
	done       chan struct{}
	renderDone chan struct{}

	// lastLen is the rune length of the last rendered line, used to blank
	// leftovers when a new line is shorter. Only the render goroutine and
	// Finish (after the render goroutine exited) touch it.
	lastLen int
}

// NewBar returns a Bar writing to out. Callers normally use New, which
// picks between Bar and NoOp; NewBar is exported for tests.
func NewBar(out io.Writer) *Bar {
	return &Bar{
		out:        out,
		done:       make(chan struct{}),
		renderDone: make(chan struct{}),
	}
}

// Start launches the render goroutine. It is idempotent.
func (b *Bar) Start() {
	b.startOnce.Do(func() {
		go func() {
			defer close(b.renderDone)
			ticker := time.NewTicker(renderInterval)
			defer ticker.Stop()
			for {
				select {
				case <-b.done:
					return
				case <-ticker.C:
					b.render()
				}
			}
		}()
	})
}

// Finish stops the render goroutine, paints the final state and moves to a
// fresh line so subsequent output does not overwrite the bar. Idempotent.
func (b *Bar) Finish() {
	b.finishOnce.Do(func() {
		close(b.done)
		<-b.renderDone
		b.render()
		fmt.Fprintln(b.out)
	})
}

// IncrementCompletedObjects implements ProgressBar.
func (b *Bar) IncrementCompletedObjects() { b.completedObjects.Add(1) }

// IncrementTotalObjects implements ProgressBar.
func (b *Bar) IncrementTotalObjects() { b.totalObjects.Add(1) }

// AddCompletedBytes implements ProgressBar.
func (b *Bar) AddCompletedBytes(n int64) { b.completedBytes.Add(n) }

// AddTotalBytes implements ProgressBar.
func (b *Bar) AddTotalBytes(n int64) { b.totalBytes.Add(n) }

// render repaints the progress line in place with \r. It never emits a
// newline; Finish adds the terminating one.
func (b *Bar) render() {
	line := b.line()
	// Blank the tail when the previous line was longer.
	pad := ""
	if d := b.lastLen - len(line); d > 0 {
		pad = fmt.Sprintf("%*s", d, "")
	}
	b.lastLen = len(line)
	fmt.Fprintf(b.out, "\r%s%s", line, pad)
}

// line formats the current progress state, e.g.
//
//	12/40 objects, 128.0M/512.3M bytes (25%)
func (b *Bar) line() string {
	completedObj := b.completedObjects.Load()
	totalObj := b.totalObjects.Load()
	completedBytes := b.completedBytes.Load()
	totalBytes := b.totalBytes.Load()

	percent := int64(0)
	if totalBytes > 0 {
		percent = completedBytes * 100 / totalBytes
		if percent > 100 {
			percent = 100
		}
	}
	return fmt.Sprintf("%d/%d objects, %s/%s bytes (%d%%)",
		completedObj, totalObj,
		strutil.HumanizeBytes(completedBytes), strutil.HumanizeBytes(totalBytes),
		percent)
}

// New returns a ProgressBar. The real terminal bar is returned only when
// isVisible is true (the user passed --show-progress on an applicable
// transfer) and stderr is a terminal; every other combination gets a NoOp
// so logs and pipes stay clean. The bar renders to STDERR because stdout
// carries the command's real output (per-object log lines, JSON).
func New(isVisible bool) ProgressBar {
	if !isVisible || !term.IsTerminal(int(os.Stderr.Fd())) {
		return &NoOp{}
	}
	return NewBar(os.Stderr)
}
