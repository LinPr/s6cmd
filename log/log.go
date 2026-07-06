// Package log provides a synchronized logger that fans messages from many
// worker goroutines onto a single output channel. All log calls push onto a
// buffered channel owned by the active Logger; a single goroutine drains
// the channel and writes to stdout/stderr so concurrent writes never
// interleave.
//
// The package exposes the same shape (Info/Debug/Trace/Error/Stat +
// Message interface) but drops the dependency on the url package by
// typing Source/Destination as strings.
//
// Lifecycle: Init configures the global logger (level + JSON mode) and
// Close flushes it. Both are optional — any log call before Init lazily
// creates a default logger (LevelInfo, plain text) so early-startup code
// can log safely, and messages arriving after Close are written directly
// instead of being dropped.
package log

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/LinPr/s6cmd/log/stat"
)

// output is an internal container for messages queued for writing.
type output struct {
	std     *os.File
	message string
}

var (
	// global holds the active logger. It is an atomic pointer so log
	// calls can read it without taking initMu, which removes the race
	// between Init/Close swapping the logger and workers queueing
	// messages.
	global atomic.Pointer[Logger]
	// initMu serializes Init, Close and the lazy default creation in
	// logger() so two concurrent first-loggers cannot both be stored.
	initMu sync.Mutex
)

// logger returns the active global logger, lazily creating a default one
// (LevelInfo, plain text) when Init has not been called yet. This makes
// Trace/Debug/Info/Error/Stat safe to call at any point in the process
// lifetime.
func logger() *Logger {
	if l := global.Load(); l != nil {
		return l
	}
	initMu.Lock()
	defer initMu.Unlock()
	if l := global.Load(); l != nil {
		return l
	}
	l := New(LevelInfo, false)
	global.Store(l)
	return l
}

// Init configures the global logger with the given level and JSON mode.
// Calling Init twice (or after a lazy default logger was created) replaces
// the global logger; the previous one is flushed and closed first.
func Init(level Level, json bool) {
	initMu.Lock()
	defer initMu.Unlock()

	if old := global.Load(); old != nil {
		// Flush the previous logger before swapping so messages
		// already queued are not lost.
		old.Close()
	}
	global.Store(New(level, json))
}

// Close drains the global logger's channel and waits for the drain
// goroutine to exit. It is a no-op if no logger is active.
//
// The closed logger stays installed: its enqueue degrades to synchronous
// direct writes once closed, so late messages are still written. Storing
// nil instead would make the next log call lazily create a fresh logger
// whose buffered channel is never drained at exit, silently dropping those
// messages.
func Close() {
	initMu.Lock()
	defer initMu.Unlock()

	if old := global.Load(); old != nil {
		old.Close()
	}
}

// Trace queues a trace-level message to stdout.
func Trace(msg Message) {
	logger().printf(LevelTrace, msg, os.Stdout)
}

// Debug queues a debug-level message to stdout.
func Debug(msg Message) {
	logger().printf(LevelDebug, msg, os.Stdout)
}

// Info queues an info-level message to stdout. Operation-attributed
// messages also feed the --stat counters (a no-op unless stat collection
// was enabled).
func Info(msg Message) {
	collectStat(msg, true)
	logger().printf(LevelInfo, msg, os.Stdout)
}

// Stat queues a stat message to stdout regardless of the configured log
// level (it bypasses the level check so statistics are always emitted).
func Stat(msg Message) {
	logger().printfBypass(LevelInfo, msg, os.Stdout)
}

// Error queues an error-level message to stderr. Operation-attributed
// messages also feed the --stat counters (a no-op unless stat collection
// was enabled).
func Error(msg Message) {
	collectStat(msg, false)
	logger().printf(LevelError, msg, os.Stderr)
}

// collectStat feeds the --stat per-operation counters. Every per-object
// success/failure already flows through exactly one Info or Error call
// carrying an Operation name ("cp", "rm", ...), so collecting here means
// no per-command wiring. Messages without an Operation (config debug
// lines, command-level errors) are not counted.
func collectStat(msg Message, success bool) {
	switch m := msg.(type) {
	case InfoMessage:
		if m.Operation != "" {
			stat.Add(m.Operation, success)
		}
	case ErrorMessage:
		if m.Operation != "" {
			stat.Add(m.Operation, success)
		}
	}
}

// Logger is a struct holding logger configuration, the message channel it
// owns and the drain goroutine lifecycle channel.
type Logger struct {
	// ch is the buffered message channel; it is owned by this Logger so
	// swapping the global logger never leaves a writer holding a stale,
	// package-level channel.
	ch     chan output
	donech chan struct{}
	json   bool
	level  Level

	// mu guards closed. enqueue holds the read lock while sending so
	// Close (which takes the write lock) cannot close ch mid-send.
	mu     sync.RWMutex
	closed bool
}

// New creates a new Logger and starts its drain goroutine.
func New(level Level, json bool) *Logger {
	logger := &Logger{
		ch:     make(chan output, 10000),
		donech: make(chan struct{}),
		json:   json,
		level:  level,
	}
	go logger.out()
	return logger
}

// Close flushes queued messages and stops the drain goroutine. It is
// idempotent; late messages arriving after Close are written directly by
// enqueue instead of being dropped.
func (l *Logger) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		<-l.donech
		return
	}
	l.closed = true
	l.mu.Unlock()
	// No sender can be inside enqueue's critical section here (the write
	// lock above waited them out), so closing ch is safe.
	close(l.ch)
	<-l.donech
}

// printf queues message onto the logger's channel if the logger's level
// permits it.
func (l *Logger) printf(level Level, message Message, std *os.File) {
	if level < l.level {
		return
	}
	l.printfBypass(level, message, std)
}

// printfBypass queues message onto the logger's channel without checking
// the level.
func (l *Logger) printfBypass(level Level, message Message, std *os.File) {
	var msg string
	if l.json {
		msg = message.JSON()
	} else {
		msg = fmt.Sprintf("%v%v", level, message.String())
	}
	l.enqueue(output{std: std, message: msg})
}

// enqueue pushes o onto the channel; if the channel is full this blocks
// until the drain goroutine catches up. That is acceptable at
// startup-shutdown boundaries but should be rare in steady state because
// the channel is large. When the logger is already closed the message is
// written synchronously so nothing is lost (and nothing panics).
func (l *Logger) enqueue(o output) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		_, _ = fmt.Fprintln(o.std, o.message)
		return
	}
	l.ch <- o
}

// out drains the channel and writes each entry to its target file. It
// exits when the channel is closed.
func (l *Logger) out() {
	defer close(l.donech)
	for o := range l.ch {
		_, _ = fmt.Fprintln(o.std, o.message)
	}
}

// Level is the granularity of logging.
type Level int

const (
	// LevelTrace is the most verbose level, used for aws-sdk-go-v2
	// request traces.
	LevelTrace Level = iota
	// LevelDebug surfaces internal command decisions.
	LevelDebug
	// LevelInfo is the default user-facing level.
	LevelInfo
	// LevelError is for failures.
	LevelError
)

// LevelFromString maps the --log flag values (trace, debug, info, error)
// to a Level. The second return value is false for unknown names.
func LevelFromString(s string) (Level, bool) {
	switch strings.ToLower(s) {
	case "trace":
		return LevelTrace, true
	case "debug":
		return LevelDebug, true
	case "info":
		return LevelInfo, true
	case "error":
		return LevelError, true
	default:
		return LevelInfo, false
	}
}

// String returns the prefix used for messages at this level. Trace and Info
// add no prefix (the SDK already prefixes trace lines and Info is the
// default) so the common case stays clean.
func (l Level) String() string {
	switch l {
	case LevelInfo:
		return ""
	case LevelError:
		return "ERROR "
	case LevelDebug:
		return "DEBUG "
	case LevelTrace:
		// Used for aws-sdk-go-v2 traces, which already add a DEBUG
		// prefix; adding another would look weird.
		return ""
	default:
		return "UNKNOWN "
	}
}
