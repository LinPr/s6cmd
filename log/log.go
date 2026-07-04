// Package log provides a synchronized logger that fans messages from many
// worker goroutines onto a single output channel. All log calls push onto a
// globally shared, buffered channel (outputCh); a single goroutine drains
// the channel and writes to stdout/stderr so concurrent writes never
// interleave.
//
// The package mirrors s5cmd/log so callers can keep the same shape
// (Info/Debug/Trace/Error/Stat + Message interface) but drops the dependency
// on the url package by typing Source/Destination as strings.
package log

import (
	"fmt"
	"os"
	"sync"
)

// output is an internal container for messages queued for writing.
type output struct {
	std     *os.File
	message string
}

// outputCh is the global channel used to synchronize writes to standard
// output. It is buffered so bursts of log messages do not block workers
// while the drain goroutine catches up.
var outputCh = make(chan output, 10000)

var (
	global    *Logger
	globalMu  sync.Mutex
)

// Init configures the global logger with the given level and JSON mode. It
// must be called once at startup before any Trace/Debug/Info/Error/Stat
// call. Calling Init twice replaces the global logger; the previous drain
// goroutine is closed first.
func Init(level Level, json bool) {
	globalMu.Lock()
	defer globalMu.Unlock()

	if global != nil {
		// Drain the previous logger before swapping so messages
		// already queued are flushed.
		close(outputCh)
		<-global.donech
		outputCh = make(chan output, 10000)
	}
	global = New(level, json)
}

// Close drains the global logger's channel and waits for the drain
// goroutine to exit. It is a no-op if Init was never called.
func Close() {
	globalMu.Lock()
	defer globalMu.Unlock()

	if global == nil {
		return
	}
	close(outputCh)
	<-global.donech
	global = nil
}

// Trace queues a trace-level message to stdout.
func Trace(msg Message) {
	global.printf(LevelTrace, msg, os.Stdout)
}

// Debug queues a debug-level message to stdout.
func Debug(msg Message) {
	global.printf(LevelDebug, msg, os.Stdout)
}

// Info queues an info-level message to stdout.
func Info(msg Message) {
	global.printf(LevelInfo, msg, os.Stdout)
}

// Stat queues a stat message to stdout regardless of the configured log
// level (it bypasses the level check so statistics are always emitted).
func Stat(msg Message) {
	global.printfBypass(LevelInfo, msg, os.Stdout)
}

// Error queues an error-level message to stderr.
func Error(msg Message) {
	global.printf(LevelError, msg, os.Stderr)
}

// Logger is a struct holding logger configuration and the drain goroutine
// lifecycle channel.
type Logger struct {
	donech chan struct{}
	json   bool
	level  Level
}

// New creates a new Logger and starts its drain goroutine.
func New(level Level, json bool) *Logger {
	logger := &Logger{
		donech: make(chan struct{}),
		json:   json,
		level:  level,
	}
	go logger.out()
	return logger
}

// printf queues message onto outputCh if the logger's level permits it.
func (l *Logger) printf(level Level, message Message, std *os.File) {
	if level < l.level {
		return
	}
	l.printfBypass(level, message, std)
}

// printfBypass queues message onto outputCh without checking the level.
func (l *Logger) printfBypass(level Level, message Message, std *os.File) {
	var msg string
	if l.json {
		msg = message.JSON()
	} else {
		msg = fmt.Sprintf("%v%v", level, message.String())
	}
	// Push onto the channel; if the channel is full this will block
	// until the drain goroutine catches up. That is acceptable at
	// startup-shutdown boundaries but should be rare in steady state
	// because the channel is large.
	outputCh <- output{std: std, message: msg}
}

// out drains outputCh and writes each entry to its target file. It exits
// when outputCh is closed.
func (l *Logger) out() {
	defer close(l.donech)
	for o := range outputCh {
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
