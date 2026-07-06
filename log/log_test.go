package log

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
)

// devNull returns a writable *os.File pointing at the OS null device so
// tests can exercise the full enqueue/drain pipeline without polluting the
// test output.
func devNull(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestLogBeforeInitDoesNotPanic verifies the nil-guard: package-level log
// calls before Init must lazily create a default logger instead of
// panicking on a nil global.
func TestLogBeforeInitDoesNotPanic(t *testing.T) {
	Close() // ensure no logger is active
	defer Close()

	// Debug is below the lazy default's Info level, so nothing is
	// printed — but the call must not panic and must create the logger.
	Debug(DebugMessage{Err: "before init"})
	Trace(TraceMessage{Message: "before init"})
	if global.Load() == nil {
		t.Fatal("lazy default logger was not created")
	}
}

// TestCloseIsIdempotent verifies Close (package-level and Logger method)
// can be called repeatedly, including without a prior Init.
func TestCloseIsIdempotent(t *testing.T) {
	Close()
	Close()
	Init(LevelInfo, false)
	Close()
	Close()

	l := New(LevelInfo, false)
	l.Close()
	l.Close()
}

// TestLoggerEnqueueAfterClose verifies that a message queued after Close
// is written directly instead of panicking on a closed channel.
func TestLoggerEnqueueAfterClose(t *testing.T) {
	null := devNull(t)
	l := New(LevelInfo, false)
	l.Close()
	// Must not panic; the message goes straight to the file.
	l.printf(LevelInfo, InfoMessage{Operation: "test", Source: "late"}, null)
}

// TestGlobalLogAfterCloseIsWritten verifies the package-level lifecycle
// contract: a message logged after Close is still written. Close must leave
// the closed logger installed (its enqueue degrades to synchronous direct
// writes); storing nil would make the next log call lazily create a fresh
// logger whose buffered channel is never drained at exit, silently dropping
// the message.
func TestGlobalLogAfterCloseIsWritten(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	saved := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = saved }()

	Init(LevelInfo, false)
	Close()

	before := global.Load()
	if before == nil {
		t.Fatal("Close must leave the closed logger installed, got nil")
	}
	// This message arrives after Close; the closed logger writes it
	// synchronously, so it is on the pipe before Info returns.
	Info(InfoMessage{Operation: "late", Source: "after-close"})
	if global.Load() != before {
		t.Fatal("a log call after Close must not install a fresh logger")
	}

	os.Stdout = saved
	w.Close()
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	if !strings.Contains(string(data), "after-close") {
		t.Fatalf("message logged after Close was lost; output: %q", string(data))
	}
}

// TestLoggerConcurrentPrintfAndClose exercises the send-vs-close race with
// the race detector: many writers printf while Close swaps the closed flag
// and closes the channel. Before the fix this could panic with "send on
// closed channel" or race on the package-level channel swap.
func TestLoggerConcurrentPrintfAndClose(t *testing.T) {
	null := devNull(t)
	l := New(LevelInfo, false)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				l.printf(LevelInfo, InfoMessage{Operation: "test", Source: "src"}, null)
			}
		}()
	}
	l.Close()
	wg.Wait()
}

// TestGlobalInitCloseRace exercises the package-level Init/Close swap while
// writers use the package functions. os.Stdout is redirected to the null
// device for the duration so the drained messages do not pollute the test
// output.
func TestGlobalInitCloseRace(t *testing.T) {
	null := devNull(t)
	saved := os.Stdout
	os.Stdout = null
	defer func() { os.Stdout = saved }()

	Close()
	Init(LevelInfo, false)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				Info(InfoMessage{Operation: "test", Source: "src"})
				Stat(InfoMessage{Operation: "stat", Source: "src"})
			}
		}()
	}
	// Swap the logger mid-stream and close it while writers are active.
	Init(LevelInfo, false)
	Close()
	wg.Wait()
	Close()
}

// captureLogger runs fn against a fresh Logger whose output is redirected
// into an os.Pipe, closes the logger (flushing the queue) and returns
// everything that was written.
func captureLogger(t *testing.T, level Level, json bool, fn func(l *Logger, out *os.File)) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	l := New(level, json)
	fn(l, w)
	l.Close()
	w.Close()
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return string(data)
}

// TestLoggerLevelFiltering verifies that messages below the configured
// level are dropped and messages at or above it are written.
func TestLoggerLevelFiltering(t *testing.T) {
	out := captureLogger(t, LevelError, false, func(l *Logger, w *os.File) {
		l.printf(LevelTrace, TraceMessage{Message: "trace-line"}, w)
		l.printf(LevelDebug, DebugMessage{Err: "debug-line"}, w)
		l.printf(LevelInfo, InfoMessage{Operation: "cp", Source: "info-line"}, w)
		l.printf(LevelError, ErrorMessage{Operation: "cp", Err: "error-line"}, w)
	})
	for _, dropped := range []string{"trace-line", "debug-line", "info-line"} {
		if strings.Contains(out, dropped) {
			t.Errorf("message %q below LevelError was not filtered; output: %q", dropped, out)
		}
	}
	if !strings.Contains(out, "error-line") {
		t.Errorf("error message missing from output %q", out)
	}
}

// TestLoggerStatBypassesLevel verifies printfBypass (the log.Stat path)
// emits even when the message level is below the configured threshold, so
// the --stat summary is never filtered away.
func TestLoggerStatBypassesLevel(t *testing.T) {
	out := captureLogger(t, LevelError, false, func(l *Logger, w *os.File) {
		l.printfBypass(LevelInfo, InfoMessage{Operation: "stat", Source: "summary"}, w)
	})
	if !strings.Contains(out, "summary") {
		t.Errorf("stat message filtered by level; output: %q", out)
	}
}

// TestLoggerJSONMode verifies that JSON mode writes one parseable JSON
// object per message with the Message's JSON() payload.
func TestLoggerJSONMode(t *testing.T) {
	out := captureLogger(t, LevelInfo, true, func(l *Logger, w *os.File) {
		l.printf(LevelInfo, InfoMessage{Operation: "cp", Source: "s3://b/k", Destination: "/tmp/k"}, w)
		l.printf(LevelError, ErrorMessage{Operation: "rm", Err: "boom"}, w)
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2; output: %q", len(lines), out)
	}
	var info struct {
		Operation string `json:"operation"`
		Success   bool   `json:"success"`
		Source    string `json:"source"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &info); err != nil {
		t.Fatalf("line 1 is not JSON: %v (%q)", err, lines[0])
	}
	if info.Operation != "cp" || !info.Success || info.Source != "s3://b/k" {
		t.Errorf("unexpected info payload: %+v", info)
	}
	var errMsg struct {
		Operation string `json:"operation"`
		Err       string `json:"error"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &errMsg); err != nil {
		t.Fatalf("line 2 is not JSON: %v (%q)", err, lines[1])
	}
	if errMsg.Operation != "rm" || errMsg.Err != "boom" {
		t.Errorf("unexpected error payload: %+v", errMsg)
	}
}

// TestLevelFromString verifies the --log flag value mapping.
func TestLevelFromString(t *testing.T) {
	cases := []struct {
		in    string
		want  Level
		valid bool
	}{
		{"trace", LevelTrace, true},
		{"debug", LevelDebug, true},
		{"info", LevelInfo, true},
		{"INFO", LevelInfo, true},
		{"Error", LevelError, true},
		{"verbose", LevelInfo, false},
		{"", LevelInfo, false},
	}
	for _, c := range cases {
		got, ok := LevelFromString(c.in)
		if got != c.want || ok != c.valid {
			t.Errorf("LevelFromString(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.valid)
		}
	}
}
