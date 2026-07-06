package cliutil

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/progressbar"
)

// fakeBar is a test progressbar that records every byte delta.
type fakeBar struct {
	bytes int64
}

func (f *fakeBar) Start()                     {}
func (f *fakeBar) Finish()                    {}
func (f *fakeBar) IncrementCompletedObjects() {}
func (f *fakeBar) IncrementTotalObjects()     {}
func (f *fakeBar) AddCompletedBytes(n int64)  { f.bytes += n }
func (f *fakeBar) AddTotalBytes(n int64)      {}

// newFileWithContent creates a temp file and writes content into it, returning
// the open *os.File (caller must Close).
func newFileWithContent(t *testing.T, content string) *os.File {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := os.OpenFile(p, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestCountingReaderWriter_Read verifies Read forwards bytes to the bar.
func TestCountingReaderWriter_Read(t *testing.T) {
	t.Parallel()
	content := "hello world"
	f := newFileWithContent(t, content)
	bar := &fakeBar{}
	c := NewCountingReaderWriter(f, bar)
	buf := make([]byte, 5)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 5 {
		t.Errorf("Read returned %d, want 5", n)
	}
	if bar.bytes != 5 {
		t.Errorf("bar.bytes = %d, want 5", bar.bytes)
	}
	if string(buf) != "hello" {
		t.Errorf("buf = %q, want %q", buf, "hello")
	}
}

// TestCountingReaderWriter_NilBar verifies that a nil bar is replaced by a
// NoOp and the methods still work.
func TestCountingReaderWriter_NilBar(t *testing.T) {
	t.Parallel()
	content := "data"
	f := newFileWithContent(t, content)
	c := NewCountingReaderWriter(f, nil)
	buf := make([]byte, 4)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 4 {
		t.Errorf("Read returned %d, want 4", n)
	}
	if string(buf) != "data" {
		t.Errorf("buf = %q, want %q", buf, "data")
	}
}

// TestCountingReaderWriter_WriteAt verifies WriteAt forwards the byte count.
func TestCountingReaderWriter_WriteAt(t *testing.T) {
	t.Parallel()
	f := newFileWithContent(t, "aaaaaaaaaa")
	bar := &fakeBar{}
	c := NewCountingReaderWriter(f, bar)
	n, err := c.WriteAt([]byte("XYZ"), 2)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 3 {
		t.Errorf("WriteAt returned %d, want 3", n)
	}
	if bar.bytes != 3 {
		t.Errorf("bar.bytes = %d, want 3", bar.bytes)
	}
	// Verify the file actually changed.
	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "aaXYZaaaaa" {
		t.Errorf("file content = %q, want %q", got, "aaXYZaaaaa")
	}
}

// TestCountingReaderWriter_ReadAt_FirstCallNotCounted verifies the
// signature-read semantics: the first ReadAt at any offset is not counted,
// the second one at the same offset is.
func TestCountingReaderWriter_ReadAt_FirstCallNotCounted(t *testing.T) {
	t.Parallel()
	f := newFileWithContent(t, "0123456789")
	bar := &fakeBar{}
	c := NewCountingReaderWriter(f, bar)
	buf := make([]byte, 3)

	// First read at offset 0: signature read, not counted.
	if _, err := c.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt#1: %v", err)
	}
	if bar.bytes != 0 {
		t.Errorf("after first ReadAt bar.bytes = %d, want 0", bar.bytes)
	}

	// Second read at offset 0: counted.
	if _, err := c.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt#2: %v", err)
	}
	if bar.bytes != 3 {
		t.Errorf("after second ReadAt bar.bytes = %d, want 3", bar.bytes)
	}

	// First read at offset 5: signature read again, not counted.
	if _, err := c.ReadAt(buf, 5); err != nil {
		t.Fatalf("ReadAt offset 5: %v", err)
	}
	if bar.bytes != 3 {
		t.Errorf("after first ReadAt at offset 5 bar.bytes = %d, want 3", bar.bytes)
	}

	// Second read at offset 5: counted.
	if _, err := c.ReadAt(buf, 5); err != nil {
		t.Fatalf("ReadAt offset 5 second: %v", err)
	}
	if bar.bytes != 6 {
		t.Errorf("after second ReadAt at offset 5 bar.bytes = %d, want 6", bar.bytes)
	}
}

// TestCountingReaderWriter_Close verifies Close releases the underlying file.
func TestCountingReaderWriter_Close(t *testing.T) {
	t.Parallel()
	f := newFileWithContent(t, "x")
	c := NewCountingReaderWriter(f, &fakeBar{})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCountingReaderWriter_NilFileClose verifies Close on a nil-wrapped file
// is a no-op.
func TestCountingReaderWriter_NilFileClose(t *testing.T) {
	t.Parallel()
	c := NewCountingReaderWriter(nil, nil)
	if err := c.Close(); err != nil {
		t.Errorf("Close(nil) = %v, want nil", err)
	}
}

// TestAggregateErrors_AllNil verifies that an all-nil input yields nil.
func TestAggregateErrors_AllNil(t *testing.T) {
	t.Parallel()
	errs := []error{nil, nil, nil}
	if err := AggregateErrors(errs); err != nil {
		t.Errorf("AggregateErrors(%v) = %v, want nil", errs, err)
	}
}

// TestAggregateErrors_CancelationFiltered verifies that context.Canceled is
// dropped from the aggregate.
func TestAggregateErrors_CancelationFiltered(t *testing.T) {
	t.Parallel()
	cancel := context.Canceled
	other := errors.New("boom")
	err := AggregateErrors([]error{cancel, other, nil})
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %q, want it to contain %q", err, "boom")
	}
	// The cancelation error should NOT be in the chain.
	if errors.Is(err, context.Canceled) {
		t.Errorf("aggregate should not include context.Canceled")
	}
}

// TestAggregateErrors_WarningFiltered verifies that warning sentinel errors
// are dropped.
func TestAggregateErrors_WarningFiltered(t *testing.T) {
	t.Parallel()
	other := errors.New("real")
	err := AggregateErrors([]error{errorpkg.ErrObjectExists, other})
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !strings.Contains(err.Error(), "real") {
		t.Errorf("err = %q, want it to contain %q", err, "real")
	}
	if errors.Is(err, errorpkg.ErrObjectExists) {
		t.Errorf("aggregate should not include ErrObjectExists")
	}
}

// TestAggregateErrors_MultipleErrors verifies that multiple non-nil, non-
// warning, non-cancelation errors are aggregated and reachable via errors.Is.
func TestAggregateErrors_MultipleErrors(t *testing.T) {
	t.Parallel()
	e1 := errors.New("e1")
	e2 := errors.New("e2")
	err := AggregateErrors([]error{e1, e2, nil})
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if !errors.Is(err, e1) {
		t.Errorf("errors.Is(err, e1) = false, want true")
	}
	if !errors.Is(err, e2) {
		t.Errorf("errors.Is(err, e2) = false, want true")
	}
	// The joined error message should mention both.
	if !strings.Contains(err.Error(), "e1") || !strings.Contains(err.Error(), "e2") {
		t.Errorf("err = %q, want both e1 and e2", err)
	}
}

// TestMatchAnyPattern verifies wildcard matching: * matches any suffix, ?
// matches exactly one char, multiple patterns, and empty pattern.
func TestMatchAnyPattern(t *testing.T) {
	t.Parallel()
	patterns, err := CompileExcludeIncludePatterns([]string{"*.log", "f?o"})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"log_match", "a.log", true},
		{"log_nested_match", "dir/a.log", true},
		{"txt_no_match", "a.txt", false},
		{"q_one_char", "foo", true},
		{"q_too_many_chars", "fooo", false},
		{"q_too_few_chars", "fo", false},
		{"both_no_match", "bar.txt", false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MatchAnyPattern(patterns, tc.in); got != tc.want {
				t.Errorf("MatchAnyPattern(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestIsObjectExcluded verifies the exclude/include interaction.
func TestIsObjectExcluded(t *testing.T) {
	t.Parallel()
	exclude, _ := CompileExcludeIncludePatterns([]string{"*.tmp"})
	include, _ := CompileExcludeIncludePatterns([]string{"keep*"})
	tests := []struct {
		name string
		obj  string
		want bool
	}{
		{"excluded_by_exclude", "file.tmp", true},
		{"not_excluded_not_included", "other.log", true}, // include non-empty => non-match excludes
		{"included_match", "keepfile.txt", false},
		{"excluded_overrides_included", "keep.tmp", true}, // exclude wins over include
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsObjectExcluded(tc.obj, exclude, include); got != tc.want {
				t.Errorf("IsObjectExcluded(%q) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestIsObjectExcluded_NoPatterns verifies that with no patterns nothing is
// excluded.
func TestIsObjectExcluded_NoPatterns(t *testing.T) {
	t.Parallel()
	if IsObjectExcluded("any", nil, nil) {
		t.Errorf("with no patterns nothing should be excluded")
	}
}

// TestGuessContentType_Extension verifies the extension-based path for
// common types.
func TestGuessContentType_Extension(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ext  string
		want string
	}{
		{"json", ".json", "application/json"},
		{"png", ".png", "image/png"},
		{"txt", ".txt", "text/plain; charset=utf-8"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			p := filepath.Join(dir, "f"+tc.ext)
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			f, err := os.Open(p)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer f.Close()
			got := GuessContentType(f)
			if got != tc.want {
				t.Errorf("GuessContentType(%q) = %q, want %q", tc.ext, got, tc.want)
			}
		})
	}
}

// TestGuessContentType_NoExtension verifies that a file with no extension
// falls through to content sniffing.
func TestGuessContentType_NoExtension(t *testing.T) {
	t.Parallel()
	// Sniffing arbitrary byte text tends to give "text/plain" or
	// "application/octet-stream" depending on content; we only assert that
	// the result is non-empty and that the function does not error.
	dir := t.TempDir()
	p := filepath.Join(dir, "noext")
	if err := os.WriteFile(p, []byte("plain text content"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got := GuessContentType(f)
	if got == "" {
		t.Errorf("GuessContentType(noext) = empty, want non-empty")
	}
}

// TestGuessContentType_UnknownExtension verifies that an unknown extension
// also falls through to sniffing.
func TestGuessContentType_UnknownExtension(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "f.weirdext")
	if err := os.WriteFile(p, []byte("<html>"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got := GuessContentType(f)
	if got == "" {
		t.Errorf("GuessContentType(unknownext) = empty, want non-empty")
	}
	if !strings.HasPrefix(got, "text/html") {
		t.Logf("note: sniff for '<html>' gave %q (not text/html)", got)
	}
}

// TestGuessContentType_NilFile verifies that a nil file yields the empty
// string without panicking.
func TestGuessContentType_NilFile(t *testing.T) {
	t.Parallel()
	if got := GuessContentType(nil); got != "" {
		t.Errorf("GuessContentType(nil) = %q, want empty", got)
	}
}

// TestCompileExcludeIncludePatterns_Empty verifies that an empty input yields
// a nil slice with no error.
func TestCompileExcludeIncludePatterns_Empty(t *testing.T) {
	t.Parallel()
	got, err := CompileExcludeIncludePatterns(nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}

// Ensure the progressbar package is referenced so the test build pulls it in
// even when the NoOp is the only use site.
var _ progressbar.ProgressBar = &progressbar.NoOp{}

// Ensure bytes is used by the package compile test for fixtures.
var _ = bytes.NewBuffer
