package cliutil

import (
	"errors"
	"io"
	"os"
	"regexp"
	"sync"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/progressbar"
	"github.com/LinPr/s6cmd/strutil"
)

// countingReaderWriter wraps an *os.File and reports every byte transferred
// to the progress bar. It implements io.Reader, io.WriterAt, io.ReaderAt
// and io.Seeker so it can be passed to both the S3 manager.Uploader (which
// reads) and manager.Downloader (which WriteAts).
//
// The ReadAt path tracks offsets that have already been accounted for via
// the SDK's signature-fetch; this mirrors s5cmd's countingReaderWriter so
// the progress bar does not double-count signature reads.
type countingReaderWriter struct {
	pb      progressbar.ProgressBar
	fp      *os.File
	signMap map[int64]struct{}
	mu      sync.Mutex
}

// NewCountingReaderWriter wraps file so byte progress is reported to pb. A
// nil pb is allowed and turns every method into a no-op pass-through.
func NewCountingReaderWriter(file *os.File, pb progressbar.ProgressBar) *countingReaderWriter {
	if pb == nil {
		pb = &progressbar.NoOp{}
	}
	return &countingReaderWriter{
		pb:      pb,
		fp:      file,
		signMap: map[int64]struct{}{},
	}
}

// WriteAt writes p at off and reports the byte count to the progress bar.
func (r *countingReaderWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := r.fp.WriteAt(p, off)
	r.pb.AddCompletedBytes(int64(n))
	return n, err
}

// Read reads from the underlying file and reports the byte count.
func (r *countingReaderWriter) Read(p []byte) (int, error) {
	n, err := r.fp.Read(p)
	r.pb.AddCompletedBytes(int64(n))
	return n, err
}

// ReadAt reads from the underlying file at off. The first ReadAt at any
// offset is treated as a signature read by the SDK and not counted; this
// matches s5cmd's behaviour so the progress bar does not double-count.
func (r *countingReaderWriter) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.fp.ReadAt(p, off)
	r.mu.Lock()
	if _, ok := r.signMap[off]; ok {
		r.pb.AddCompletedBytes(int64(n))
	} else {
		r.signMap[off] = struct{}{}
	}
	r.mu.Unlock()
	return n, err
}

// Seek delegates to the underlying file so the SDK can rewind for parts.
func (r *countingReaderWriter) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}

// Close closes the underlying file. It is a no-op when the file is nil.
func (r *countingReaderWriter) Close() error {
	if r.fp == nil {
		return nil
	}
	return r.fp.Close()
}

// GuessContentType returns the content type for the file based first on its
// extension and, failing that, on a sniff of the first 512 bytes. It mirrors
// s5cmd's guessContentType so cp/mv/sync can populate the metadata on
// uploads without duplicating the logic.
func GuessContentType(file *os.File) string {
	if file == nil {
		return ""
	}
	return guessContentType(file)
}

// guessContentType is the unexported implementation; it is kept separate so
// the exported wrapper can short-circuit on nil without dragging a second
// import of mime/net/http into every caller that already has a non-nil
// file.
func guessContentType(file *os.File) string {
	// Local import to keep the package surface small for callers that use
	// only the counting reader/writer.
	return guessContentTypeImpl(file)
}

// guessContentTypeImpl lives in counting_content.go to keep mime/http
// imports here instead of in counting.go.
var guessContentTypeImpl = func(file *os.File) string { return "" }

// CompileExcludeIncludePatterns converts the user-supplied --exclude and
// --include wildcard patterns to anchored regex strings understood by
// MatchAnyPattern. An empty input returns a nil slice with no error.
func CompileExcludeIncludePatterns(patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p == "" {
			continue
		}
		out = append(out, strutil.MatchFromStartToEnd(strutil.WildCardToRegexp(p)))
	}
	return out, nil
}

// MatchAnyPattern reports whether name matches any of the precompiled
// wildcard regex strings. Patterns are anchored to the full name.
func MatchAnyPattern(patterns []string, name string) bool {
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if wildcardMatch(p, name) {
			return true
		}
	}
	return false
}

// wildcardRegexCache caches compiled regexes for the anchored wildcard
// patterns produced by CompileExcludeIncludePatterns. The patterns are
// anchored with ^...$ and use (?s) so . crosses newlines, which the
// iterative wildcardMatch below does not. Using regex here keeps the
// semantics consistent with strutil.WildCardToRegexp and the storage/url
// filter regex.
var wildcardRegexCache sync.Map

// wildcardMatch matches an anchored wildcard pattern against s. It prefers a
// cached regex (built the first time a pattern is seen) so that, e.g., the
// "." in the regex can cross newlines exactly like strutil's regexes do. If
// regex compilation fails it falls back to the simple iterative matcher.
func wildcardMatch(pattern, s string) bool {
	if v, ok := wildcardRegexCache.Load(pattern); ok {
		re := v.(*regexp.Regexp)
		return re.MatchString(s)
	}
	// The pattern arriving here is already WildCardToRegexp + anchored; add
	// the (?s) flag so . matches newlines, matching the url filter regex.
	re, err := regexp.Compile(strutil.AddNewLineFlag(pattern))
	if err != nil {
		// Fall back to the iterative matcher if the pattern somehow did
		// not compile. This path should be unreachable because
		// CompileExcludeIncludePatterns already used regexp.QuoteMeta.
		return wildcardMatchIterative(pattern, s)
	}
	wildcardRegexCache.Store(pattern, re)
	return re.MatchString(s)
}

// wildcardMatchIterative is the simple iterative glob matcher used as a
// fallback when regex compilation fails. It supports ? and * but the "*" does
// not cross "/" unless the pattern explicitly contains "/".
func wildcardMatchIterative(pattern, s string) bool {
	pi, si := 0, 0
	star := -1
	ss := 0
	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			star = pi
			ss = si
			pi++
		} else if star != -1 {
			pi = star + 1
			ss++
			si = ss
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// IsObjectExcluded reports whether name should be excluded given the
// exclude/include pattern sets. The semantics match s5cmd: an object is
// excluded when it matches any exclude pattern; if include patterns are
// present, only matching objects are included (so a non-match excludes).
//
// The empty-pattern cases are handled explicitly so callers can pass nil
// slices without filtering everything out.
func IsObjectExcluded(name string, excludePatterns, includePatterns []string) bool {
	if len(excludePatterns) > 0 && MatchAnyPattern(excludePatterns, name) {
		return true
	}
	if len(includePatterns) > 0 && !MatchAnyPattern(includePatterns, name) {
		return true
	}
	return false
}

// AggregateErrors concatenates errs into a single error. Nil entries are
// dropped; if every entry is nil the result is nil. The returned error
// implements Unwrap() []error so callers can use errors.As to recover the
// full list, and is recognized by errorpkg.IsCancelation because we use
// errors.Join which preserves the Unwrap chain.
//
// We use errors.Join (Go 1.20+) instead of hashicorp/go-multierror so the
// project does not pull in a new dependency.
func AggregateErrors(errs []error) error {
	cleaned := make([]error, 0, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		if errorpkg.IsCancelation(err) {
			continue
		}
		if errorpkg.IsWarning(err) {
			// Warnings are surfaced via the log channel, not the error
			// aggregate, so they do not turn the command's exit code red.
			continue
		}
		cleaned = append(cleaned, err)
	}
	if len(cleaned) == 0 {
		return nil
	}
	return errors.Join(cleaned...)
}

// ReadSeekerCloser is the union of io.Reader, io.Seeker and io.Closer. It
// is the interface the S3 uploader expects from its body. It is named so
// callers can write a clear type assertion instead of the inline 3-method
// interface.
type ReadSeekerCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}
