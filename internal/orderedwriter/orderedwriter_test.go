// Package orderedwriter_test contains tests for the OrderedWriterAt. The
// cases are written without the gotest.tools dependency so they can run on
// a bare stdlib.
package orderedwriter_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/LinPr/s6cmd/internal/orderedwriter"
)

const testRuns = 32

func randomBytes(n int) []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return b
}

// TestSequentialWrite verifies that writing chunks in offset order from a
// single goroutine produces the expected output and that the fast path
// (offset == written) is taken.
func TestSequentialWrite(t *testing.T) {
	t.Parallel()

	const chunkSize = 5
	const fileSize = 100
	expected := randomBytes(fileSize)
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	for i := 0; i < fileSize; i += chunkSize {
		end := i + chunkSize
		if end > fileSize {
			end = fileSize
		}
		if n, err := w.WriteAt(expected[i:end], int64(i)); err != nil {
			t.Fatalf("WriteAt(%d): %v", i, err)
		} else if n != end-i {
			t.Errorf("WriteAt(%d) returned %d, want %d", i, n, end-i)
		}
	}

	if !bytes.Equal(result.Bytes(), expected) {
		t.Errorf("output mismatch: got %d bytes, want %d bytes", result.Len(), len(expected))
	}
}

// TestShuffleWriteWithStaticChunkSize writes fixed-size chunks in random
// order and asserts the output is the original expected byte stream.
func TestShuffleWriteWithStaticChunkSize(t *testing.T) {
	t.Parallel()

	const (
		chunkSize = 5
		fileSize  = 1000
	)

	for r := 0; r < testRuns; r++ {
		r := r
		t.Run(fmt.Sprintf("Run%d", r), func(t *testing.T) {
			t.Parallel()
			var result bytes.Buffer
			expected := randomBytes(fileSize)

			type chunk struct {
				offset int64
				value  []byte
			}
			var chunks []chunk
			for i := 0; i < fileSize; i += chunkSize {
				chunks = append(chunks, chunk{int64(i), expected[i : i+chunkSize]})
			}
			rand.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })

			w := orderedwriter.New(&result)
			for _, c := range chunks {
				if _, err := w.WriteAt(c.value, c.offset); err != nil {
					t.Fatalf("WriteAt(%d): %v", c.offset, err)
				}
			}
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("output mismatch (run %d): got %d bytes, want %d", r, result.Len(), len(expected))
			}
		})
	}
}

// TestShuffleWriteWithRandomChunkSize is the same as above but with random
// chunk sizes.
func TestShuffleWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 100
		minChunkSize, maxChunkSize = 5, 1000
	)

	for r := 0; r < testRuns; r++ {
		r := r
		t.Run(fmt.Sprintf("Run%d", r), func(t *testing.T) {
			t.Parallel()
			var (
				result   bytes.Buffer
				offset   int64
				expected []byte
			)
			type chunk struct {
				offset int64
				value  []byte
			}
			var chunks []chunk
			for i := 0; i <= maxFileSize; {
				size := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				c := randomBytes(size)
				chunks = append(chunks, chunk{offset, c})
				expected = append(expected, c...)
				offset += int64(size)
				i += size
			}
			rand.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })
			w := orderedwriter.New(&result)
			for _, c := range chunks {
				if _, err := w.WriteAt(c.value, c.offset); err != nil {
					t.Fatalf("WriteAt(%d): %v", c.offset, err)
				}
			}
			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("output mismatch (run %d): got %d bytes, want %d", r, result.Len(), len(expected))
			}
		})
	}
}

// TestShuffleConcurrentWriteWithRandomChunkSize is the concurrent variant:
// multiple worker goroutines pull chunks from a shared channel and write
// them to a single OrderedWriterAt, simulating the S3 download manager.
func TestShuffleConcurrentWriteWithRandomChunkSize(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 100
		minChunkSize, maxChunkSize = 5, 1000
	)

	for r := 0; r < testRuns; r++ {
		r := r
		t.Run(fmt.Sprintf("Run%d", r), func(t *testing.T) {
			t.Parallel()
			var (
				result   bytes.Buffer
				expected []byte
			)
			type chunk struct {
				offset int64
				value  []byte
			}
			var chunks []chunk
			for i := 0; i <= maxFileSize; {
				size := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				c := randomBytes(size)
				chunks = append(chunks, chunk{int64(i), c})
				expected = append(expected, c...)
				i += size
			}
			rand.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })
			w := orderedwriter.New(&result)

			ch := make(chan chunk)
			var wg sync.WaitGroup
			workers := 5 + rand.Intn(20)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for c := range ch {
						if _, err := w.WriteAt(c.value, c.offset); err != nil {
							t.Errorf("WriteAt(%d): %v", c.offset, err)
							return
						}
					}
				}()
			}
			for _, c := range chunks {
				ch <- c
			}
			close(ch)
			wg.Wait()

			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("output mismatch (run %d): got %d bytes, want %d", r, result.Len(), len(expected))
			}
		})
	}
}

// TestWriteAtZeroLength verifies that a zero-length write is a no-op that
// does not advance the written cursor.
func TestWriteAtZeroLength(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)
	if n, err := w.WriteAt(nil, 0); err != nil {
		t.Fatalf("WriteAt(nil,0): %v", err)
	} else if n != 0 {
		t.Errorf("WriteAt(nil,0) returned %d, want 0", n)
	}
	if result.Len() != 0 {
		t.Errorf("result.Len() = %d, want 0", result.Len())
	}
	// Now write a real chunk at offset 0; it should land.
	if _, err := w.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatalf("WriteAt(abc,0): %v", err)
	}
	if got := result.String(); got != "abc" {
		t.Errorf("result = %q, want %q", got, "abc")
	}
}

// TestOutrunningChunk verifies that a chunk whose offset is beyond the
// current written pointer is buffered and flushed only when the gap is
// filled by a later write.
func TestOutrunningChunk(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	// Write the second chunk first; it must be buffered.
	if _, err := w.WriteAt([]byte("B"), 1); err != nil {
		t.Fatalf("WriteAt(B,1): %v", err)
	}
	if result.Len() != 0 {
		t.Errorf("after WriteAt(B,1) result.Len() = %d, want 0", result.Len())
	}

	// Now write the first chunk; both should flush in order.
	if _, err := w.WriteAt([]byte("A"), 0); err != nil {
		t.Fatalf("WriteAt(A,0): %v", err)
	}
	if got := result.String(); got != "AB" {
		t.Errorf("result = %q, want %q", got, "AB")
	}
}

// TestBufferWithChangingSlice verifies that a worker pool can write chunks
// whose backing slice is reused (we simulate this by mutating the slice
// before the writer is forced to flush). The OrderedWriterAt must copy on
// buffer, so the final output should still equal the expected stream.
func TestBufferWithChangingSlice(t *testing.T) {
	t.Parallel()

	const (
		maxFileSize                = 1024 * 100
		minChunkSize, maxChunkSize = 5, 100
	)

	for r := 0; r < testRuns; r++ {
		r := r
		t.Run(fmt.Sprintf("Run%d", r), func(t *testing.T) {
			t.Parallel()
			var (
				expected []byte
			)
			type chunk struct {
				offset int64
				value  []byte
			}
			var chunks []chunk
			for i := 0; i <= maxFileSize; {
				size := minChunkSize + rand.Intn(maxChunkSize-minChunkSize)
				c := randomBytes(size)
				chunks = append(chunks, chunk{int64(i), c})
				expected = append(expected, c...)
				i += size
			}
			rand.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })
			result := bytes.NewBuffer(make([]byte, 0, len(expected)))
			w := orderedwriter.New(result)

			// Sanity: write everything in shuffled order, mutating the
			// backing slice immediately after each WriteAt. If the writer
			// correctly copies on buffer, the output will still equal
			// expected.
			for i := range chunks {
				v := chunks[i].value
				off := chunks[i].offset
				if _, err := w.WriteAt(v, off); err != nil {
					t.Fatalf("WriteAt(%d): %v", off, err)
				}
				// Mutate the slice to simulate the SDK reusing the buffer.
				for j := range v {
					v[j] = 'X'
				}
			}

			if !bytes.Equal(result.Bytes(), expected) {
				t.Errorf("output mismatch (run %d): got %d bytes, want %d", r, result.Len(), len(expected))
			}
		})
	}
}

// flakyWriter fails the first failures writes with a transient error after
// consuming partialN bytes, mimicking a destination that accepts part of a
// chunk before erroring. Subsequent writes succeed.
type flakyWriter struct {
	buf      bytes.Buffer
	failures int
	partialN int
}

func (f *flakyWriter) Write(p []byte) (int, error) {
	if f.failures > 0 {
		f.failures--
		n := f.partialN
		if n > len(p) {
			n = len(p)
		}
		f.buf.Write(p[:n])
		return n, errors.New("transient write failure")
	}
	return f.buf.Write(p)
}

// TestDuplicateFlushedChunk verifies that re-issuing a chunk that was
// already flushed (an SDK part retry) is dropped instead of stalling the
// flush loop: before the watermark check was added, the stale chunk sat at
// the front of the list forever and output silently stopped.
func TestDuplicateFlushedChunk(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	if _, err := w.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatalf("WriteAt(abc,0): %v", err)
	}
	// Retry of the already-flushed part: must be accepted and dropped.
	if n, err := w.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatalf("WriteAt(abc,0) retry: %v", err)
	} else if n != 3 {
		t.Errorf("retry WriteAt returned %d, want 3", n)
	}
	// The stream must keep flowing after the duplicate.
	if _, err := w.WriteAt([]byte("def"), 3); err != nil {
		t.Fatalf("WriteAt(def,3): %v", err)
	}
	if got := result.String(); got != "abcdef" {
		t.Errorf("result = %q, want %q", got, "abcdef")
	}
}

// TestDuplicateBufferedChunk verifies that a duplicate of a still-buffered
// (out-of-order) chunk is flushed exactly once.
func TestDuplicateBufferedChunk(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	if _, err := w.WriteAt([]byte("B"), 1); err != nil {
		t.Fatalf("WriteAt(B,1): %v", err)
	}
	if _, err := w.WriteAt([]byte("B"), 1); err != nil {
		t.Fatalf("WriteAt(B,1) duplicate: %v", err)
	}
	if _, err := w.WriteAt([]byte("A"), 0); err != nil {
		t.Fatalf("WriteAt(A,0): %v", err)
	}
	if got := result.String(); got != "AB" {
		t.Errorf("result = %q, want %q", got, "AB")
	}
	// The stream must continue at the right offset after the duplicate
	// was discarded.
	if _, err := w.WriteAt([]byte("C"), 2); err != nil {
		t.Fatalf("WriteAt(C,2): %v", err)
	}
	if got := result.String(); got != "ABC" {
		t.Errorf("result = %q, want %q", got, "ABC")
	}
}

// TestOverlapStraddlingWatermark verifies that a chunk that overlaps the
// already-written prefix is trimmed, not re-written.
func TestOverlapStraddlingWatermark(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	if _, err := w.WriteAt([]byte("abcd"), 0); err != nil {
		t.Fatalf("WriteAt(abcd,0): %v", err)
	}
	// Overlaps [2,4) which is already written; only "ef" must land.
	if n, err := w.WriteAt([]byte("cdef"), 2); err != nil {
		t.Fatalf("WriteAt(cdef,2): %v", err)
	} else if n != 4 {
		t.Errorf("WriteAt(cdef,2) returned %d, want 4", n)
	}
	if got := result.String(); got != "abcdef" {
		t.Errorf("result = %q, want %q", got, "abcdef")
	}
}

// TestBufferedOverlapTrimmedAtFlush verifies that a buffered chunk whose
// range is partially consumed by the time it reaches the front of the list
// is trimmed against the watermark during the flush sweep.
func TestBufferedOverlapTrimmedAtFlush(t *testing.T) {
	t.Parallel()
	var result bytes.Buffer
	w := orderedwriter.New(&result)

	// Buffer an out-of-order chunk overlapping [2,6).
	if _, err := w.WriteAt([]byte("cdef"), 2); err != nil {
		t.Fatalf("WriteAt(cdef,2): %v", err)
	}
	// Now write [0,4): flushes straight through and overtakes the front
	// half of the buffered chunk, which must be trimmed to "ef".
	if _, err := w.WriteAt([]byte("abcd"), 0); err != nil {
		t.Fatalf("WriteAt(abcd,0): %v", err)
	}
	if got := result.String(); got != "abcdef" {
		t.Errorf("result = %q, want %q", got, "abcdef")
	}
}

// TestRetryAfterFastPathError verifies the retry contract on the fast
// path: when the underlying writer consumes part of the chunk and errors,
// the watermark advances past the consumed bytes, so the SDK's retried
// WriteAt resumes exactly at the watermark and cannot double-write.
func TestRetryAfterFastPathError(t *testing.T) {
	t.Parallel()
	fw := &flakyWriter{failures: 1, partialN: 2}
	w := orderedwriter.New(fw)

	if _, err := w.WriteAt([]byte("hello"), 0); err == nil {
		t.Fatalf("WriteAt(hello,0): expected transient error, got nil")
	}
	// SDK retry re-issues the whole part.
	if _, err := w.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("WriteAt(hello,0) retry: %v", err)
	}
	if _, err := w.WriteAt([]byte("world"), 5); err != nil {
		t.Fatalf("WriteAt(world,5): %v", err)
	}
	if got := fw.buf.String(); got != "helloworld" {
		t.Errorf("result = %q, want %q", got, "helloworld")
	}
}

// TestRetryAfterBufferedFlushError verifies the retry contract on the
// buffered path: a mid-flush partial write + error leaves the failed chunk
// truncated to its unwritten tail, so retrying both parts produces the
// stream exactly once.
func TestRetryAfterBufferedFlushError(t *testing.T) {
	t.Parallel()
	fw := &flakyWriter{failures: 1, partialN: 2}
	w := orderedwriter.New(fw)

	// Buffer the second part so the first goes through the flush loop.
	if _, err := w.WriteAt([]byte("world"), 5); err != nil {
		t.Fatalf("WriteAt(world,5): %v", err)
	}
	if _, err := w.WriteAt([]byte("hello"), 0); err == nil {
		t.Fatalf("WriteAt(hello,0): expected transient error, got nil")
	}
	// SDK retry of the failed part: the consumed prefix must be dropped
	// and the remainder must flush, pulling the buffered part with it.
	if _, err := w.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("WriteAt(hello,0) retry: %v", err)
	}
	if got := fw.buf.String(); got != "helloworld" {
		t.Errorf("result = %q, want %q", got, "helloworld")
	}
}
