// Package orderedwriter_test contains tests for the OrderedWriterAt. The
// cases mirror the s5cmd orderedwriter_test.go structure but are written
// without the gotest.tools dependency so they can run on a bare stdlib.
package orderedwriter_test

import (
	"bytes"
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

// TestBufferWithChangingSlice mirrors the s5cmd test of the same name: a
// worker pool writes chunks whose backing slice is reused (we simulate this
// by mutating the slice before the writer is forced to flush). The
// OrderedWriterAt must copy on buffer, so the final output should still
// equal the expected stream.
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
