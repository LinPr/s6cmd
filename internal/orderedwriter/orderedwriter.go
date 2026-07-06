// Package orderedwriter implements an unbounded buffer for ordering
// concurrent writes against a non-seekable io.Writer.
//
// Concurrent downloaders (e.g. the S3 manager) often produce chunks out of
// order but expect them written in offset order. OrderedWriterAt buffers
// out-of-order chunks in a linked list and flushes them to the underlying
// writer as soon as the next expected offset becomes available.
package orderedwriter

import (
	"container/list"
	"io"
	"sync"
)

// chunk is a buffered slice waiting for its offset to become the next
// expected write.
type chunk struct {
	offset int64
	value  []byte
}

// OrderedWriterAt wraps an io.Writer and accepts WriteAt calls from
// multiple goroutines, flushing them in offset order.
type OrderedWriterAt struct {
	mu      *sync.Mutex
	list    *list.List
	w       io.Writer
	written int64
}

// New creates an OrderedWriterAt that writes to w in offset order.
func New(w io.Writer) *OrderedWriterAt {
	return &OrderedWriterAt{
		mu:      &sync.Mutex{},
		list:    list.New(),
		w:       w,
		written: 0,
	}
}

// WriteAt writes p at the given offset. If offset is the next expected
// byte, p is written straight through to the underlying writer; otherwise
// p is copied (because callers may reuse the slice before it is flushed)
// and queued. After queueing, any prefix of the queue that is now
// contiguous with written is flushed.
//
// Bytes at offsets below the written watermark are dropped (or the chunk
// is trimmed when it straddles the watermark): the SDK retries parts on
// error and a retried WriteAt may re-issue bytes that were already
// flushed. Re-buffering them would stall the flush loop forever, because
// the stale chunk's offset can never equal the watermark again.
func (w *OrderedWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	origLen := len(p)

	// Drop or trim the portion of p that is already below the watermark
	// (duplicate bytes from an SDK part retry).
	if offset < w.written {
		if offset+int64(len(p)) <= w.written {
			// Entirely below the watermark: already written, drop.
			return origLen, nil
		}
		p = p[w.written-offset:]
		offset = w.written
	}

	// Fast path: nothing buffered and this chunk is exactly the next
	// expected byte. Write straight through without copying.
	if w.list.Front() == nil && w.written == offset {
		n, err := w.w.Write(p)
		// Advance the watermark even on error: the underlying writer
		// consumed n bytes, and a retry of this part must not write
		// them a second time.
		w.written += int64(n)
		if err != nil {
			return n, err
		}
		if n < len(p) {
			return n, io.ErrShortWrite
		}
		return origLen, nil
	}

	// Copy the chunk because buffered callers may mutate the slice
	// before we drain it.
	b := make([]byte, len(p))
	copy(b, p)

	// If the list is empty we couldn't take the fast path because the
	// offset was out of order; just queue and return.
	if w.list.Front() == nil {
		w.list.PushBack(&chunk{offset: offset, value: b})
		return origLen, nil
	}

	// Otherwise insert the chunk in offset order so the flush loop
	// below can walk the front of the list.
	var inserted bool
	for e := w.list.Front(); e != nil; e = e.Next() {
		v, _ := e.Value.(*chunk)
		if offset < v.offset {
			w.list.InsertBefore(&chunk{offset: offset, value: b}, e)
			inserted = true
			break
		}
	}
	if !inserted {
		w.list.PushBack(&chunk{offset: offset, value: b})
	}

	// Flush any chunks that are now contiguous with written. Stale or
	// overlapping chunks (possible after retries) are dropped or trimmed
	// against the watermark so they can neither double-write nor stall
	// the flush.
	for e := w.list.Front(); e != nil; {
		next := e.Next()
		v, _ := e.Value.(*chunk)
		if v.offset+int64(len(v.value)) <= w.written {
			// Fully below the watermark: duplicate of already-written
			// bytes; drop it.
			w.list.Remove(e)
			e = next
			continue
		}
		if v.offset < w.written {
			// Straddles the watermark: trim the already-written prefix.
			v.value = v.value[w.written-v.offset:]
			v.offset = w.written
		}
		if v.offset != w.written {
			break
		}
		n, err := w.w.Write(v.value)
		w.written += int64(n)
		if n == len(v.value) {
			w.list.Remove(e)
		} else {
			// Short write: keep only the unwritten tail so a later
			// flush resumes exactly at the watermark and cannot
			// double-write the consumed prefix.
			v.value = v.value[n:]
			v.offset = w.written
			if err == nil {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			return origLen, err
		}
		e = next
	}

	return origLen, nil
}
