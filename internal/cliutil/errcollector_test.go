package cliutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
)

// TestErrorCollector_ConcurrentCollect verifies (under -race) that the
// drain goroutine and the submission loop can collect concurrently: N
// failing tasks report through the Waiter while the main goroutine also
// collects its own errors, mirroring cp/sync/select's structure.
func TestErrorCollector_ConcurrentCollect(t *testing.T) {
	t.Parallel()
	const taskErrs = 50
	const mainErrs = 50

	pm := parallel.New(8)
	defer pm.Close()

	waiter := parallel.NewWaiter()
	ec := NewErrorCollector("test")
	wait := ec.Drain(waiter)

	for i := 0; i < taskErrs; i++ {
		i := i
		pm.Run(func() error { return fmt.Errorf("task %d failed", i) }, waiter)
		// Interleave main-goroutine collects with task submission, the
		// exact pattern that raced on the shared errs slice before.
		if i < mainErrs {
			ec.Collect(fmt.Errorf("main %d failed", i))
		}
	}
	waiter.Wait()
	wait()

	err := ec.Aggregate()
	if err == nil {
		t.Fatal("Aggregate() = nil, want error")
	}
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Fatalf("Aggregate() error does not unwrap to []error: %v", err)
	}
	if got := len(joined.Unwrap()); got != taskErrs+mainErrs {
		t.Errorf("collected %d errors, want %d", got, taskErrs+mainErrs)
	}
}

// TestErrorCollector_ExitOnErrorNoDeadlock is a regression test for the
// sync --exit-on-error deadlock: the old drain goroutine broke out of the
// waiter.Err() loop on the first error, and because the channel is
// unbuffered, the next failing task blocked on send before wg.Done, so
// waiter.Wait() hung forever. The collector's drain never stops early;
// the submission loop stops instead (via HasError).
func TestErrorCollector_ExitOnErrorNoDeadlock(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")

	pm := parallel.New(4)
	defer pm.Close()

	waiter := parallel.NewWaiter()
	ec := NewErrorCollector("test")
	wait := ec.Drain(waiter)

	// Submit failing tasks the way sync's planAndRun does with
	// --exit-on-error: stop submitting once an error has been seen, but
	// tasks already in flight still report their errors.
	for i := 0; i < 128; i++ {
		if ec.HasError() {
			break
		}
		pm.Run(func() error { return boom }, waiter)
	}

	done := make(chan struct{})
	go func() {
		waiter.Wait()
		wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: waiter.Wait()/drain did not complete")
	}

	if !ec.HasError() {
		t.Fatal("HasError() = false, want true")
	}
	if err := ec.Aggregate(); !errors.Is(err, boom) {
		t.Errorf("Aggregate() = %v, want to wrap %v", err, boom)
	}
}

// TestErrorCollector_Filtering verifies the shared filtering rules: nil,
// cancelations and warnings are not aggregated; real errors are.
func TestErrorCollector_Filtering(t *testing.T) {
	t.Parallel()
	ec := NewErrorCollector("test")

	ec.Collect(nil)
	ec.Collect(context.Canceled)
	ec.Collect(errorpkg.ErrObjectExists)
	ec.Collect(errorpkg.ErrObjectSizesMatch)
	if ec.HasError() {
		t.Fatal("HasError() = true after warnings/cancelations only")
	}
	if err := ec.Aggregate(); err != nil {
		t.Fatalf("Aggregate() = %v, want nil", err)
	}

	boom := errors.New("boom")
	ec.Collect(boom)
	if !ec.HasError() {
		t.Fatal("HasError() = false after a real error")
	}
	if err := ec.Aggregate(); !errors.Is(err, boom) {
		t.Errorf("Aggregate() = %v, want to wrap %v", err, boom)
	}
}

// TestErrorCollector_CollectRace hammers Collect from many goroutines
// (no Waiter involved) so -race can catch any unsynchronized append.
func TestErrorCollector_CollectRace(t *testing.T) {
	t.Parallel()
	ec := NewErrorCollector("test")
	var wg sync.WaitGroup
	const goroutines = 16
	const perGoroutine = 100
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ec.Collect(errors.New("x"))
			}
		}()
	}
	wg.Wait()

	var joined interface{ Unwrap() []error }
	if err := ec.Aggregate(); !errors.As(err, &joined) {
		t.Fatalf("Aggregate() does not unwrap to []error: %v", err)
	} else if got := len(joined.Unwrap()); got != goroutines*perGoroutine {
		t.Errorf("collected %d errors, want %d", got, goroutines*perGoroutine)
	}
}
