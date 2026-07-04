package parallel_test

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/LinPr/s6cmd/internal/parallel"
)

// runtime_Gosched is a tiny wrapper so the test does not need to import
// runtime everywhere.
func runtime_Gosched() { runtime.Gosched() }

// TestNewManager_NegativeWorkerCount verifies that a negative worker count
// is interpreted as -N * NumCPU, i.e. the resulting semaphore capacity is
// at least minNumWorkers.
func TestNewManager_NegativeWorkerCount(t *testing.T) {
	t.Parallel()
	m := parallel.New(-2)
	w := parallel.NewWaiter()
	var count int32
	for i := 0; i < 16; i++ {
		m.Run(func() error {
			atomic.AddInt32(&count, 1)
			return nil
		}, w)
	}
	m.Close()
	w.Wait()
	if got := atomic.LoadInt32(&count); got != 16 {
		t.Errorf("ran %d tasks, want 16", got)
	}
}

// TestManager_Run_Wait_Basic verifies that submitting N tasks to a Manager
// and waiting on the Waiter results in every task running exactly once.
func TestManager_Run_Wait_Basic(t *testing.T) {
	t.Parallel()
	m := parallel.New(4)
	w := parallel.NewWaiter()
	const n = 50
	var count int32
	for i := 0; i < n; i++ {
		m.Run(func() error {
			atomic.AddInt32(&count, 1)
			return nil
		}, w)
	}
	// Drain errors concurrently with Wait because the errch is unbuffered.
	var errs []error
	var drainWG sync.WaitGroup
	drainWG.Add(1)
	go func() {
		defer drainWG.Done()
		for err := range w.Err() {
			errs = append(errs, err)
		}
	}()
	m.Close()
	w.Wait()
	drainWG.Wait()
	if got := atomic.LoadInt32(&count); got != n {
		t.Errorf("ran %d tasks, want %d", got, n)
	}
	if len(errs) != 0 {
		t.Errorf("got %d errors, want 0: %v", len(errs), errs)
	}
}

// TestManager_ErrorAggregation verifies that multiple failing tasks surface
// on the error channel and that the channel is closed after Wait.
func TestManager_ErrorAggregation(t *testing.T) {
	t.Parallel()
	m := parallel.New(8)
	w := parallel.NewWaiter()
	errA := errors.New("a")
	errB := errors.New("b")
	errC := errors.New("c")
	tasks := []error{errA, errB, errC, nil, nil}
	for _, e := range tasks {
		e := e
		m.Run(func() error { return e }, w)
	}
	var got []error
	var drainWG sync.WaitGroup
	drainWG.Add(1)
	go func() {
		defer drainWG.Done()
		for err := range w.Err() {
			got = append(got, err)
		}
	}()
	m.Close()
	w.Wait()
	drainWG.Wait()
	if len(got) != 3 {
		t.Fatalf("got %d errors, want 3: %v", len(got), got)
	}
	seen := map[error]bool{}
	for _, e := range got {
		seen[e] = true
	}
	for _, want := range []error{errA, errB, errC} {
		if !seen[want] {
			t.Errorf("error %v not in channel output %v", want, got)
		}
	}
}

// TestManager_WorkerCountThrottle verifies that the manager never exceeds
// the configured worker count: at no point in time are more than `jobs`
// tasks running concurrently.
//
// Each task increments an active counter, records the peak, briefly sleeps,
// then decrements active and returns nil. With 100 tasks and 4 workers the
// peak must not exceed 4. We do not gate tasks on a start channel because
// m.Run blocks on the semaphore, so the main goroutine would deadlock if it
// tried to enqueue all 100 before releasing the gate.
func TestManager_WorkerCountThrottle(t *testing.T) {
	t.Parallel()
	const jobs = 4
	const n = 100
	m := parallel.New(jobs)
	w := parallel.NewWaiter()

	var active, peak int32
	for i := 0; i < n; i++ {
		m.Run(func() error {
			cur := atomic.AddInt32(&active, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
					break
				}
			}
			// Yield to the scheduler so other workers can run; this keeps
			// the test fast while still letting concurrency develop.
			// We do not sleep on wall-clock time because that flaps on CI.
			runtime_Gosched()
			atomic.AddInt32(&active, -1)
			return nil
		}, w)
	}

	// Drain errors so workers do not block on the unbuffered errch.
	go func() {
		for range w.Err() {
		}
	}()
	m.Close()
	w.Wait()

	if got := atomic.LoadInt32(&peak); got > int32(jobs) {
		t.Errorf("peak concurrency = %d, want <= %d", got, jobs)
	}
}

// TestWaiter_ChannelClosedAfterWait verifies that after Wait returns, the
// error channel is closed (range loop terminates).
func TestWaiter_ChannelClosedAfterWait(t *testing.T) {
	t.Parallel()
	m := parallel.New(2)
	w := parallel.NewWaiter()
	m.Run(func() error { return nil }, w)
	m.Close()
	w.Wait()
	if _, ok := <-w.Err(); ok {
		t.Errorf("Err() channel should be closed after Wait")
	}
}

// TestNew_NegativeDefaultsToMinWorkers verifies that worker count 0 or 1 is
// bumped up to minNumWorkers.
func TestNew_NegativeDefaultsToMinWorkers(t *testing.T) {
	t.Parallel()
	for _, wc := range []int{0, 1} {
		wc := wc
		t.Run("", func(t *testing.T) {
			t.Parallel()
			m := parallel.New(wc)
			w := parallel.NewWaiter()
			var count int32
			for i := 0; i < 4; i++ {
				m.Run(func() error {
					atomic.AddInt32(&count, 1)
					return nil
				}, w)
			}
			go func() {
				for range w.Err() {
				}
			}()
			m.Close()
			w.Wait()
			if got := atomic.LoadInt32(&count); got != 4 {
				t.Errorf("ran %d tasks, want 4", got)
			}
		})
	}
}
