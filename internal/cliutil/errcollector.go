// ErrorCollector centralizes the "drain the Waiter's error channel while
// the main goroutine keeps submitting tasks" pattern shared by cp, mv,
// sync and select. Both the drain goroutine and the submission loop used
// to append to a plain []error, which was a data race; the collector
// serializes every append under a mutex so either side can call Collect
// safely.
package cliutil

import (
	"sync"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
)

// ErrorCollector aggregates task errors from concurrent producers. It
// applies the shared filtering rules: cancelations are dropped, warnings
// are logged at debug level, and real errors are logged at error level and
// recorded for the final Aggregate.
type ErrorCollector struct {
	// op is the operation name used in log messages ("cp", "sync", ...).
	op string

	mu   sync.Mutex
	errs []error
}

// NewErrorCollector creates a collector for the given operation name.
func NewErrorCollector(op string) *ErrorCollector {
	return &ErrorCollector{op: op}
}

// Collect filters and records err. It is safe to call from any goroutine:
// the drain goroutine started by Drain and the caller's submission loop
// may collect concurrently.
func (c *ErrorCollector) Collect(err error) {
	if err == nil || errorpkg.IsCancelation(err) {
		return
	}
	if errorpkg.IsWarning(err) {
		log.Debug(log.DebugMessage{Operation: c.op, Err: err.Error()})
		return
	}
	log.Error(log.ErrorMessage{Operation: c.op, Err: err.Error()})
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
}

// HasError reports whether at least one non-warning error has been
// collected. Submission loops use it to stop scheduling further tasks
// (e.g. sync --exit-on-error) while the drain goroutine keeps draining, so
// in-flight tasks that fail never block on the Waiter's unbuffered error
// channel.
func (c *ErrorCollector) HasError() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errs) > 0
}

// Drain starts a goroutine that consumes waiter.Err() into the collector
// and returns a function that blocks until the channel is closed. The
// returned function must be called after waiter.Wait() and before
// Aggregate so no late error is lost.
//
// The goroutine never stops early: parallel.Waiter's error channel is
// unbuffered and task goroutines block on send, so breaking out of the
// loop would deadlock waiter.Wait() as soon as another task errors.
func (c *ErrorCollector) Drain(waiter *parallel.Waiter) (wait func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for err := range waiter.Err() {
			c.Collect(err)
		}
	}()
	return func() { <-done }
}

// Aggregate joins the collected errors via AggregateErrors. It returns nil
// when nothing but warnings/cancelations was collected.
func (c *ErrorCollector) Aggregate() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return AggregateErrors(c.errs)
}
