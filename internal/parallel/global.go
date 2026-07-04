// Package parallel exposes a process-wide singleton Manager so callers
// can schedule concurrent work without threading an instance through
// every layer of the application.
//
// Init must be called once at startup to raise the file-descriptor limit
// and construct the global Manager. Run panics if Init was not called,
// surfacing the misconfiguration early instead of silently spinning up a
// default-sized Manager.
package parallel

import "github.com/LinPr/s6cmd/internal/parallel/fdlimit"

const (
	// defaultWorkerCount is used when Init is invoked with a non-positive
	// worker count, matching s5cmd's historical default.
	defaultWorkerCount = 256
)

var (
	global *Manager
)

// Init raises the soft RLIMIT_NOFILE and constructs the global Manager.
// If workercount is non-positive, defaultWorkerCount is used.
func Init(workercount int) {
	if workercount <= 0 {
		workercount = defaultWorkerCount
	}
	_ = fdlimit.Raise()
	global = New(workercount)
}

// Close waits for all in-flight tasks on the global Manager to finish
// and then closes its semaphore. It is a no-op if Init was never called.
func Close() {
	if global != nil {
		global.Close()
	}
}

// Run schedules task on the global Manager. It panics if Init has not
// been called, since silently defaulting would hide a startup-ordering
// bug.
func Run(task Task, waiter *Waiter) {
	if global == nil {
		panic("parallel: Init must be called before Run")
	}
	global.Run(task, waiter)
}
