// Package progressbar defines the progress-reporting interface used by
// commands. The real implementation (CommandProgressBar in s5cmd) pulls in
// github.com/cheggaaa/pb/v3; s6cmd does not want that dependency for this
// pass, so New returns a NoOp regardless of visibility. The interface is
// in place so commands can be wired against it now and a real bar can be
// dropped in later without touching call sites.
package progressbar

// ProgressBar is the surface commands depend on. It tracks both byte-level
// and object-level progress so callers can report whichever makes sense
// for their operation.
type ProgressBar interface {
	// Start begins progress reporting. It is called once before the
	// first work unit is processed.
	Start()
	// Finish stops progress reporting and flushes any final state.
	Finish()
	// IncrementCompletedObjects advances the count of completed
	// objects by one.
	IncrementCompletedObjects()
	// IncrementTotalObjects advances the count of total objects by
	// one (used when the total is not known up front).
	IncrementTotalObjects()
	// AddCompletedBytes adds n to the count of completed bytes.
	AddCompletedBytes(n int64)
	// AddTotalBytes adds n to the count of total bytes.
	AddTotalBytes(n int64)
}

// NoOp is a ProgressBar that does nothing. It is the value returned by New
// until a real implementation is added.
type NoOp struct{}

// Start implements ProgressBar.
func (pb *NoOp) Start() {}

// Finish implements ProgressBar.
func (pb *NoOp) Finish() {}

// IncrementCompletedObjects implements ProgressBar.
func (pb *NoOp) IncrementCompletedObjects() {}

// IncrementTotalObjects implements ProgressBar.
func (pb *NoOp) IncrementTotalObjects() {}

// AddCompletedBytes implements ProgressBar.
func (pb *NoOp) AddCompletedBytes(n int64) {}

// AddTotalBytes implements ProgressBar.
func (pb *NoOp) AddTotalBytes(n int64) {}

// New returns a ProgressBar. isVisible hints whether a real bar would be
// shown, but the current implementation always returns a NoOp because the
// progressbar dependency is not yet wired in.
//
// TODO: when github.com/cheggaaa/pb/v3 (or equivalent) is added, return a
// *CommandProgressBar when isVisible is true.
func New(isVisible bool) ProgressBar {
	_ = isVisible
	return &NoOp{}
}
