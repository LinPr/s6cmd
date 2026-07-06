// Package errorpkg provides error types and helpers used across s6cmd.
//
// The Error struct uses plain strings for Src/Dst so it can be reused
// before the storage/url abstraction is fully wired up. IsCancelation
// recognizes context.Canceled and smithy's CanceledError (the two forms
// aws-sdk-go-v2 surfaces when a request is canceled) without depending on
// the storage layer.
package errorpkg

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/smithy-go"
)

// Error is the type that implements error interface. It decorates an
// underlying error with the operation that was being performed and the
// source/destination arguments involved.
type Error struct {
	// Op is the operation being performed, usually the name of the
	// command or method being invoked (copy, move, etc.)
	Op string
	// Src is the source argument, rendered as a string for logging.
	Src string
	// Dst is the destination argument, rendered as a string for logging.
	Dst string
	// Err is the underlying error if any.
	Err error
}

// FullCommand returns the command string that the error occurred at.
func (e *Error) FullCommand() string {
	return fmt.Sprintf("%v %v %v", e.Op, e.Src, e.Dst)
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Err == nil {
		return e.FullCommand()
	}
	return e.Err.Error()
}

// Unwrap unwraps the error so callers can use errors.Is/errors.As against
// the underlying cause.
func (e *Error) Unwrap() error {
	return e.Err
}

// IsCancelation reports whether the given error is (or wraps) a cancelation
// error. Only context.Canceled counts: a canceled context means the user
// (or the signal handler) asked the run to stop, so per-object cancelation
// errors are not failures. context.DeadlineExceeded is deliberately NOT a
// cancelation — a timed-out transfer is a real failure and must surface in
// the exit code. errors.Is walks Unwrap chains, including errors.Join'ed
// lists and *errorpkg.Error wrappers.
func IsCancelation(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	// smithy.CanceledError is the marker aws-sdk-go-v2 returns when an
	// in-flight request is canceled. It wraps ctx.Err(), so the errors.Is
	// above already matches the common case; this handles a nil or opaque
	// inner error. A CanceledError wrapping context.DeadlineExceeded is a
	// timeout, not a cancelation, and stays a failure.
	var canceledErr *smithy.CanceledError
	if errors.As(err, &canceledErr) {
		return !errors.Is(err, context.DeadlineExceeded)
	}

	return false
}

// Sentinel errors used by commands to signal non-fatal conditions that
// should be logged as warnings rather than failures.
var (
	// ErrObjectExists indicates a specified object already exists.
	ErrObjectExists = errors.New("object already exists")

	// ErrObjectIsNewer indicates a specified object is newer or same age.
	ErrObjectIsNewer = errors.New("object is newer or same age")

	// ErrObjectSizesMatch indicates the sizes of objects match.
	ErrObjectSizesMatch = errors.New("object size matches")

	// ErrObjectIsNewerAndSizesMatch indicates the specified object is
	// newer or same age and the sizes of the objects match.
	ErrObjectIsNewerAndSizesMatch = fmt.Errorf("%v and %v", ErrObjectIsNewer, ErrObjectSizesMatch)

	// ErrNoObjectFound indicates no objects were found for the given
	// source.
	ErrNoObjectFound = errors.New("no object found")

	// ErrGivenObjectNotFound indicates a specific object was not found.
	ErrGivenObjectNotFound = errors.New("given object not found")

	// ErrObjectIsGlacier indicates the object is in Glacier storage class
	// and would require a restore before it can be downloaded.
	ErrObjectIsGlacier = errors.New("object is in Glacier storage class")
)

// warningSentinels is the set of errors recognized by IsWarning.
var warningSentinels = []error{
	ErrObjectExists,
	ErrObjectIsNewer,
	ErrObjectSizesMatch,
	ErrObjectIsNewerAndSizesMatch,
	ErrNoObjectFound,
	ErrGivenObjectNotFound,
	ErrObjectIsGlacier,
}

// IsWarning reports whether the given error is (or wraps) one of the
// sentinel warning errors. Warnings are surfaced to the user but do not
// fail the command. errors.Is is used per sentinel so wrapped sentinels
// (fmt.Errorf %w, *errorpkg.Error, errors.Join) are recognized too.
func IsWarning(err error) bool {
	for _, sentinel := range warningSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}

	return false
}
