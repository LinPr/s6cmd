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
// error. It recognizes context.Canceled and smithy.CanceledError, which is
// the form aws-sdk-go-v2 returns when an in-flight request is canceled. The
// check recurses through errors.Unwrap so wrapped errors are handled.
func IsCancelation(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	// smithy.CanceledError satisfies the interface checked by
	// transport/http when a request is canceled mid-flight. Use As so we
	// still match when the error is wrapped by middleware/op decorators.
	var canceledErr *smithy.CanceledError
	if errors.As(err, &canceledErr) {
		return true
	}

	// Fall back to a string match for opaque wrappers that don't
	// implement Unwrap correctly; smithy surfaces "canceled, ..." and
	// aws-sdk-go-v2 may bubble up "context canceled" verbatim.
	if containsCancelation(err.Error()) {
		return true
	}

	return false
}

// containsCancelation reports whether the message indicates a cancelation.
// It is intentionally narrow to avoid matching unrelated errors.
func containsCancelation(msg string) bool {
	return stringsContain(msg, "context canceled") ||
		stringsContain(msg, "context deadline exceeded") ||
		stringsContain(msg, "canceled,")
}

// stringsContain is a tiny wrapper so this file does not need to import
// strings solely for two substring checks.
func stringsContain(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
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

// IsWarning reports whether the given error is one of the sentinel warning
// errors. Warnings are surfaced to the user but do not fail the command.
func IsWarning(err error) bool {
	switch err {
	case ErrObjectExists,
		ErrObjectIsNewer,
		ErrObjectSizesMatch,
		ErrObjectIsNewerAndSizesMatch,
		ErrNoObjectFound,
		ErrGivenObjectNotFound,
		ErrObjectIsGlacier:
		return true
	}

	return false
}
