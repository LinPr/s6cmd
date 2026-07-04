package errorpkg_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/aws/smithy-go"
)

// TestError_Error_NoUnderlying verifies that Error.Error returns the full
// command string when Err is nil.
func TestError_Error_NoUnderlying(t *testing.T) {
	t.Parallel()
	e := &errorpkg.Error{Op: "cp", Src: "s3://b/a", Dst: "s3://b/b"}
	want := "cp s3://b/a s3://b/b"
	if got := e.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestError_Error_WithUnderlying verifies that Error.Error returns the
// underlying error message when Err is non-nil.
func TestError_Error_WithUnderlying(t *testing.T) {
	t.Parallel()
	e := &errorpkg.Error{
		Op:   "cp",
		Src:  "s3://b/a",
		Dst:  "s3://b/b",
		Err:  errors.New("boom"),
	}
	if got := e.Error(); got != "boom" {
		t.Errorf("Error() = %q, want %q", got, "boom")
	}
}

// TestError_FullCommand verifies FullCommand formats Op/Src/Dst.
func TestError_FullCommand(t *testing.T) {
	t.Parallel()
	e := &errorpkg.Error{Op: "mv", Src: "a", Dst: "b"}
	want := "mv a b"
	if got := e.FullCommand(); got != want {
		t.Errorf("FullCommand() = %q, want %q", got, want)
	}
}

// TestError_Unwrap verifies Unwrap returns the underlying error so
// errors.Is/errors.As can walk the chain.
func TestError_Unwrap(t *testing.T) {
	t.Parallel()
	base := errors.New("base")
	e := &errorpkg.Error{Op: "cp", Src: "a", Dst: "b", Err: base}
	if !errors.Is(e, base) {
		t.Errorf("errors.Is(e, base) = false, want true")
	}
	if got := e.Unwrap(); got != base {
		t.Errorf("Unwrap() = %v, want %v", got, base)
	}
}

// TestIsCancelation_NilError verifies nil is not a cancelation.
func TestIsCancelation_NilError(t *testing.T) {
	t.Parallel()
	if errorpkg.IsCancelation(nil) {
		t.Errorf("IsCancelation(nil) = true, want false")
	}
}

// TestIsCancelation_ContextCanceled verifies context.Canceled is a
// cancelation.
func TestIsCancelation_ContextCanceled(t *testing.T) {
	t.Parallel()
	if !errorpkg.IsCancelation(context.Canceled) {
		t.Errorf("IsCancelation(context.Canceled) = false, want true")
	}
}

// TestIsCancelation_WrappedContextCanceled verifies that an error wrapping
// context.Canceled is still recognized as a cancelation.
func TestIsCancelation_WrappedContextCanceled(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("wrap: %w", context.Canceled)
	if !errorpkg.IsCancelation(wrapped) {
		t.Errorf("IsCancelation(wrapped) = false, want true")
	}
}

// TestIsCancelation_SmithyCanceled verifies that smithy.CanceledError is
// recognized.
func TestIsCancelation_SmithyCanceled(t *testing.T) {
	t.Parallel()
	canceled := &smithy.CanceledError{}
	if !errorpkg.IsCancelation(canceled) {
		t.Errorf("IsCancelation(*smithy.CanceledError) = false, want true")
	}
}

// TestIsCancelation_JoinedWithCanceled verifies that errors.Join of a
// normal error and context.Canceled is recognized (the joined chain
// contains context.Canceled).
func TestIsCancelation_JoinedWithCanceled(t *testing.T) {
	t.Parallel()
	joined := errors.Join(errors.New("boom"), context.Canceled)
	if !errorpkg.IsCancelation(joined) {
		t.Errorf("IsCancelation(joined) = false, want true")
	}
}

// TestIsCancelation_StringFallback verifies the string-match fallback for
// opaque wrappers that don't implement Unwrap correctly.
func TestIsCancelation_StringFallback(t *testing.T) {
	t.Parallel()
	// A plain errors.New with the cancelation message is not unwrappable
	// to context.Canceled; the fallback string match should catch it.
	cases := []string{
		"context canceled",
		"operation canceled, please retry",
		"context deadline exceeded",
	}
	for _, msg := range cases {
		err := errors.New(msg)
		if !errorpkg.IsCancelation(err) {
			t.Errorf("IsCancelation(%q) = false, want true", msg)
		}
	}
}

// TestIsCancelation_NotCancelation verifies that unrelated errors are not
// flagged.
func TestIsCancelation_NotCancelation(t *testing.T) {
	t.Parallel()
	if errorpkg.IsCancelation(errors.New("boom")) {
		t.Errorf("IsCancelation(boom) = true, want false")
	}
}

// TestIsWarning_Sentinels verifies each warning sentinel is recognized.
func TestIsWarning_Sentinels(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		errorpkg.ErrObjectExists,
		errorpkg.ErrObjectIsNewer,
		errorpkg.ErrObjectSizesMatch,
		errorpkg.ErrObjectIsNewerAndSizesMatch,
		errorpkg.ErrNoObjectFound,
		errorpkg.ErrGivenObjectNotFound,
		errorpkg.ErrObjectIsGlacier,
	}
	for _, e := range sentinels {
		if !errorpkg.IsWarning(e) {
			t.Errorf("IsWarning(%q) = false, want true", e)
		}
	}
}

// TestIsWarning_NotWarning verifies plain errors are not warnings.
func TestIsWarning_NotWarning(t *testing.T) {
	t.Parallel()
	if errorpkg.IsWarning(errors.New("boom")) {
		t.Errorf("IsWarning(boom) = true, want false")
	}
	if errorpkg.IsWarning(nil) {
		t.Errorf("IsWarning(nil) = true, want false")
	}
}

// TestIsWarning_WrappedSentinel verifies that wrapping a sentinel still
// reports true. This documents the current implementation: IsWarning uses a
// direct == comparison, so wrapped sentinels are NOT recognized. Callers
// should pass the unwrapped sentinel.
func TestIsWarning_WrappedSentinel(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("cp: %w", errorpkg.ErrObjectExists)
	if errorpkg.IsWarning(wrapped) {
		t.Errorf("IsWarning(wrapped ErrObjectExists) = true, want false (sentinels are compared by ==")
	}
	// Unwrapping first gives the sentinel back, and IsWarning must accept it.
	if unwrapped := errors.Unwrap(wrapped); !errorpkg.IsWarning(unwrapped) {
		t.Errorf("IsWarning(Unwrap(wrapped)) = false, want true")
	}
}

// TestErrObjectIsNewerAndSizesMatch_Message verifies the composed sentinel
// mentions both component sentinels.
func TestErrObjectIsNewerAndSizesMatch_Message(t *testing.T) {
	t.Parallel()
	msg := errorpkg.ErrObjectIsNewerAndSizesMatch.Error()
	if !strings.Contains(msg, errorpkg.ErrObjectIsNewer.Error()) {
		t.Errorf("message %q does not contain %q", msg, errorpkg.ErrObjectIsNewer.Error())
	}
	if !strings.Contains(msg, errorpkg.ErrObjectSizesMatch.Error()) {
		t.Errorf("message %q does not contain %q", msg, errorpkg.ErrObjectSizesMatch.Error())
	}
}
