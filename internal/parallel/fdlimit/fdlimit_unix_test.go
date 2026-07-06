//go:build !windows

package fdlimit

import (
	"testing"

	"golang.org/x/sys/unix"
)

// TestRaise verifies that after Raise() the soft limit is at least
// min(hard limit, preferredOpenFilesLimit). The old target of 1024
// equalled the typical Linux soft default, making Raise a no-op while the
// default worker pool could easily exhaust it.
func TestRaise(t *testing.T) {
	if err := Raise(); err != nil {
		t.Fatalf("Raise(): %v", err)
	}

	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		t.Fatalf("Getrlimit: %v", err)
	}
	want := rLimit.Max
	if want > preferredOpenFilesLimit {
		want = preferredOpenFilesLimit
	}
	if rLimit.Cur < want {
		t.Errorf("soft limit = %d, want >= min(hard=%d, %d) = %d", rLimit.Cur, rLimit.Max, preferredOpenFilesLimit, want)
	}
}

// TestRaiseDoesNotLowerLimit verifies the no-downgrade contract: when the
// current soft limit is already at or above the target, Raise leaves it
// untouched instead of pulling it down to preferredOpenFilesLimit.
func TestRaiseDoesNotLowerLimit(t *testing.T) {
	var orig unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &orig); err != nil {
		t.Fatalf("Getrlimit: %v", err)
	}
	defer func() {
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &orig); err != nil {
			t.Fatalf("restore rlimit: %v", err)
		}
	}()

	if orig.Max <= preferredOpenFilesLimit {
		t.Skipf("hard limit %d <= preferred %d; cannot place the soft limit above the target", orig.Max, preferredOpenFilesLimit)
	}

	// Push the soft limit above the preferred target (allowed, because it
	// stays within the hard limit).
	high := orig
	high.Cur = orig.Max
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &high); err != nil {
		t.Skipf("cannot raise soft limit to hard limit: %v", err)
	}

	if err := Raise(); err != nil {
		t.Fatalf("Raise(): %v", err)
	}

	var got unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &got); err != nil {
		t.Fatalf("Getrlimit: %v", err)
	}
	if got.Cur != high.Cur {
		t.Errorf("Raise lowered the soft limit from %d to %d", high.Cur, got.Cur)
	}
}
