//go:build !windows

// Package fdlimit raises the soft limit on open file descriptors
// (RLIMIT_NOFILE) so the process can run many concurrent I/O operations.
//
// Raise() attempts to raise the soft limit to min(hard limit,
// preferredOpenFilesLimit). Failures are returned but treated as advisory
// by the caller.
package fdlimit

import (
	"golang.org/x/sys/unix"
)

const (
	// preferredOpenFilesLimit is the soft-limit target. The default pool
	// runs 256 workers, each of which may hold several descriptors
	// (multipart streams, temp files), so the typical Linux soft default
	// of 1024 leaves no headroom and "too many open files" is reachable.
	preferredOpenFilesLimit = 65536
)

// Raise raises the soft RLIMIT_NOFILE to min(hard limit,
// preferredOpenFilesLimit) when the current limit is below that target.
// Errors are returned but expected to be treated as advisory by the
// caller: the process still works at the lower limit, just with less
// concurrency headroom.
func Raise() error {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}

	// Cap the target at the hard limit; unprivileged processes cannot
	// raise the soft limit beyond it.
	target := rLimit.Max
	if target > preferredOpenFilesLimit {
		target = preferredOpenFilesLimit
	}

	if rLimit.Cur >= target {
		return nil
	}

	rLimit.Cur = target
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}
