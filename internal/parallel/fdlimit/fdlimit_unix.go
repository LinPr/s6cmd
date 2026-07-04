//go:build !windows

// Package fdlimit raises the soft limit on open file descriptors
// (RLIMIT_NOFILE) so the process can run many concurrent I/O operations.
//
// Raise() attempts to raise the soft limit to at least minOpenFilesLimit.
// Failures are returned but expected to be ignored by the caller.
package fdlimit

import (
	"golang.org/x/sys/unix"
)

const (
	minOpenFilesLimit = 1024
)

// Raise raises the soft RLIMIT_NOFILE to at least minOpenFilesLimit when
// the current limit is below it and the hard limit allows it. Errors are
// returned but expected to be ignored by the caller.
func Raise() error {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}

	if rLimit.Cur >= minOpenFilesLimit {
		return nil
	}

	if rLimit.Max < minOpenFilesLimit {
		// Hard limit is too low to reach minOpenFilesLimit; leave the
		// current limit untouched rather than risking a downgrade.
		return nil
	}

	rLimit.Cur = minOpenFilesLimit
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}
