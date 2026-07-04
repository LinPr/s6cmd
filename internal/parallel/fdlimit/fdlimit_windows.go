//go:build windows

// Package fdlimit is a no-op on Windows; the platform does not expose
// RLIMIT_NOFILE, so Raise() simply succeeds without doing anything.
package fdlimit

// Raise is a no-op on Windows.
func Raise() error { return nil }
