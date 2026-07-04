// Package version exposes the build-time version metadata for s6cmd.
//
// Version and GitCommit are stamped at link time via -ldflags
// "-X github.com/LinPr/s6cmd/version.Version=... -X .../version.GitCommit=...".
// When the binary is built without those flags (e.g. `go build .`), the
// defaults below are used so the command still prints a recognizable value
// instead of an empty string.
package version

import "fmt"

// Version is the human-readable version of the binary. It defaults to "dev"
// when the binary is not stamped via -ldflags.
var Version = "dev"

// GitCommit is the full Git SHA the binary was built from. It defaults to
// "none" when not stamped.
var GitCommit = "none"

// GetHumanVersion returns a single-line version string suitable for the
// `version` command. When both Version and GitCommit carry their defaults
// (i.e. an unstamped dev build) it returns "dev (none)"; when only the commit
// is known it falls back to it so a CI-built binary without a tag still
// prints something useful.
func GetHumanVersion() string {
	if Version == "dev" && GitCommit == "none" {
		return "dev"
	}
	if Version != "" && GitCommit != "" && GitCommit != "none" {
		return fmt.Sprintf("%s (commit: %s)", Version, GitCommit)
	}
	if Version != "" {
		return Version
	}
	return GitCommit
}
