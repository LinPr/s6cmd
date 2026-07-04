// Package e2e contains end-to-end tests that run against a real s6cmd
// binary, compiled on the fly at the start of the test run. The tests use
// gofakes3 to spin up an in-process S3-compatible server so no external
// services are required.
package e2e

import (
	"flag"
	"os"
	"testing"
)

// s6cmdPath is the path to the binary built by goBuildS6cmd in TestMain.
var s6cmdPath string

// TestMain parses the test flags, builds the s6cmd binary into a temp dir,
// runs the tests, and cleans up on exit.
func TestMain(m *testing.M) {
	flag.Parse()

	cleanup := goBuildS6cmd()
	code := m.Run()
	cleanup()
	os.Exit(code)
}
