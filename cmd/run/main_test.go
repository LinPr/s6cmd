package run

import (
	"os"
	"testing"

	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
)

// TestMain initializes the process-wide infrastructure (parallel.Manager
// and the global logger) so the run command's parallel.Run calls do not
// panic and log.Error does not dereference a nil *Logger. main.go does
// this for production runs; the test binary has its own main, so we
// repeat the minimal init here.
func TestMain(m *testing.M) {
	parallel.Init(0)
	log.Init(log.LevelInfo, false)
	code := m.Run()
	parallel.Close()
	log.Close()
	os.Exit(code)
}
