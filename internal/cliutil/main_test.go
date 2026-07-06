package cliutil

import (
	"os"
	"testing"

	"github.com/LinPr/s6cmd/log"
)

// TestMain initializes the global logger so ErrorCollector's log.Error /
// log.Debug calls do not dereference a nil *Logger. main.go does this for
// production runs; the test binary has its own main, so we repeat the
// minimal init here.
func TestMain(m *testing.M) {
	log.Init(log.LevelInfo, false)
	code := m.Run()
	log.Close()
	os.Exit(code)
}
