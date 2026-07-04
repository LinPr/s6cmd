package main

import (
	"github.com/LinPr/s6cmd/cmd"
	"github.com/LinPr/s6cmd/internal/parallel"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/log/stat"
)

func main() {
	// Initialize process-wide infrastructure before any command runs.
	// parallel.Init raises the soft RLIMIT_NOFILE and constructs the
	// global Manager so parallel.Run does not panic. log.Init starts the
	// single drain goroutine that fans messages onto stdout/stderr.
	// stat.InitStat enables per-operation counters; until InitStat is
	// called, stat.Collect is a no-op, so deferring it is always safe.
	parallel.Init(0)
	log.Init(log.LevelInfo, false)
	stat.InitStat()

	cmd.Execute()
}
