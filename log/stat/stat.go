// Package stat collects per-operation success/error counts so a summary
// can be printed at the end of a run. It is opt-in: until InitStat is
// called, Collect is a cheap no-op, so commands can always defer
// Collect(...) without worrying about whether stats are enabled.
package stat

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/LinPr/s6cmd/strutil"
)

const (
	// totalCount is the index into stats for the total count map.
	totalCount = iota
	// succCount is the index into stats for the success count map.
	succCount
)

var (
	enabled bool
	stats   statistics
)

// statistics holds two guarded maps: total attempts and successful
// attempts, keyed by operation name.
type statistics [2]syncMapStrInt64

// InitStat enables statistics collection. It must be called once at
// startup before any command runs; otherwise Collect is a no-op.
func InitStat() {
	enabled = true
	for i := range stats {
		stats[i] = syncMapStrInt64{
			Mutex:       sync.Mutex{},
			mapStrInt64: map[string]int64{},
		}
	}
}

// syncMapStrInt64 is a statically typed, synchronized map.
type syncMapStrInt64 struct {
	sync.Mutex
	mapStrInt64 map[string]int64
}

// add increments the value at key by val under the lock.
func (s *syncMapStrInt64) add(key string, val int64) {
	s.Lock()
	defer s.Unlock()
	s.mapStrInt64[key] += val
}

// Stat is a single operation's statistics.
type Stat struct {
	Operation string `json:"operation"`
	Success   int64  `json:"success"`
	Error     int64  `json:"error"`
}

// Collect returns a closure intended for `defer` at the top of an Action.
// When the deferred call runs, if *err is nil the operation's success
// counter is incremented, and the total counter is always incremented
// (provided stat collection is enabled). The closure is a no-op when stat
// collection is disabled, so it is safe to defer unconditionally.
func Collect(op string, err *error) func() {
	return func() {
		if !enabled {
			return
		}
		if err == nil || *err == nil {
			stats[succCount].add(op, 1)
		}
		stats[totalCount].add(op, 1)
	}
}

// Stats is a slice of Stat that implements log.Message.
type Stats []Stat

// String renders Stats as a right-aligned table.
func (s Stats) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 8, 1, '\t', tabwriter.AlignRight)
	fmt.Fprintf(w, "\n%s\t%s\t%s\t%s\t\n", "Operation", "Total", "Error", "Success")
	for _, stat := range s {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t\n", stat.Operation, stat.Error+stat.Success, stat.Error, stat.Success)
	}
	w.Flush()
	return buf.String()
}

// JSON renders Stats as one JSON object per line.
func (s Stats) JSON() string {
	var builder strings.Builder
	for _, stat := range s {
		builder.WriteString(strutil.JSON(stat) + "\n")
	}
	return builder.String()
}

// Statistics returns the stats collected so far. Returns an empty slice if
// stat collection is disabled.
func Statistics() Stats {
	if !enabled {
		return Stats{}
	}
	var result Stats
	for op, total := range stats[totalCount].mapStrInt64 {
		success := stats[succCount].mapStrInt64[op]
		result = append(result, Stat{
			Operation: op,
			Success:   success,
			Error:     total - success,
		})
	}
	return result
}
