// Package stat collects per-operation success/error counts so a summary
// can be printed at the end of a run. It is opt-in: until InitStat is
// called (root --stat flag), Add and Collect are cheap no-ops, so callers
// can report unconditionally without worrying about whether stats are
// enabled.
package stat

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	// enabled gates every collection call. It is an atomic.Bool because
	// Add/Collect run on worker goroutines while InitStat runs on the
	// main goroutine.
	enabled atomic.Bool
	stats   statistics
)

// statistics holds two guarded maps: total attempts and successful
// attempts, keyed by operation name.
type statistics [2]syncMapStrInt64

// InitStat enables statistics collection. It must be called before any
// command runs (the root PersistentPreRunE does this when --stat is set);
// otherwise Add and Collect are no-ops.
func InitStat() {
	for i := range stats {
		stats[i] = syncMapStrInt64{
			mapStrInt64: map[string]int64{},
		}
	}
	enabled.Store(true)
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

// snapshot returns a copy of the map taken under the lock.
func (s *syncMapStrInt64) snapshot() map[string]int64 {
	s.Lock()
	defer s.Unlock()
	out := make(map[string]int64, len(s.mapStrInt64))
	for k, v := range s.mapStrInt64 {
		out[k] = v
	}
	return out
}

// Stat is a single operation's statistics.
type Stat struct {
	Operation string `json:"operation"`
	Success   int64  `json:"success"`
	Error     int64  `json:"error"`
}

// Add records one completed operation. It is a no-op until InitStat has
// been called. The log package calls it for every operation-attributed
// Info (success) and Error (failure) message, so enabling --stat requires
// no per-command wiring.
func Add(op string, success bool) {
	if !enabled.Load() {
		return
	}
	if success {
		stats[succCount].add(op, 1)
	}
	stats[totalCount].add(op, 1)
}

// Collect returns a closure intended for `defer` at the top of an Action.
// When the deferred call runs, if *err is nil the operation's success
// counter is incremented, and the total counter is always incremented
// (provided stat collection is enabled). The closure is a no-op when stat
// collection is disabled, so it is safe to defer unconditionally.
func Collect(op string, err *error) func() {
	return func() {
		Add(op, err == nil || *err == nil)
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

// Statistics returns the stats collected so far, sorted by operation name
// so the summary table is deterministic. Returns an empty slice if stat
// collection is disabled. The maps are snapshotted under their mutexes so
// Statistics is safe to call while workers are still adding.
func Statistics() Stats {
	if !enabled.Load() {
		return Stats{}
	}
	totals := stats[totalCount].snapshot()
	successes := stats[succCount].snapshot()
	result := make(Stats, 0, len(totals))
	for op, total := range totals {
		success := successes[op]
		result = append(result, Stat{
			Operation: op,
			Success:   success,
			Error:     total - success,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Operation < result[j].Operation })
	return result
}
