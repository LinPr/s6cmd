package stat

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// reset puts the package back into its pristine "collection disabled"
// state so each test controls whether InitStat has run.
func reset() {
	enabled.Store(false)
	for i := range stats {
		stats[i] = syncMapStrInt64{mapStrInt64: map[string]int64{}}
	}
}

// TestAddDisabledIsNoOp verifies that until InitStat is called, Add is a
// no-op and Statistics returns an empty slice.
func TestAddDisabledIsNoOp(t *testing.T) {
	reset()
	Add("cp", true)
	Add("cp", false)
	if got := Statistics(); len(got) != 0 {
		t.Errorf("Statistics() with collection disabled = %v, want empty", got)
	}
}

// TestAddAndStatistics verifies counting and the sorted, per-operation
// success/error split.
func TestAddAndStatistics(t *testing.T) {
	reset()
	InitStat()
	Add("rm", true)
	Add("cp", true)
	Add("cp", true)
	Add("cp", false)

	got := Statistics()
	if len(got) != 2 {
		t.Fatalf("Statistics() returned %d entries, want 2: %v", len(got), got)
	}
	// Sorted by operation name: cp before rm.
	if got[0].Operation != "cp" || got[0].Success != 2 || got[0].Error != 1 {
		t.Errorf("cp stats = %+v, want {cp 2 1}", got[0])
	}
	if got[1].Operation != "rm" || got[1].Success != 1 || got[1].Error != 0 {
		t.Errorf("rm stats = %+v, want {rm 1 0}", got[1])
	}
}

// TestCollectDefer verifies the defer-oriented Collect closure counts a
// success when *err is nil and an error otherwise.
func TestCollectDefer(t *testing.T) {
	reset()
	InitStat()

	var nilErr error
	Collect("mv", &nilErr)()
	realErr := errors.New("boom")
	Collect("mv", &realErr)()

	got := Statistics()
	if len(got) != 1 || got[0].Success != 1 || got[0].Error != 1 {
		t.Errorf("Statistics() = %v, want [{mv 1 1}]", got)
	}
}

// TestStatisticsConcurrentWithAdd exercises Statistics racing with Add
// under -race: the maps must be snapshotted under their mutexes.
func TestStatisticsConcurrentWithAdd(t *testing.T) {
	reset()
	InitStat()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				Add("cp", j%2 == 0)
				if j%10 == 0 {
					_ = Statistics()
				}
			}
		}(i)
	}
	wg.Wait()

	got := Statistics()
	if len(got) != 1 {
		t.Fatalf("Statistics() returned %d entries, want 1", len(got))
	}
	if total := got[0].Success + got[0].Error; total != 8*200 {
		t.Errorf("total = %d, want %d", total, 8*200)
	}
}

// TestStatsRendering pins the table and JSON renderings used by log.Stat.
func TestStatsRendering(t *testing.T) {
	s := Stats{{Operation: "cp", Success: 3, Error: 1}}
	table := s.String()
	for _, want := range []string{"Operation", "Total", "Error", "Success", "cp", "4", "1", "3"} {
		if !strings.Contains(table, want) {
			t.Errorf("String() missing %q:\n%s", want, table)
		}
	}
	j := s.JSON()
	if !strings.Contains(j, `"operation":"cp"`) || !strings.Contains(j, `"success":3`) || !strings.Contains(j, `"error":1`) {
		t.Errorf("JSON() = %q, missing expected fields", j)
	}
}
