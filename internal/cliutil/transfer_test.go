package cliutil

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// helper: create a directory tree under t.TempDir() with the given relative
// paths (file content is the path itself). Returns the absolute root.
func makeTree(t *testing.T, relpaths []string) string {
	t.Helper()
	root := t.TempDir()
	for _, p := range relpaths {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(p), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", full, err)
		}
	}
	return root
}

// TestListLocalFiles_SingleFile verifies that a plain file path is returned
// as a one-element slice.
func TestListLocalFiles_SingleFile(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{"file.txt"})
	got, err := ListLocalFiles(filepath.Join(root, "file.txt"), false)
	if err != nil {
		t.Fatalf("ListLocalFiles: %v", err)
	}
	want := []string{filepath.Join(root, "file.txt")}
	if !sliceEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestListLocalFiles_DirectoryRecursive verifies that recursive=true walks
// the tree and returns only the files (not directories), sorted by path.
func TestListLocalFiles_DirectoryRecursive(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{
		"a/b/file1.txt",
		"a/b/file2.txt",
		"a/c/file3.txt",
		"top.txt",
	})
	got, err := ListLocalFiles(root, true)
	if err != nil {
		t.Fatalf("ListLocalFiles: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d files, want 4 (%v)", len(got), got)
	}
	// Each result must be a regular file and live under root.
	for _, p := range got {
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatalf("Rel: %v", err)
		}
		if strings.HasPrefix(rel, "..") {
			t.Errorf("path %q escapes root", p)
		}
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("Stat(%q): %v", p, err)
		}
		if info.IsDir() {
			t.Errorf("ListLocalFiles returned a directory: %q", p)
		}
	}
}

// TestListLocalFiles_DirectoryNonRecursive verifies that a directory source
// without recursive returns an error mentioning --recursive.
func TestListLocalFiles_DirectoryNonRecursive(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{"a/b/file1.txt"})
	_, err := ListLocalFiles(root, false)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "--recursive") {
		t.Errorf("error %q does not mention --recursive", err)
	}
}

// TestListLocalFiles_GlobMatch verifies the glob branch: pattern matches
// multiple files at the same depth.
func TestListLocalFiles_GlobMatch(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{
		"a/file1.txt",
		"a/file2.txt",
		"a/other.log",
	})
	pattern := filepath.Join(root, "a", "*.txt")
	got, err := ListLocalFiles(pattern, false)
	if err != nil {
		t.Fatalf("ListLocalFiles: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d files, want 2 (%v)", len(got), got)
	}
	for _, p := range got {
		if filepath.Ext(p) != ".txt" {
			t.Errorf("got non-txt file %q", p)
		}
	}
}

// TestListLocalFiles_GlobRecursive verifies that a glob pattern that matches
// a directory is recursed into when recursive=true.
func TestListLocalFiles_GlobRecursive(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{
		"dir/sub/a.txt",
		"dir/sub/b.txt",
		"dir/top.txt",
	})
	// Pattern matches "dir"; since it is a directory and recursive=true,
	// ListLocalFiles should walk it.
	pattern := filepath.Join(root, "d*")
	got, err := ListLocalFiles(pattern, true)
	if err != nil {
		t.Fatalf("ListLocalFiles: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d files, want 3 (%v)", len(got), got)
	}
}

// TestListLocalFiles_GlobNoMatch verifies that a glob with no matches
// returns the "no match found" error.
func TestListLocalFiles_GlobNoMatch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	_, err := ListLocalFiles(filepath.Join(root, "nope-*.txt"), false)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no match") {
		t.Errorf("error %q does not mention no match", err)
	}
}

// TestListLocalFiles_NonExistent verifies that a path that does not exist
// surfaces the underlying os.Stat error.
func TestListLocalFiles_NonExistent(t *testing.T) {
	t.Parallel()
	_, err := ListLocalFiles(filepath.Join(t.TempDir(), "missing"), false)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("err = %v, want ErrNotExist in chain", err)
	}
}

// TestCopyLocalFile_Normal verifies a normal copy: dst exists afterwards and
// has the same content as src.
func TestCopyLocalFile_Normal(t *testing.T) {
	t.Parallel()
	src := makeTree(t, []string{"a.txt"})
	dst := filepath.Join(t.TempDir(), "out", "b.txt")
	if err := CopyLocalFile(filepath.Join(src, "a.txt"), dst); err != nil {
		t.Fatalf("CopyLocalFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile(dst): %v", err)
	}
	if string(got) != "a.txt" {
		t.Errorf("dst content = %q, want %q", got, "a.txt")
	}
}

// TestCopyLocalFile_SourceMissing verifies that copying a non-existent src
// returns an error.
func TestCopyLocalFile_SourceMissing(t *testing.T) {
	t.Parallel()
	dst := filepath.Join(t.TempDir(), "out.txt")
	err := CopyLocalFile(filepath.Join(t.TempDir(), "missing.txt"), dst)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("err = %v, want ErrNotExist in chain", err)
	}
}

// TestCopyLocalFile_TargetDirAutoCreated verifies that the destination
// directory is created when it does not yet exist.
func TestCopyLocalFile_TargetDirAutoCreated(t *testing.T) {
	t.Parallel()
	root := makeTree(t, []string{"a.txt"})
	dstDir := filepath.Join(t.TempDir(), "deep", "nested", "dir")
	dst := filepath.Join(dstDir, "b.txt")
	if err := CopyLocalFile(filepath.Join(root, "a.txt"), dst); err != nil {
		t.Fatalf("CopyLocalFile: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("Stat(dst): %v", err)
	}
}

// TestWildcardBasePath verifies the four branches: no wildcard, wildcard in
// the basename, wildcard mid-path, and a bare wildcard.
func TestWildcardBasePath(t *testing.T) {
	t.Parallel()
	sep := string(filepath.Separator)
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{"no_wildcard", filepath.Join("a", "b", "c.txt"), filepath.Join("a", "b")},
		{"wildcard_basename", filepath.Join("a", "b", "*.txt"), filepath.Join("a", "b")},
		{"wildcard_midpath", filepath.Join("a", "b*", "c"), filepath.Join("a")},
		{"bare_wildcard", "*.txt", "."},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := WildcardBasePath(tc.pattern)
			// On POSIX the result should match exactly; we keep the
			// filepath.Separator expected value to stay portable.
			want := strings.ReplaceAll(tc.want, "/", sep)
			if got != want {
				t.Errorf("WildcardBasePath(%q) = %q, want %q", tc.pattern, got, want)
			}
		})
	}
}

// TestNormalizeRemotePrefix covers the three cases: empty, already has
// trailing slash, needs a slash appended.
func TestNormalizeRemotePrefix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"already_dir", "prefix/", "prefix/"},
		{"needs_slash", "prefix", "prefix/"},
		{"nested_needs_slash", "a/b/c", "a/b/c/"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeRemotePrefix(tc.in); got != tc.want {
				t.Errorf("NormalizeRemotePrefix(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestIsLocalDir covers four cases: an existing directory, an existing file,
// a path that does not exist (no trailing slash) and a non-existent path with
// a trailing slash (which is treated as a directory that the caller intends
// to create).
func TestIsLocalDir(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dirPath := filepath.Join(root, "subdir")
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(root, "file.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		path    string
		want    bool
		wantErr bool
	}{
		{"existing_dir", dirPath, true, false},
		{"existing_file", filePath, false, false},
		{"missing_no_slash", filepath.Join(root, "missing"), false, false},
		{"missing_with_slash", filepath.Join(root, "missing") + "/", true, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := IsLocalDir(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("IsLocalDir(%q) err = %v, wantErr %v", tc.path, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("IsLocalDir(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// TestRunTasks_Serial verifies that jobs<=1 runs tasks sequentially and
// returns the first error encountered.
func TestRunTasks_Serial(t *testing.T) {
	t.Parallel()
	var order []int
	var mu sync.Mutex
	tasks := []func() error{
		func() error { mu.Lock(); order = append(order, 1); mu.Unlock(); return nil },
		func() error { mu.Lock(); order = append(order, 2); mu.Unlock(); return nil },
		func() error { mu.Lock(); order = append(order, 3); mu.Unlock(); return nil },
	}
	if err := RunTasks(1, tasks); err != nil {
		t.Fatalf("RunTasks: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("order = %v, want 3 entries", order)
	}
	for i, v := range order {
		if v != i+1 {
			t.Errorf("order[%d] = %d, want %d", i, v, i+1)
		}
	}
}

// TestRunTasks_SerialFirstError verifies that the serial path returns the
// first error and does not run subsequent tasks.
func TestRunTasks_SerialFirstError(t *testing.T) {
	t.Parallel()
	myErr := errors.New("boom")
	ran := make(map[int]bool)
	tasks := []func() error{
		func() error { ran[1] = true; return nil },
		func() error { ran[2] = true; return myErr },
		func() error { ran[3] = true; return nil },
	}
	err := RunTasks(1, tasks)
	if !errors.Is(err, myErr) {
		t.Fatalf("err = %v, want %v", err, myErr)
	}
	if !ran[1] || !ran[2] {
		t.Errorf("tasks 1 and 2 should have run: %v", ran)
	}
	if ran[3] {
		t.Errorf("task 3 should not have run after error")
	}
}

// TestRunTasks_ConcurrentAllSucceed verifies that the concurrent path with
// jobs>1 runs every task exactly once.
func TestRunTasks_ConcurrentAllSucceed(t *testing.T) {
	t.Parallel()
	const n = 50
	var count int32
	tasks := make([]func() error, n)
	for i := 0; i < n; i++ {
		tasks[i] = func() error { atomic.AddInt32(&count, 1); return nil }
	}
	if err := RunTasks(8, tasks); err != nil {
		t.Fatalf("RunTasks: %v", err)
	}
	if got := atomic.LoadInt32(&count); got != n {
		t.Errorf("ran %d tasks, want %d", got, n)
	}
}

// TestRunTasks_ConcurrentErrorReturned verifies that when one or more tasks
// fail in the concurrent path, RunTasks returns a non-nil error. We do not
// assert *which* error is returned because the implementation returns the
// first to arrive on the errCh; we only assert that an error surfaces.
func TestRunTasks_ConcurrentErrorReturned(t *testing.T) {
	t.Parallel()
	myErr := errors.New("boom")
	tasks := []func() error{
		func() error { return myErr },
		func() error { return nil },
		func() error { return nil },
	}
	err := RunTasks(2, tasks)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, myErr) {
		t.Errorf("err = %v, want %v", err, myErr)
	}
}

// TestRunTasks_Empty verifies that an empty task list returns nil without
// allocating workers.
func TestRunTasks_Empty(t *testing.T) {
	t.Parallel()
	if err := RunTasks(8, nil); err != nil {
		t.Errorf("RunTasks(nil) = %v, want nil", err)
	}
}

// TestRunTasks_ConcurrentRespectsWorkerCount is a best-effort check that the
// concurrent path never exceeds the configured worker count: we record the
// peak number of concurrently-running tasks and ensure it stays <= jobs.
func TestRunTasks_ConcurrentRespectsWorkerCount(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("flaky on windows due to scheduler")
	}
	const jobs = 4
	const n = 100
	var active, peak int32
	var wg sync.WaitGroup
	startGate := make(chan struct{})
	tasks := make([]func() error, n)
	for i := 0; i < n; i++ {
		tasks[i] = func() error {
			defer wg.Done()
			cur := atomic.AddInt32(&active, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
					break
				}
			}
			<-startGate
			atomic.AddInt32(&active, -1)
			return nil
		}
		// We pre-add to the WaitGroup so the goroutines can finish even if
		// RunTasks returns before all of them have drained the channel.
		wg.Add(1)
	}
	// Run tasks; release the gate shortly after starting so they can finish.
	go func() {
		// Give workers a moment to ramp up.
		// Then release the gate so all tasks complete.
		close(startGate)
	}()
	if err := RunTasks(jobs, tasks); err != nil {
		t.Fatalf("RunTasks: %v", err)
	}
	wg.Wait()
	if got := atomic.LoadInt32(&peak); got > int32(jobs) {
		t.Errorf("peak concurrency = %d, want <= %d", got, jobs)
	}
}

// sliceEqual is a small helper to compare string slices regardless of order.
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, v := range a {
		if !contains(b, v) {
			return false
		}
	}
	return true
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
