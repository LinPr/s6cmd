package cliutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func WildcardBasePath(pattern string) string {
	idx := strings.IndexAny(pattern, "?*")
	if idx < 0 {
		return filepath.Dir(pattern)
	}
	prefix := pattern[:idx]
	return filepath.Dir(prefix)
}

func ListLocalFiles(src string, recursive bool) ([]string, error) {
	if strings.ContainsAny(src, "?*") {
		matches, err := filepath.Glob(src)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no match found for %q", src)
		}
		files := make([]string, 0, len(matches))
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil {
				return nil, err
			}
			if info.IsDir() {
				if !recursive {
					return nil, fmt.Errorf("%s is a directory (use --recursive)", m)
				}
				sub, err := ListLocalFiles(filepath.Join(m, "*"), recursive)
				if err != nil {
					return nil, err
				}
				files = append(files, sub...)
				continue
			}
			files = append(files, m)
		}
		return files, nil
	}

	info, err := os.Stat(src)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{src}, nil
	}
	if !recursive {
		return nil, fmt.Errorf("%s is a directory (use --recursive)", src)
	}

	files := make([]string, 0, 128)
	if err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return nil, err
	}
	return files, nil
}

// CopyLocalFile copies src to dst, creating dst's parent directories as
// needed. The data is written to a temporary file in the destination
// directory and renamed into place only after a successful write+close, so
// a failed copy (e.g. ENOSPC surfacing at Close) never truncates or
// replaces an existing destination file.
func CopyLocalFile(src, dst string) error {
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := os.CreateTemp(dstDir, "s6cmd-")
	if err != nil {
		return err
	}
	tempPath := to.Name()
	// os.CreateTemp creates the file with 0600; widen to the usual 0644.
	err = to.Chmod(0o644)
	if err == nil {
		_, err = io.Copy(to, from)
	}
	if closeErr := to.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tempPath, dst)
	}
	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return nil
}

// RunTasks runs tasks on up to jobs worker goroutines. With jobs <= 1 the
// tasks run sequentially and the first error stops the run. With jobs > 1
// every task runs to completion; all errors are collected under a mutex
// and returned as a single errors.Join error, so a failed transfer can
// never be dropped (the previous implementation raced an errCh receive
// against a done channel and could exit 0 after a failure).
func RunTasks(jobs int, tasks []func() error) error {
	if len(tasks) == 0 {
		return nil
	}
	if jobs <= 1 {
		for _, task := range tasks {
			if err := task(); err != nil {
				return err
			}
		}
		return nil
	}

	taskCh := make(chan func() error)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if task == nil {
					continue
				}
				if err := task(); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}

	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()

	return errors.Join(errs...)
}
