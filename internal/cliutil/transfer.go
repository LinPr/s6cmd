package cliutil

import (
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

func CopyLocalFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()
	to, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer to.Close()
	_, err = io.Copy(to, from)
	return err
}

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
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if task == nil {
					continue
				}
				if err := task(); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}

	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		return err
	case <-done:
		return nil
	}
}
