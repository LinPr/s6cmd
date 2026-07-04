package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

// osMkdirAll is a tiny wrapper around os.MkdirAll so the test file does
// not need to import "os" directly for a single call. It is defined here
// so other e2e tests can reuse mkdirAll.
func osMkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

// TestE2E_GetSingleObject verifies that `s6cmd get s3://bucket/key <local>`
// downloads a single object.
func TestE2E_GetSingleObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	content := "from-s3-get"
	putObject(t, client, bucket, "a.txt", content)

	workdir := t.TempDir()
	dst := filepath.Join(workdir, "out.txt")
	res := runS6cmd(t, workdir, endpoint, "get", "s3://"+bucket+"/a.txt", dst)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd get failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := fileContent(t, dst); got != content {
		t.Errorf("local file content = %q, want %q", got, content)
	}
}

// TestE2E_GetRecursive verifies that `s6cmd get --recursive s3://bucket/
// <local-dir>` downloads every object under the bucket into the local
// directory, preserving the key structure.
func TestE2E_GetRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")
	putObject(t, client, bucket, "sub/b.txt", "b")

	workdir := t.TempDir()
	dstDir := filepath.Join(workdir, "out")
	// get requires the destination directory to exist when source is a
	// prefix/wildcard, so create it before running.
	mkdirAll(t, dstDir)
	res := runS6cmd(t, workdir, endpoint, "get", "--recursive", "s3://"+bucket, dstDir)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd get --recursive failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := fileContent(t, filepath.Join(dstDir, "a.txt")); got != "a" {
		t.Errorf("a.txt = %q, want %q", got, "a")
	}
	if got := fileContent(t, filepath.Join(dstDir, "sub", "b.txt")); got != "b" {
		t.Errorf("sub/b.txt = %q, want %q", got, "b")
	}
}

// mkdirAll creates a directory, failing the test on error.
func mkdirAll(t *testing.T, dir string) {
	t.Helper()
	if err := osMkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll(%q): %v", dir, err)
	}
}
