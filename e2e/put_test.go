package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runS6cmdStdin runs the s6cmd binary with the given stdin and args. It
// routes through runS6cmdRawStdin (like runS6cmdWithStdin in
// pipe_test.go) so the hardened e2e environment applies; the helper used
// to build its own minimal env with HOME=/tmp — a world-writable
// directory on the config search path.
func runS6cmdStdin(t *testing.T, workdir, endpoint, stdin string, args ...string) s6cmdResult {
	t.Helper()
	full := append([]string{"--endpoint-url", endpoint, "--path-style"}, args...)
	return runS6cmdRawStdin(t, workdir, stdin, full)
}

// TestE2E_PutLocalFile verifies that `s6cmd put <local> s3://bucket/key`
// uploads a single local file.
func TestE2E_PutLocalFile(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "hello.txt")
	content := "put-content"
	writeFile(t, src, content)

	res := runS6cmd(t, workdir, endpoint, "put", src, "s3://"+bucket+"/hello.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd put failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "hello.txt"); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}

// TestE2E_PutStdin verifies that `s6cmd put - s3://bucket/key` uploads
// stdin to the object.
func TestE2E_PutStdin(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	content := "stdin-put-content\n"
	res := runS6cmdStdin(t, workdir, endpoint, content, "put", "-", "s3://"+bucket+"/stdin.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd put - failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	got := objectContent(t, client, bucket, "stdin.txt")
	if got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}

// TestE2E_PutGetMultipartFlags verifies that put/get accept the shared
// --part-size/--concurrency multipart-tuning flags and that a file larger
// than the part size round-trips intact through the forced multipart path
// (6 MiB with 5 MiB parts => 2-part upload, ranged 2-part download).
func TestE2E_PutGetMultipartFlags(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	content := strings.Repeat("0123456789abcdef", 6*1024*1024/16) // 6 MiB
	src := filepath.Join(workdir, "big.bin")
	if err := os.WriteFile(src, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	res := runS6cmd(t, workdir, endpoint, "put", "--part-size", "5", "--concurrency", "2", src, "s3://"+bucket+"/big.bin")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd put --part-size failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "big.bin"); got != content {
		t.Errorf("uploaded content mismatch: len=%d want %d", len(got), len(content))
	}

	dst := filepath.Join(workdir, "big.down")
	res = runS6cmd(t, workdir, endpoint, "get", "--part-size", "5", "--concurrency", "2", "s3://"+bucket+"/big.bin", dst)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd get --part-size failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != content {
		t.Errorf("downloaded content mismatch: len=%d want %d", len(got), len(content))
	}
}
