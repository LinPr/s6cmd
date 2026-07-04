package e2e

import (
	"strings"
	"testing"
)

// TestE2E_CatSingleObject verifies that `s6cmd cat s3://bucket/key` prints
// the object body to stdout.
func TestE2E_CatSingleObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	content := "line one\nline two\n"
	putObject(t, client, bucket, "file.txt", content)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "cat", "s3://"+bucket+"/file.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cat failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := strings.TrimRight(res.Stdout, "\n"); got != strings.TrimRight(content, "\n") {
		t.Errorf("stdout = %q, want %q", res.Stdout, content)
	}
}

// TestE2E_CatMissingObject verifies that `s6cmd cat` on a non-existent
// object fails with a non-zero exit code.
func TestE2E_CatMissingObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "cat", "s3://"+bucket+"/missing.txt")
	if res.ExitCode == 0 {
		t.Fatalf("s6cmd cat on missing object should fail, got exit 0: %q", res.Stdout)
	}
}
