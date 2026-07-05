package e2e

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_PathStyle verifies that `s6cmd --path-style cp/ls` works
// against a gofakes3 server using explicit path-style addressing. This is
// the default mode used by every other e2e test; this test is the
// canonical regression check for path-style.
func TestE2E_PathStyle(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "path-style.txt", "hello")

	workdir := t.TempDir()
	// ls should work.
	lsRes := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket)
	if lsRes.ExitCode != 0 {
		t.Fatalf("ls with --path-style failed: %s\nstderr: %s", lsRes.Stdout, lsRes.Stderr)
	}
	if !strings.Contains(lsRes.Stdout, "path-style.txt") {
		t.Errorf("ls stdout = %q, want it to contain path-style.txt", lsRes.Stdout)
	}

	// cp should work.
	dst := filepath.Join(workdir, "downloaded.txt")
	cpRes := runS6cmd(t, workdir, endpoint, "cp", "s3://"+bucket+"/path-style.txt", dst)
	if cpRes.ExitCode != 0 {
		t.Fatalf("cp with --path-style failed: %s\nstderr: %s", cpRes.Stdout, cpRes.Stderr)
	}
	if got := fileContent(t, dst); got != "hello" {
		t.Errorf("downloaded content = %q, want %q", got, "hello")
	}
}
