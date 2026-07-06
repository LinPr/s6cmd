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

// TestE2E_CustomEndpointDefaultsToPathStyle pins the addressing policy for
// a custom endpoint WITHOUT --path-style: s6cmd must default to path-style
// (the s5cmd/mc behaviour). The gofakes3 endpoint host cannot serve
// virtual-host requests (a "bucket.127.0.0.1" hostname never resolves), so
// this test empirically fails if the default regresses to virtual-host —
// which is exactly the MinIO-style breakage the policy exists to prevent.
func TestE2E_CustomEndpointDefaultsToPathStyle(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "auto-style.txt", "hello")

	workdir := t.TempDir()
	// Note: no --path-style. Only --endpoint-url is passed.
	lsRes := runS6cmdRaw(t, workdir, []string{"--endpoint-url", endpoint, "ls", "s3://" + bucket})
	if lsRes.ExitCode != 0 {
		t.Fatalf("ls with a custom endpoint and no --path-style must default to path-style, got exit %d\nstderr: %s", lsRes.ExitCode, lsRes.Stderr)
	}
	if !strings.Contains(lsRes.Stdout, "auto-style.txt") {
		t.Errorf("ls stdout = %q, want it to contain auto-style.txt", lsRes.Stdout)
	}

	dst := filepath.Join(workdir, "downloaded.txt")
	cpRes := runS6cmdRaw(t, workdir, []string{"--endpoint-url", endpoint, "cp", "s3://" + bucket + "/auto-style.txt", dst})
	if cpRes.ExitCode != 0 {
		t.Fatalf("cp with a custom endpoint and no --path-style failed: %s\nstderr: %s", cpRes.Stdout, cpRes.Stderr)
	}
	if got := fileContent(t, dst); got != "hello" {
		t.Errorf("downloaded content = %q, want %q", got, "hello")
	}
}
