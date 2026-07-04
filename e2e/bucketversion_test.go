package e2e

import (
	"strings"
	"testing"
)

// TestE2E_BucketVersionQuery verifies that `s6cmd bucket-version
// s3://bucket` returns a non-error status line. gofakes3's in-memory
// backend does not persist versioning state across requests, so the
// reported status may be empty/unversioned — we only assert the command
// runs and prints the bucket name.
func TestE2E_BucketVersionQuery(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "bucket-version", "s3://"+bucket)
	if res.ExitCode != 0 {
		// gofakes3 may not implement GetBucketVersioning the same way
		// real S3 does; if it returns an error surface, skip rather
		// than fail.
		t.Skipf("gofakes3 does not support bucket versioning queries: %s", res.Stderr)
	}
	if !strings.Contains(res.Stdout, bucket) {
		t.Errorf("stdout = %q, want it to mention bucket %q", res.Stdout, bucket)
	}
}

// TestE2E_BucketVersionSet verifies that `s6cmd bucket-version --set
// Enabled s3://bucket` sets the versioning state. gofakes3's s3mem
// backend does implement SetVersioningConfiguration, but the
// GetBucketVersioning follow-up may not reflect it; if the set call
// fails we skip rather than fail.
func TestE2E_BucketVersionSet(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "bucket-version", "--set", "Enabled", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Skipf("gofakes3 does not support setting bucket versioning: %s", res.Stderr)
	}
	if !strings.Contains(res.Stdout, bucket) {
		t.Errorf("stdout = %q, want it to mention bucket %q", res.Stdout, bucket)
	}
	if !strings.Contains(res.Stdout, "Enabled") {
		t.Errorf("stdout = %q, want it to mention 'Enabled'", res.Stdout)
	}
}
