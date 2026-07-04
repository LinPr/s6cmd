package e2e

import (
	"strings"
	"testing"
)

// TestE2E_StatObject verifies that `s6cmd stat s3://bucket/key` prints
// the object metadata including ContentType, Size, ETag and Key.
func TestE2E_StatObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "hello-stat")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "stat", "s3://"+bucket+"/a.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd stat failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Required fields.
	for _, want := range []string{
		"Key:",
		"ContentLength:",
		"ETag:",
		"ContentType:",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout = %q, want it to contain %q", res.Stdout, want)
		}
	}
	// ContentLength should report 10 bytes.
	if !strings.Contains(res.Stdout, "10") {
		t.Errorf("stdout = %q, want it to contain '10' (ContentLength)", res.Stdout)
	}
}

// TestE2E_StatBucket verifies that `s6cmd stat s3://bucket` prints the
// bucket metadata line.
func TestE2E_StatBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "stat", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd stat bucket failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "Bucket:") {
		t.Errorf("stdout = %q, want it to contain 'Bucket:'", res.Stdout)
	}
	if !strings.Contains(res.Stdout, bucket) {
		t.Errorf("stdout = %q, want it to contain bucket name %q", res.Stdout, bucket)
	}
}
