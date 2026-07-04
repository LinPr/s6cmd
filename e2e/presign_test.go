package e2e

import (
	"strings"
	"testing"
)

// TestE2E_Presign verifies that `s6cmd presign s3://bucket/key` prints a
// presigned URL containing the bucket/key and the X-Amz-Algorithm query
// parameter.
func TestE2E_Presign(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "presign-me")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "presign", "s3://"+bucket+"/a.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd presign failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	url := strings.TrimSpace(res.Stdout)
	if !strings.Contains(url, bucket) {
		t.Errorf("presigned URL %q does not contain bucket %q", url, bucket)
	}
	if !strings.Contains(url, "a.txt") {
		t.Errorf("presigned URL %q does not contain key 'a.txt'", url)
	}
	if !strings.Contains(url, "X-Amz-Algorithm") {
		t.Errorf("presigned URL %q does not contain X-Amz-Algorithm", url)
	}
}
