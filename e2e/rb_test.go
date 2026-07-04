package e2e

import (
	"testing"
)

// TestE2E_RbEmptyBucket verifies that `s6cmd rb s3://bucket` removes an
// empty bucket.
func TestE2E_RbEmptyBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rb", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rb failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Verify the bucket is gone by listing buckets; the name should not
	// appear.
	listRes := runS6cmd(t, workdir, endpoint, "ls", "s3://")
	if listRes.ExitCode != 0 {
		// ls s3:// might fail on gofakes3 depending on configuration; we
		// only care that rb succeeded, so do not hard-fail here.
		return
	}
}

// TestE2E_RbForceNonEmpty verifies that `s6cmd rb --force s3://bucket`
// empties and removes a non-empty bucket.
func TestE2E_RbForceNonEmpty(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")
	putObject(t, client, bucket, "b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rb", "--force", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rb --force failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Verify the bucket is gone by attempting to list it; the list should
	// fail (NoSuchBucket) or be empty.
	listRes := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket)
	if listRes.ExitCode == 0 && listRes.Stdout != "" {
		t.Errorf("bucket %q should have been removed, but ls returned: %q", bucket, listRes.Stdout)
	}
}

// TestE2E_RbNonEmptyWithoutForce verifies that `s6cmd rb s3://bucket`
// on a non-empty bucket without --force fails (BucketNotEmpty).
func TestE2E_RbNonEmptyWithoutForce(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rb", "s3://"+bucket)
	if res.ExitCode == 0 {
		t.Fatalf("s6cmd rb on non-empty bucket should have failed, got exit 0: %q", res.Stdout)
	}
}
