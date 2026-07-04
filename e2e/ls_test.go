package e2e

import (
	"strings"
	"testing"
)

// TestE2E_LsBucket verifies that `s6cmd ls s3://bucket` lists objects in a
// bucket. We only assert that the key appears somewhere in the output.
func TestE2E_LsBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")
	putObject(t, client, bucket, "b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "a.txt") {
		t.Errorf("stdout = %q, want it to contain a.txt", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "b.txt") {
		t.Errorf("stdout = %q, want it to contain b.txt", res.Stdout)
	}
}

// TestE2E_LsPrefix verifies that `s6cmd ls s3://bucket/prefix/` only lists
// objects under the prefix.
func TestE2E_LsPrefix(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "a")
	putObject(t, client, bucket, "logs/b.log", "b")
	putObject(t, client, bucket, "other.txt", "c")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "a.log") || !strings.Contains(res.Stdout, "b.log") {
		t.Errorf("stdout = %q, want a.log and b.log", res.Stdout)
	}
	if strings.Contains(res.Stdout, "other.txt") {
		t.Errorf("stdout = %q, should not contain other.txt", res.Stdout)
	}
}

// TestE2E_LsRecursive verifies that --recursive flattens the listing.
func TestE2E_LsRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "dir/sub/a.txt", "a")
	putObject(t, client, bucket, "dir/b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "--recursive", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Both nested and shallow keys must appear with --recursive.
	if !strings.Contains(res.Stdout, "dir/sub/a.txt") {
		t.Errorf("stdout = %q, want dir/sub/a.txt", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "dir/b.txt") {
		t.Errorf("stdout = %q, want dir/b.txt", res.Stdout)
	}
}
