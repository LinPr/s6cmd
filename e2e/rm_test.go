package e2e

import (
	"testing"
)

// TestE2E_RmSingleObject verifies that `s6cmd rm s3://bucket/key` deletes a
// single object.
func TestE2E_RmSingleObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "file.txt", "content")
	if !objectExists(t, client, bucket, "file.txt") {
		t.Fatalf("setup: object should exist")
	}

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "s3://"+bucket+"/file.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if objectExists(t, client, bucket, "file.txt") {
		t.Errorf("object should have been deleted")
	}
}

// TestE2E_RmWildcard verifies that `s6cmd rm s3://bucket/*.log` deletes all
// matching objects and leaves non-matching objects untouched.
func TestE2E_RmWildcard(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.log", "1")
	putObject(t, client, bucket, "b.log", "2")
	putObject(t, client, bucket, "c.txt", "3")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "s3://"+bucket+"/*.log")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if objectExists(t, client, bucket, "a.log") {
		t.Errorf("a.log should have been deleted")
	}
	if objectExists(t, client, bucket, "b.log") {
		t.Errorf("b.log should have been deleted")
	}
	if !objectExists(t, client, bucket, "c.txt") {
		t.Errorf("c.txt should still exist")
	}
}

// TestE2E_RmPrefixRecursive verifies that `s6cmd rm --recursive s3://bucket/prefix/`
// deletes every object under the prefix.
func TestE2E_RmPrefixRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "1")
	putObject(t, client, bucket, "logs/sub/b.log", "2")
	putObject(t, client, bucket, "other.txt", "3")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "--recursive", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if objectExists(t, client, bucket, "logs/a.log") {
		t.Errorf("logs/a.log should have been deleted")
	}
	if objectExists(t, client, bucket, "logs/sub/b.log") {
		t.Errorf("logs/sub/b.log should have been deleted")
	}
	if !objectExists(t, client, bucket, "other.txt") {
		t.Errorf("other.txt should still exist")
	}
}
