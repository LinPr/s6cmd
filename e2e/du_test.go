package e2e

import (
	"strings"
	"testing"
)

// TestE2E_DuBucket verifies that `s6cmd du s3://bucket` reports the total
// size and object count for the bucket.
func TestE2E_DuBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "12345")
	putObject(t, client, bucket, "b.txt", "67890")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "du", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd du failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Two objects, 5 bytes each = 10 bytes total. The exact format may
	// differ but should mention "10 bytes" and "2 objects".
	if !strings.Contains(res.Stdout, "10 bytes") {
		t.Errorf("stdout = %q, want it to contain '10 bytes'", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "2 objects") {
		t.Errorf("stdout = %q, want it to contain '2 objects'", res.Stdout)
	}
}

// TestE2E_DuPrefix verifies that `s6cmd du s3://bucket/prefix/` only
// counts objects under the prefix.
func TestE2E_DuPrefix(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "aaa")
	putObject(t, client, bucket, "logs/b.log", "bb")
	putObject(t, client, bucket, "other.txt", "ccccc")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "du", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd du failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// logs/a.log (3) + logs/b.log (2) = 5 bytes, 2 objects
	if !strings.Contains(res.Stdout, "5 bytes") {
		t.Errorf("stdout = %q, want it to contain '5 bytes'", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "2 objects") {
		t.Errorf("stdout = %q, want it to contain '2 objects'", res.Stdout)
	}
}

// TestE2E_DuHumanize verifies that --humanize/-H produces a human-readable
// size string.
func TestE2E_DuHumanize(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	// 2048 bytes -> "2.0K".
	big := strings.Repeat("x", 2048)
	putObject(t, client, bucket, "big.txt", big)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "du", "-H", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd du -H failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "2.0K") {
		t.Errorf("stdout = %q, want it to contain '2.0K'", res.Stdout)
	}
}

// TestE2E_DuGroup verifies that --group/-g produces a per-storage-class
// breakdown. gofakes3 reports STANDARD for all objects.
func TestE2E_DuGroup(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "abc")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "du", "-g", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd du -g failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "STANDARD") {
		t.Errorf("stdout = %q, want it to contain 'STANDARD'", res.Stdout)
	}
}
