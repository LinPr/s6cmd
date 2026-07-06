package e2e

import (
	"strings"
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

// TestE2E_RmDryRun verifies that `s6cmd rm --dry-run --recursive` prints
// one line per object that would be deleted and deletes nothing.
func TestE2E_RmDryRun(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.txt", "a")
	putObject(t, client, bucket, "logs/b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "--dry-run", "--recursive", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm --dry-run failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	for _, key := range []string{"logs/a.txt", "logs/b.txt"} {
		if !strings.Contains(res.Stdout, key) {
			t.Errorf("stdout = %q, want it to list %s", res.Stdout, key)
		}
		if !objectExists(t, client, bucket, key) {
			t.Errorf("dry-run should not have deleted %s", key)
		}
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

// TestE2E_RmAllVersionsSingleKey verifies that `s6cmd rm --all-versions
// s3://bucket/key` purges every version of the key AND its delete marker,
// even when the latest entry is the marker (a plain Stat would 404).
func TestE2E_RmAllVersionsSingleKey(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)
	enableVersioning(t, client, bucket)

	putObject(t, client, bucket, "file.txt", "v1")
	putObject(t, client, bucket, "file.txt", "v2")
	deleteObject(t, client, bucket, "file.txt") // records a delete marker
	// A sibling key sharing the prefix must be left untouched.
	putObject(t, client, bucket, "file.txt.bak", "keep")

	versions, markers := listVersionEntries(t, client, bucket, "file.txt")
	if len(versions) < 2 || len(markers) != 1 {
		t.Fatalf("setup: want >=2 versions and 1 marker, got %d/%d", len(versions), len(markers))
	}

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "--all-versions", "s3://"+bucket+"/file.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm --all-versions failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}

	versions, markers = listVersionEntries(t, client, bucket, "file.txt")
	for _, k := range versions {
		if k == "file.txt" {
			t.Errorf("version of file.txt should have been purged")
		}
	}
	for _, k := range markers {
		if k == "file.txt" {
			t.Errorf("delete marker of file.txt should have been purged")
		}
	}
	if !objectExists(t, client, bucket, "file.txt.bak") {
		t.Errorf("sibling file.txt.bak should still exist")
	}
}

// TestE2E_RmAllVersionsPrefixRecursive verifies that `s6cmd rm --recursive
// --all-versions s3://bucket/prefix/` purges versions and delete markers of
// every key under the prefix — with the correct per-object keys, not the
// prefix string — and leaves other keys untouched.
func TestE2E_RmAllVersionsPrefixRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)
	enableVersioning(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "1")
	putObject(t, client, bucket, "logs/a.log", "2")
	putObject(t, client, bucket, "logs/sub/b.log", "3")
	deleteObject(t, client, bucket, "logs/a.log") // records a delete marker
	putObject(t, client, bucket, "other.txt", "keep")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "--recursive", "--all-versions", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm --recursive --all-versions failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}

	versions, markers := listVersionEntries(t, client, bucket, "logs/")
	if len(versions) != 0 || len(markers) != 0 {
		t.Errorf("logs/ should be fully purged, got versions=%v markers=%v", versions, markers)
	}
	if !objectExists(t, client, bucket, "other.txt") {
		t.Errorf("other.txt should still exist")
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

// TestE2E_RmListingFailureExitCode verifies that a failed listing surfaces
// in the exit code: expanding a wildcard against a bucket that does not
// exist used to log the error but still exit 0 (the listing errors were
// not included in the aggregated errors). A wildcard that simply matches
// nothing must keep exiting 0, though — "no object found" is a warning,
// not a failure.
func TestE2E_RmListingFailureExitCode(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "rm", "s3://"+bucket+"-nonexistent/*.log")
	if res.ExitCode == 0 {
		t.Errorf("rm with a failing listing (missing bucket) should exit non-zero, got 0: %q", res.Stdout)
	}

	// No-match wildcard on an existing bucket: warning, exit 0.
	putObject(t, client, bucket, "keep.txt", "keep")
	res = runS6cmd(t, workdir, endpoint, "rm", "s3://"+bucket+"/nomatch-*.log")
	if res.ExitCode != 0 {
		t.Errorf("rm with a no-match wildcard should exit 0, got %d\nstderr: %s", res.ExitCode, res.Stderr)
	}
	if !objectExists(t, client, bucket, "keep.txt") {
		t.Errorf("keep.txt should still exist")
	}
}
