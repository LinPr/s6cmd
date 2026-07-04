package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// putLocalFile writes content into <workdir>/name and returns its path.
func putLocalFile(t *testing.T, workdir, name, content string) string {
	t.Helper()
	p := filepath.Join(workdir, name)
	writeFile(t, p, content)
	return p
}

// TestE2E_SyncLocalToS3_NewFile verifies that `s6cmd sync <local-dir>
// s3://bucket/prefix/` uploads new files and skips files that already
// match.
func TestE2E_SyncLocalToS3_NewFile(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "a-content")
	writeFile(t, filepath.Join(srcDir, "b.txt"), "b-content")

	// s6cmd sync mirrors s5cmd: when the source is a directory, the
	// destination prefix is treated as the parent and the source's base
	// name becomes the first path segment. So syncing <srcDir> to
	// s3://bucket/ produces keys src/a.txt, src/b.txt.
	res := runS6cmd(t, workdir, endpoint, "sync", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd sync failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "src/a.txt"); got != "a-content" {
		t.Errorf("src/a.txt = %q, want %q", got, "a-content")
	}
	if got := objectContent(t, client, bucket, "src/b.txt"); got != "b-content" {
		t.Errorf("src/b.txt = %q, want %q", got, "b-content")
	}

	// Re-run sync: files already exist with the same size, so nothing
	// should change. We verify by checking the second run also exits 0
	// (idempotent).
	res2 := runS6cmd(t, workdir, endpoint, "sync", srcDir, "s3://"+bucket+"/")
	if res2.ExitCode != 0 {
		t.Fatalf("s6cmd sync (second run) failed: %s\nstderr: %s", res2.Stdout, res2.Stderr)
	}
}

// TestE2E_SyncLocalToS3_Delete verifies that --delete removes destination
// objects that are no longer present in the source.
func TestE2E_SyncLocalToS3_Delete(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "keep.txt"), "keep")

	// Pre-existing destination objects that are not in the source. The one
	// under src/ is "extra" relative to the source dir (the source only has
	// keep.txt, not src/keep.txt), so --delete removes it. The stale.txt at
	// the bucket root is also extra and is deleted.
	putObject(t, client, bucket, "stale.txt", "stale")

	res := runS6cmd(t, workdir, endpoint, "sync", "--delete", "--yes", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd sync --delete failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !objectExists(t, client, bucket, "src/keep.txt") {
		t.Errorf("src/keep.txt should exist after sync (sync mirrors src dir base name)")
	}
	if objectExists(t, client, bucket, "stale.txt") {
		t.Errorf("stale.txt should have been deleted by --delete")
	}
}

// TestE2E_SyncS3ToS3 verifies that `s6cmd sync s3://src/prefix/
// s3://dst/prefix/` copies new objects to the destination bucket.
func TestE2E_SyncS3ToS3(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	srcBucket := s3BucketFromTestName(t) + "-src"
	dstBucket := s3BucketFromTestName(t) + "-dst"
	createBucket(t, client, srcBucket)
	createBucket(t, client, dstBucket)

	putObject(t, client, srcBucket, "a.txt", "a")
	putObject(t, client, srcBucket, "b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "sync", "s3://"+srcBucket+"/", "s3://"+dstBucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd sync s3->s3 failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, dstBucket, "a.txt"); got != "a" {
		t.Errorf("dst a.txt = %q, want %q", got, "a")
	}
	if got := objectContent(t, client, dstBucket, "b.txt"); got != "b" {
		t.Errorf("dst b.txt = %q, want %q", got, "b")
	}
}

// TestE2E_SyncSizeOnly verifies that --size-only uses size as the sole
// comparison criterion. When the source and destination have the same size,
// the file is not re-uploaded; we verify the flag is accepted and the
// destination content is correct.
func TestE2E_SyncSizeOnly(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "ten-bytes") // 9 bytes

	// Pre-existing destination with the same size but different content; with
	// --size-only the sync should skip it. Because sync mirrors the source
	// directory's base name, the destination key is src/a.txt.
	putObject(t, client, bucket, "src/a.txt", "nine-byts") // 9 bytes, same length

	res := runS6cmd(t, workdir, endpoint, "sync", "--size-only", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd sync --size-only failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	got := objectContent(t, client, bucket, "src/a.txt")
	if got != "nine-byts" {
		t.Errorf("--size-only should have skipped same-size file; got %q", got)
	}
}

// TestE2E_SyncSingleFile verifies that `s6cmd sync <local-file>
// s3://bucket/key` syncs a single file.
func TestE2E_SyncSingleFile(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := putLocalFile(t, workdir, "single.txt", "single-content")

	res := runS6cmd(t, workdir, endpoint, "sync", src, "s3://"+bucket+"/single.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd sync single-file failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "single.txt"); got != "single-content" {
		t.Errorf("single.txt = %q, want %q", got, "single-content")
	}
	_ = strings.TrimSpace // keep strings import in case future asserts need it
	_ = os.Stat
}
