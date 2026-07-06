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

	// s6cmd sync: when the source is a directory, the destination prefix
	// is treated as the parent and the source's base name becomes the
	// first path segment. So syncing <srcDir> to s3://bucket/ produces
	// keys src/a.txt, src/b.txt.
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

// TestE2E_SyncDeleteRequiresYes verifies that sync --delete without --yes
// fails loudly on non-interactive stdin (instead of silently skipping the
// deletes and exiting 0, which made scripts believe the sync ran) and
// leaves both source and destination untouched.
func TestE2E_SyncDeleteRequiresYes(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "keep.txt"), "keep")
	putObject(t, client, bucket, "stale.txt", "stale")

	res := runS6cmd(t, workdir, endpoint, "sync", "--delete", srcDir, "s3://"+bucket+"/")
	if res.ExitCode == 0 {
		t.Fatalf("sync --delete without --yes on non-interactive stdin should fail, got exit 0: %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "--yes") {
		t.Errorf("stderr = %q, want a hint about --yes", res.Stderr)
	}
	if !objectExists(t, client, bucket, "stale.txt") {
		t.Errorf("refused sync --delete must not delete anything")
	}
}

// TestE2E_SyncDeleteDryRun verifies that --dry-run skips the confirmation
// prompt (nothing will be deleted), prints the plan, and mutates nothing on
// either side.
func TestE2E_SyncDeleteDryRun(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "new.txt"), "new")
	putObject(t, client, bucket, "stale.txt", "stale")

	res := runS6cmd(t, workdir, endpoint, "sync", "--dry-run", "--delete", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("sync --dry-run --delete failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "new.txt") {
		t.Errorf("stdout = %q, want the planned upload of new.txt", res.Stdout)
	}
	// The delete phase must be part of the printed plan too: a dry run
	// that silently skipped the delete phase would otherwise pass.
	if !strings.Contains(res.Stdout, "rm s3://"+bucket+"/stale.txt") {
		t.Errorf("stdout = %q, want the planned delete of stale.txt", res.Stdout)
	}
	if objectExists(t, client, bucket, "src/new.txt") {
		t.Errorf("dry-run should not have uploaded new.txt")
	}
	if !objectExists(t, client, bucket, "stale.txt") {
		t.Errorf("dry-run should not have deleted stale.txt")
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

// TestE2E_SyncSingleFileDeleteDifferentBasename is the regression test for
// the sync --delete data loss: syncing a single file to a destination key
// with a DIFFERENT basename used to copy the file and then delete the
// destination it had just written (the plan compared Base() names, so the
// destination key was classified as only-destination). The destination
// must exist with the new content after the sync, and sibling keys must
// not be touched (the sync targets a single key, not the parent prefix).
func TestE2E_SyncSingleFileDeleteDifferentBasename(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := putLocalFile(t, workdir, "newsrc.txt", "brand-new-content")
	putObject(t, client, bucket, "target-root.txt", "old-destination-content")
	putObject(t, client, bucket, "sibling.txt", "sibling")

	// Run the sync multiple times: the bug raced the copy and the delete
	// on the same key, so repeat runs sometimes deleted first.
	for i := 0; i < 3; i++ {
		res := runS6cmd(t, workdir, endpoint, "sync", "--delete", "--yes", src, "s3://"+bucket+"/target-root.txt")
		if res.ExitCode != 0 {
			t.Fatalf("run %d: sync --delete single-file failed: %s\nstderr: %s", i, res.Stdout, res.Stderr)
		}
		// Every performed delete logs an "rm <url>" line. The destination
		// key of the copy must never be enqueued for deletion — checking
		// the log is deterministic even when the racing copy happens to
		// land after the delete and recreates the object.
		if strings.Contains(res.Stdout, "rm s3://"+bucket+"/target-root.txt") {
			t.Fatalf("run %d: sync deleted its own destination key:\n%s", i, res.Stdout)
		}
		if !objectExists(t, client, bucket, "target-root.txt") {
			t.Fatalf("run %d: target-root.txt was deleted by the sync that wrote it", i)
		}
		if got := objectContent(t, client, bucket, "target-root.txt"); got != "brand-new-content" {
			t.Fatalf("run %d: target-root.txt = %q, want %q", i, got, "brand-new-content")
		}
		if !objectExists(t, client, bucket, "sibling.txt") {
			t.Fatalf("run %d: sibling.txt was deleted; a single-key sync must not delete siblings", i)
		}
	}
}

// TestE2E_SyncSingleFileDeleteSubPrefixDest covers the sub-prefix wrinkle
// of the same bug: for a destination key under a sub-prefix
// (s3://bucket/dir/k.txt) the old parent listing with a delimiter only
// returned a CommonPrefix, so the existing destination was never seen. The
// destination must be found (and overwritten in place), never deleted.
func TestE2E_SyncSingleFileDeleteSubPrefixDest(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := putLocalFile(t, workdir, "newsrc.txt", "sub-prefix-new-content")
	putObject(t, client, bucket, "dir/k.txt", "old")
	putObject(t, client, bucket, "dir/other.txt", "other")

	res := runS6cmd(t, workdir, endpoint, "sync", "--delete", "--yes", src, "s3://"+bucket+"/dir/k.txt")
	if res.ExitCode != 0 {
		t.Fatalf("sync --delete to sub-prefix key failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if strings.Contains(res.Stdout, "rm s3://"+bucket+"/dir/") {
		t.Fatalf("sync deleted under the destination sub-prefix:\n%s", res.Stdout)
	}
	if got := objectContent(t, client, bucket, "dir/k.txt"); got != "sub-prefix-new-content" {
		t.Errorf("dir/k.txt = %q, want %q", got, "sub-prefix-new-content")
	}
	if !objectExists(t, client, bucket, "dir/other.txt") {
		t.Errorf("dir/other.txt was deleted; a single-key sync must not delete siblings")
	}
}

// TestE2E_SyncEmptyDestPrefixExitOnError verifies that a first-time sync
// into a destination prefix that has no objects yet is treated as an empty
// listing, not an error: it used to log "ERROR no object found" on every
// first-time sync, and with --exit-on-error it exited 1 having copied
// nothing.
func TestE2E_SyncEmptyDestPrefixExitOnError(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "a-content")
	writeFile(t, filepath.Join(srcDir, "b.txt"), "b-content")

	res := runS6cmd(t, workdir, endpoint, "sync", "--exit-on-error", srcDir+"/", "s3://"+bucket+"/newprefix/")
	if res.ExitCode != 0 {
		t.Fatalf("first-time sync --exit-on-error to empty prefix failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if strings.Contains(res.Stderr, "no object found") {
		t.Errorf("stderr = %q, want no 'no object found' error for an empty destination", res.Stderr)
	}
	if got := objectContent(t, client, bucket, "newprefix/a.txt"); got != "a-content" {
		t.Errorf("newprefix/a.txt = %q, want %q", got, "a-content")
	}
	if got := objectContent(t, client, bucket, "newprefix/b.txt"); got != "b-content" {
		t.Errorf("newprefix/b.txt = %q, want %q", got, "b-content")
	}
}

// TestE2E_SyncExitOnErrorStopsOnListingFailure verifies that a source
// listing failure with --exit-on-error aborts before anything is
// submitted: the exit code is non-zero and nothing is uploaded.
func TestE2E_SyncExitOnErrorStopsOnListingFailure(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not block access")
	}
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "a-content")
	writeFile(t, filepath.Join(srcDir, "locked", "hidden.txt"), "hidden")
	lockedDir := filepath.Join(srcDir, "locked")
	if err := os.Chmod(lockedDir, 0o000); err != nil {
		t.Fatalf("Chmod(%q): %v", lockedDir, err)
	}
	t.Cleanup(func() { _ = os.Chmod(lockedDir, 0o755) })

	res := runS6cmd(t, workdir, endpoint, "sync", "--exit-on-error", srcDir+"/", "s3://"+bucket+"/")
	if res.ExitCode == 0 {
		t.Fatalf("sync --exit-on-error with unreadable source subdir should fail, got exit 0: %q", res.Stdout)
	}
	if objectExists(t, client, bucket, "a.txt") {
		t.Errorf("a.txt was uploaded; --exit-on-error on a listing failure must stop before submissions")
	}
}

// TestE2E_SyncExitOnErrorMidBatchTaskFailure verifies that a mid-batch
// transfer failure with --exit-on-error makes the command exit non-zero
// (in-flight tasks may still finish, but the run must be reported failed).
func TestE2E_SyncExitOnErrorMidBatchTaskFailure(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not block access")
	}
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	unreadable := filepath.Join(srcDir, "aaa-unreadable.txt")
	writeFile(t, unreadable, "cannot-read-me")
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatalf("Chmod(%q): %v", unreadable, err)
	}
	writeFile(t, filepath.Join(srcDir, "zzz-good.txt"), "good")

	res := runS6cmd(t, workdir, endpoint, "sync", "--exit-on-error", srcDir+"/", "s3://"+bucket+"/")
	if res.ExitCode == 0 {
		t.Fatalf("sync --exit-on-error with an unreadable source file should exit non-zero: %q", res.Stdout)
	}
	if objectExists(t, client, bucket, "aaa-unreadable.txt") {
		t.Errorf("aaa-unreadable.txt should not have been uploaded")
	}
}
