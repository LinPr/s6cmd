package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_MvLocalToS3 verifies that `s6cmd mv <local> s3://bucket/key`
// uploads the file to S3 and removes the local source.
func TestE2E_MvLocalToS3(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "hello.txt")
	content := "hello mv"
	writeFile(t, src, content)

	res := runS6cmd(t, workdir, endpoint, "mv", src, "s3://"+bucket+"/hello.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !objectExists(t, client, bucket, "hello.txt") {
		t.Fatalf("object should exist after mv")
	}
	if got := objectContent(t, client, bucket, "hello.txt"); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("local source should have been removed, stat err=%v", err)
	}
}

// TestE2E_MvS3ToLocal verifies that `s6cmd mv s3://bucket/key <local>`
// downloads the object and removes the source object.
func TestE2E_MvS3ToLocal(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	content := "from-s3-mv"
	putObject(t, client, bucket, "file.txt", content)

	workdir := t.TempDir()
	dst := filepath.Join(workdir, "out.txt")
	res := runS6cmd(t, workdir, endpoint, "mv", "s3://"+bucket+"/file.txt", dst)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := fileContent(t, dst); got != content {
		t.Errorf("local file content = %q, want %q", got, content)
	}
	if objectExists(t, client, bucket, "file.txt") {
		t.Errorf("source object should have been removed")
	}
}

// TestE2E_MvS3ToS3 verifies that `s6cmd mv s3://src/key s3://dst/key`
// copies the object to the destination and removes the source.
func TestE2E_MvS3ToS3(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)

	srcBucket := s3BucketFromTestName(t) + "-src"
	dstBucket := s3BucketFromTestName(t) + "-dst"
	createBucket(t, client, srcBucket)
	createBucket(t, client, dstBucket)

	content := "server-side-mv"
	putObject(t, client, srcBucket, "a.txt", content)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "mv",
		"s3://"+srcBucket+"/a.txt",
		"s3://"+dstBucket+"/b.txt",
	)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !objectExists(t, client, dstBucket, "b.txt") {
		t.Fatalf("destination object should exist")
	}
	if got := objectContent(t, client, dstBucket, "b.txt"); got != content {
		t.Errorf("dst content = %q, want %q", got, content)
	}
	if objectExists(t, client, srcBucket, "a.txt") {
		t.Errorf("source object should have been removed")
	}
}

// TestE2E_MvRecursive verifies that `s6cmd mv --recursive <local-dir>
// s3://bucket/prefix/` uploads every file under the directory and removes
// the local source directory.
func TestE2E_MvRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "a")
	writeFile(t, filepath.Join(srcDir, "sub", "b.txt"), "b")

	res := runS6cmd(t, workdir, endpoint, "mv", "--recursive", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv --recursive failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "a.txt"); got != "a" {
		t.Errorf("a.txt content = %q, want %q", got, "a")
	}
	if got := objectContent(t, client, bucket, "sub/b.txt"); got != "b" {
		t.Errorf("sub/b.txt content = %q, want %q", got, "b")
	}
	if _, err := os.Stat(srcDir); !os.IsNotExist(err) {
		t.Errorf("source directory should have been removed, stat err=%v", err)
	}
}

// TestE2E_MvDryRun verifies that --dryRun/-n does not move anything and
// prints a DRYRUN line.
func TestE2E_MvDryRun(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "dry.txt")
	writeFile(t, src, "content")

	res := runS6cmd(t, workdir, endpoint, "mv", "-n", src, "s3://"+bucket+"/dry.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv -n failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "DRYRUN") {
		t.Errorf("stdout = %q, want it to contain DRYRUN", res.Stdout)
	}
	if objectExists(t, client, bucket, "dry.txt") {
		t.Errorf("dry-run should not have uploaded the object")
	}
	if _, err := os.Stat(src); err != nil {
		t.Errorf("dry-run should not have removed the local source, stat err=%v", err)
	}
}
