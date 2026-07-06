package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_CopyLocalToS3 uploads a local file to a fresh bucket via
// `s6cmd cp <local> s3://bucket/key`, then verifies the object exists and
// its content matches the source file.
func TestE2E_CopyLocalToS3(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "hello.txt")
	content := "hello s6cmd"
	writeFile(t, src, content)

	res := runS6cmd(t, workdir, endpoint, "cp", src, "s3://"+bucket+"/hello.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !objectExists(t, client, bucket, "hello.txt") {
		t.Fatalf("object not found after cp")
	}
	if got := objectContent(t, client, bucket, "hello.txt"); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}

// TestE2E_CopyS3ToLocal downloads a single object from a fresh bucket via
// `s6cmd cp s3://bucket/key <local>`, then verifies the local file exists
// and its content matches the object.
func TestE2E_CopyS3ToLocal(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	content := "from-s3-content"
	putObject(t, client, bucket, "file.txt", content)

	workdir := t.TempDir()
	dst := filepath.Join(workdir, "out.txt")
	res := runS6cmd(t, workdir, endpoint, "cp", "s3://"+bucket+"/file.txt", dst)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := fileContent(t, dst); got != content {
		t.Errorf("local file content = %q, want %q", got, content)
	}
}

// TestE2E_CopyS3ToS3 performs a server-side copy from one bucket to another
// via `s6cmd cp s3://src/key s3://dst/key`, then verifies the destination
// object exists with the same content.
func TestE2E_CopyS3ToS3(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)

	srcBucket := s3BucketFromTestName(t) + "-src"
	dstBucket := s3BucketFromTestName(t) + "-dst"
	createBucket(t, client, srcBucket)
	createBucket(t, client, dstBucket)

	content := "server-side-copy"
	putObject(t, client, srcBucket, "a.txt", content)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "cp",
		"s3://"+srcBucket+"/a.txt",
		"s3://"+dstBucket+"/b.txt",
	)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !objectExists(t, client, dstBucket, "b.txt") {
		t.Fatalf("destination object not found")
	}
	if got := objectContent(t, client, dstBucket, "b.txt"); got != content {
		t.Errorf("dst content = %q, want %q", got, content)
	}
}

// TestE2E_CopyDryRun verifies that --dry-run/-n runs the plan phase (one
// log line per would-be operation) without uploading anything, and that
// the deprecated --dryRun spelling still parses.
func TestE2E_CopyDryRun(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "dry.txt")
	writeFile(t, src, "content")

	res := runS6cmd(t, workdir, endpoint, "cp", "-n", src, "s3://"+bucket+"/dry.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp -n failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// The plan phase logs the operation that would run.
	if !strings.Contains(res.Stdout, "dry.txt") || !strings.Contains(res.Stdout, "cp") {
		t.Errorf("stdout = %q, want a per-operation cp line mentioning dry.txt", res.Stdout)
	}
	if objectExists(t, client, bucket, "dry.txt") {
		t.Errorf("dry-run should not have uploaded the object")
	}

	// The deprecated camelCase spelling is a hidden alias.
	res2 := runS6cmd(t, workdir, endpoint, "cp", "--dryRun", src, "s3://"+bucket+"/dry.txt")
	if res2.ExitCode != 0 {
		t.Fatalf("s6cmd cp --dryRun (deprecated alias) failed: %s\nstderr: %s", res2.Stdout, res2.Stderr)
	}
	if objectExists(t, client, bucket, "dry.txt") {
		t.Errorf("deprecated --dryRun alias should not have uploaded the object")
	}
}

// TestE2E_CopyPrefixRequiresRecursive verifies that copying a bucket/prefix
// source without --recursive is rejected, and that --recursive copies the
// whole prefix (including nested keys) preserving relative paths.
func TestE2E_CopyPrefixRequiresRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.txt", "a")
	putObject(t, client, bucket, "logs/nested/b.txt", "b")

	workdir := t.TempDir()
	dstDir := filepath.Join(workdir, "out")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	res := runS6cmd(t, workdir, endpoint, "cp", "s3://"+bucket+"/logs/", dstDir+"/")
	if res.ExitCode == 0 {
		t.Fatalf("cp of a prefix without --recursive should fail, got exit 0: %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "--recursive") {
		t.Errorf("stderr = %q, want a hint about --recursive", res.Stderr)
	}

	res2 := runS6cmd(t, workdir, endpoint, "cp", "--recursive", "s3://"+bucket+"/logs/", dstDir+"/")
	if res2.ExitCode != 0 {
		t.Fatalf("cp --recursive failed: %s\nstderr: %s", res2.Stdout, res2.Stderr)
	}
	if got := fileContent(t, filepath.Join(dstDir, "a.txt")); got != "a" {
		t.Errorf("a.txt = %q, want %q", got, "a")
	}
	if got := fileContent(t, filepath.Join(dstDir, "nested", "b.txt")); got != "b" {
		t.Errorf("nested/b.txt = %q, want %q", got, "b")
	}
}
