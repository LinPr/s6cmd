package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// TestE2E_MvDryRun verifies that --dry-run/-n runs the plan phase (one
// log line per would-be operation) without uploading anything and without
// removing the local source.
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
	// The plan phase logs the operation that would run.
	if !strings.Contains(res.Stdout, "dry.txt") || !strings.Contains(res.Stdout, "mv") {
		t.Errorf("stdout = %q, want a per-operation mv line mentioning dry.txt", res.Stdout)
	}
	if objectExists(t, client, bucket, "dry.txt") {
		t.Errorf("dry-run should not have uploaded the object")
	}
	if _, err := os.Stat(src); err != nil {
		t.Errorf("dry-run should not have removed the local source, stat err=%v", err)
	}
}

// TestE2E_MvWildcard verifies that `s6cmd mv ./*.log s3://bucket/` uploads
// every matching file and deletes exactly those files — the literal glob
// string used to be passed to os.Remove, which always failed and left the
// sources behind. Non-matching files must be untouched.
func TestE2E_MvWildcard(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	writeFile(t, filepath.Join(workdir, "a.log"), "1")
	writeFile(t, filepath.Join(workdir, "b.log"), "2")
	writeFile(t, filepath.Join(workdir, "keep.txt"), "3")

	res := runS6cmd(t, workdir, endpoint, "mv", filepath.Join(workdir, "*.log"), "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv wildcard failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "a.log"); got != "1" {
		t.Errorf("a.log = %q, want %q", got, "1")
	}
	if got := objectContent(t, client, bucket, "b.log"); got != "2" {
		t.Errorf("b.log = %q, want %q", got, "2")
	}
	for _, name := range []string{"a.log", "b.log"} {
		if _, err := os.Stat(filepath.Join(workdir, name)); !os.IsNotExist(err) {
			t.Errorf("source %s should have been removed, stat err=%v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "keep.txt")); err != nil {
		t.Errorf("keep.txt should be untouched, stat err=%v", err)
	}
	if objectExists(t, client, bucket, "keep.txt") {
		t.Errorf("keep.txt should not have been uploaded")
	}
}

// TestE2E_MvMetadataAndExclude verifies that mv routes its transfers
// through the shared cp plumbing: --metadata reaches the uploaded object
// and --exclude'd files are neither uploaded NOR deleted (mv used to
// advertise these flags but ignore them entirely).
func TestE2E_MvMetadataAndExclude(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	writeFile(t, filepath.Join(srcDir, "data.txt"), "data")
	writeFile(t, filepath.Join(srcDir, "skip.tmp"), "tmp")

	res := runS6cmd(t, workdir, endpoint, "mv", "--recursive",
		"--metadata", "team=infra", "--exclude", "*.tmp",
		srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd mv --metadata --exclude failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "data.txt"); got != "data" {
		t.Errorf("data.txt = %q, want %q", got, "data")
	}
	head, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("data.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := head.Metadata["team"]; got != "infra" {
		t.Errorf("metadata team = %q, want %q (metadata: %v)", got, "infra", head.Metadata)
	}
	// The excluded file must not be uploaded and — critically — must not
	// be deleted from the source either.
	if objectExists(t, client, bucket, "skip.tmp") {
		t.Errorf("excluded skip.tmp should not have been uploaded")
	}
	if got := fileContent(t, filepath.Join(srcDir, "skip.tmp")); got != "tmp" {
		t.Errorf("excluded skip.tmp must stay in place, got %q", got)
	}
	// data.txt was moved, so it is gone from the source.
	if _, err := os.Stat(filepath.Join(srcDir, "data.txt")); !os.IsNotExist(err) {
		t.Errorf("moved data.txt should have been removed, stat err=%v", err)
	}
}

// TestE2E_MvPrefixRequiresRecursive verifies that moving a bucket/prefix
// source without --recursive is rejected (mv also deletes the source, so
// the guard matters even more than for cp).
func TestE2E_MvPrefixRequiresRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.txt", "a")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "mv", "s3://"+bucket+"/logs/", "s3://"+bucket+"/moved/")
	if res.ExitCode == 0 {
		t.Fatalf("mv of a prefix without --recursive should fail, got exit 0: %q", res.Stdout)
	}
	if !objectExists(t, client, bucket, "logs/a.txt") {
		t.Errorf("source object must be untouched after the rejected mv")
	}
}
