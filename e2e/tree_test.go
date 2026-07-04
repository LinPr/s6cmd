package e2e

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_TreeLocal verifies that `s6cmd tree <local-dir>` prints a tree
// view of the local directory, including nested files.
func TestE2E_TreeLocal(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	workdir := t.TempDir()
	root := filepath.Join(workdir, "root")
	writeFile(t, filepath.Join(root, "a.txt"), "a")
	writeFile(t, filepath.Join(root, "sub", "b.txt"), "b")

	res := runS6cmd(t, workdir, endpoint, "tree", root)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd tree failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	for _, want := range []string{"a.txt", "sub", "b.txt"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout = %q, want it to contain %q", res.Stdout, want)
		}
	}
}

// TestE2E_TreeS3Prefix verifies that `s6cmd tree s3://bucket/prefix/`
// prints a tree view of the objects under the prefix.
func TestE2E_TreeS3Prefix(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "a")
	putObject(t, client, bucket, "logs/sub/b.log", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "tree", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd tree s3 failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	for _, want := range []string{"a.log", "sub", "b.log"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout = %q, want it to contain %q", res.Stdout, want)
		}
	}
}
