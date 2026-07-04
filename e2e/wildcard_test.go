package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_CpWildcardS3ToLocal verifies that `s6cmd cp s3://bucket/*.log
// <local-dir>` downloads every matching object to the local directory.
func TestE2E_CpWildcardS3ToLocal(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.log", "1")
	putObject(t, client, bucket, "b.log", "2")
	putObject(t, client, bucket, "c.txt", "3")

	workdir := t.TempDir()
	dstDir := filepath.Join(workdir, "out")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	res := runS6cmd(t, workdir, endpoint, "cp", "s3://"+bucket+"/*.log", dstDir+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp wildcard failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := fileContent(t, filepath.Join(dstDir, "a.log")); got != "1" {
		t.Errorf("a.log = %q, want %q", got, "1")
	}
	if got := fileContent(t, filepath.Join(dstDir, "b.log")); got != "2" {
		t.Errorf("b.log = %q, want %q", got, "2")
	}
	if _, err := os.Stat(filepath.Join(dstDir, "c.txt")); err == nil {
		t.Errorf("c.txt should not have been downloaded")
	}
}

// TestE2E_CpConcurrencyHigh verifies that `s6cmd cp --concurrency=N`
// uploads several files concurrently without dropping any. The flag is
// accepted and every file arrives.
func TestE2E_CpConcurrencyHigh(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	srcDir := filepath.Join(workdir, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		writeFile(t, filepath.Join(srcDir, string(rune('a'+i))+".txt"), "content-"+string(rune('a'+i)))
	}

	res := runS6cmd(t, workdir, endpoint, "cp", "--recursive", "--concurrency", "8", srcDir, "s3://"+bucket+"/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd cp --concurrency failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// cp mirrors the source directory's base name into the destination
	// prefix, so the keys are src/<name>.txt rather than <name>.txt.
	for i := 0; i < n; i++ {
		key := "src/" + string(rune('a'+i)) + ".txt"
		if !objectExists(t, client, bucket, key) {
			t.Errorf("object %q should exist", key)
		}
	}
}

// keep strings import for future asserts.
var _ = strings.Contains
