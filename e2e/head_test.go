package e2e

import (
	"strings"
	"testing"
)

// TestE2E_HeadObject verifies that `s6cmd head s3://bucket/key` prints a
// single JSON line with the object metadata, including the key, size and
// etag fields.
func TestE2E_HeadObject(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "hello-head")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "head", "s3://"+bucket+"/a.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd head failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Output is a single JSON line.
	out := strings.TrimSpace(res.Stdout)
	if !strings.HasPrefix(out, "{") || !strings.HasSuffix(out, "}") {
		t.Fatalf("stdout = %q, want a JSON object", out)
	}
	for _, want := range []string{
		`"key"`,
		`"size"`,
		`"etag"`,
		`"content_type"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %s", out, want)
		}
	}
	if !strings.Contains(out, "/a.txt") {
		t.Errorf("stdout = %q, want it to mention /a.txt", out)
	}
}

// TestE2E_HeadBucket verifies that `s6cmd head s3://bucket` prints a JSON
// object with the bucket field.
func TestE2E_HeadBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "head", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd head bucket failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	out := strings.TrimSpace(res.Stdout)
	if !strings.HasPrefix(out, "{") || !strings.HasSuffix(out, "}") {
		t.Fatalf("stdout = %q, want a JSON object", out)
	}
	if !strings.Contains(out, `"bucket"`) {
		t.Errorf("stdout = %q, want it to contain 'bucket' field", out)
	}
}
