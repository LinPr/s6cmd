package e2e

import (
	"testing"
)

// runS6cmdWithStdin runs the s6cmd binary with the given stdin and args.
// It routes through runS6cmdRawStdin so the hardened e2e environment
// (blanked AWS_* endpoint/profile vars, /dev/null shared config, private
// HOME) applies; the helper used to build its own env with HOME=/tmp — a
// world-writable directory on the config search path.
func runS6cmdWithStdin(t *testing.T, workdir, endpoint, stdin string, args ...string) s6cmdResult {
	t.Helper()
	full := append([]string{"--endpoint-url", endpoint, "--path-style"}, args...)
	return runS6cmdRawStdin(t, workdir, stdin, full)
}

// TestE2E_Pipe verifies that `echo "data" | s6cmd pipe s3://bucket/key`
// uploads stdin to the object.
func TestE2E_Pipe(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	content := "piped-data\n"
	res := runS6cmdWithStdin(t, workdir, endpoint, content, "pipe", "s3://"+bucket+"/pipe.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd pipe failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	got := objectContent(t, client, bucket, "pipe.txt")
	if got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}
