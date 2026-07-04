package e2e

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runS6cmdWithStdinPath runs the s6cmd binary with the given stdin and
// args. It is similar to runS6cmdWithStdin in pipe_test.go but uses a
// minimal env so the pipe tests can share the same pattern.
func runS6cmdStdin(t *testing.T, workdir, endpoint, stdin string, args ...string) s6cmdResult {
	t.Helper()
	full := append([]string{"--endpoint-url", endpoint, "--path-style"}, args...)
	cmd := exec.Command(s6cmdPath, full...)
	cmd.Dir = workdir
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = []string{
		"AWS_ACCESS_KEY_ID=" + defaultAccessKeyID,
		"AWS_SECRET_ACCESS_KEY=" + defaultSecretAccessKey,
		"AWS_REGION=" + defaultRegion,
		"HOME=/tmp",
		"PATH=/usr/bin:/bin",
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return s6cmdResult{stdout.String(), stderr.String(), exitErr.ExitCode()}
		}
		t.Fatalf("failed to run s6cmd: %v", err)
	}
	return s6cmdResult{stdout.String(), stderr.String(), 0}
}

// TestE2E_PutLocalFile verifies that `s6cmd put <local> s3://bucket/key`
// uploads a single local file.
func TestE2E_PutLocalFile(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	src := filepath.Join(workdir, "hello.txt")
	content := "put-content"
	writeFile(t, src, content)

	res := runS6cmd(t, workdir, endpoint, "put", src, "s3://"+bucket+"/hello.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd put failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if got := objectContent(t, client, bucket, "hello.txt"); got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}

// TestE2E_PutStdin verifies that `s6cmd put - s3://bucket/key` uploads
// stdin to the object.
func TestE2E_PutStdin(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	workdir := t.TempDir()
	content := "stdin-put-content\n"
	res := runS6cmdStdin(t, workdir, endpoint, content, "put", "-", "s3://"+bucket+"/stdin.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd put - failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	got := objectContent(t, client, bucket, "stdin.txt")
	if got != content {
		t.Errorf("object content = %q, want %q", got, content)
	}
}
