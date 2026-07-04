package e2e

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// runS6cmdWithStdin runs the s6cmd binary with the given stdin and args.
func runS6cmdWithStdin(t *testing.T, workdir, endpoint, stdin string, args ...string) s6cmdResult {
	t.Helper()
	full := append([]string{"--endpoint-url", endpoint, "--path-style"}, args...)
	cmd := exec.Command(s6cmdPath, full...)
	cmd.Dir = workdir
	cmd.Stdin = strings.NewReader(stdin)
	env := append([]string{},
		"AWS_ACCESS_KEY_ID="+defaultAccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+defaultSecretAccessKey,
		"AWS_REGION="+defaultRegion,
		"HOME=/tmp",
		"PATH=/usr/bin:/bin",
	)
	cmd.Env = env
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
