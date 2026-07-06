package e2e

import (
	"strings"
	"testing"
)

// TestE2E_StatSummaryTable verifies that the root --stat flag prints an
// end-of-run summary table with per-operation counts, and that the table
// is absent without the flag.
func TestE2E_StatSummaryTable(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.txt", "a")
	putObject(t, client, bucket, "logs/b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "--stat", "rm", "--recursive", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd --stat rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	for _, want := range []string{"Operation", "Total", "Error", "Success", "rm"} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q from the --stat summary:\n%s", want, res.Stdout)
		}
	}

	// Without --stat no summary table is printed.
	putObject(t, client, bucket, "logs/c.txt", "c")
	res = runS6cmd(t, workdir, endpoint, "rm", "s3://"+bucket+"/logs/c.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if strings.Contains(res.Stdout, "Operation") {
		t.Errorf("summary table printed without --stat:\n%s", res.Stdout)
	}
}

// TestE2E_StatSummaryJSON verifies that with --output json the --stat
// summary is emitted as one JSON object per operation instead of a table.
func TestE2E_StatSummaryJSON(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "--stat", "--output", "json", "rm", "s3://"+bucket+"/a.txt")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd --stat -o json rm failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, `"operation":"rm"`) || !strings.Contains(res.Stdout, `"success":1`) {
		t.Errorf("stdout missing JSON stat summary:\n%s", res.Stdout)
	}
}
