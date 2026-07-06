package e2e

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestE2E_LsBucket verifies that `s6cmd ls s3://bucket` lists objects in a
// bucket. We only assert that the key appears somewhere in the output.
func TestE2E_LsBucket(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "a")
	putObject(t, client, bucket, "b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "a.txt") {
		t.Errorf("stdout = %q, want it to contain a.txt", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "b.txt") {
		t.Errorf("stdout = %q, want it to contain b.txt", res.Stdout)
	}
}

// TestE2E_LsPrefix verifies that `s6cmd ls s3://bucket/prefix/` only lists
// objects under the prefix.
func TestE2E_LsPrefix(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "logs/a.log", "a")
	putObject(t, client, bucket, "logs/b.log", "b")
	putObject(t, client, bucket, "other.txt", "c")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "s3://"+bucket+"/logs/")
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "a.log") || !strings.Contains(res.Stdout, "b.log") {
		t.Errorf("stdout = %q, want a.log and b.log", res.Stdout)
	}
	if strings.Contains(res.Stdout, "other.txt") {
		t.Errorf("stdout = %q, should not contain other.txt", res.Stdout)
	}
}

// TestE2E_LsAllVersions verifies that `ls --all-versions` lists every
// object version and delete marker with its version ID.
func TestE2E_LsAllVersions(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)
	enableVersioning(t, client, bucket)

	putObject(t, client, bucket, "v.txt", "one")
	putObject(t, client, bucket, "v.txt", "two")
	deleteObject(t, client, bucket, "v.txt") // records a delete marker

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "--all-versions", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls --all-versions failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}

	// Every version line must carry the version ID the fake assigned.
	out, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(out.Versions) != 2 || len(out.DeleteMarkers) != 1 {
		t.Fatalf("fixture: want 2 versions + 1 delete marker, got %d + %d",
			len(out.Versions), len(out.DeleteMarkers))
	}
	for _, v := range out.Versions {
		if id := aws.ToString(v.VersionId); !strings.Contains(res.Stdout, id) {
			t.Errorf("stdout = %q, want version id %q", res.Stdout, id)
		}
	}
	if got := strings.Count(res.Stdout, "v.txt"); got != 3 {
		t.Errorf("stdout = %q, want 3 v.txt entries (2 versions + 1 marker), got %d", res.Stdout, got)
	}
	if !strings.Contains(res.Stdout, "(delete-marker)") {
		t.Errorf("stdout = %q, want the delete marker to be flagged", res.Stdout)
	}
}

// TestE2E_LsOutputJSON verifies that --output json makes ls emit one JSON
// object per line.
func TestE2E_LsOutputJSON(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "a.txt", "abc")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "--output", "json", "ls", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls --output json failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}

	type lsLine struct {
		Key  string `json:"key"`
		Size int64  `json:"size"`
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if line == "" {
			continue
		}
		var l lsLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("line %q is not valid JSON: %v", line, err)
		}
		if l.Key == "s3://"+bucket+"/a.txt" {
			found = true
			if l.Size != 3 {
				t.Errorf("size = %d, want 3", l.Size)
			}
		}
	}
	if !found {
		t.Errorf("stdout = %q, want a JSON line with key s3://%s/a.txt", res.Stdout, bucket)
	}
}

// TestE2E_LsRecursive verifies that --recursive flattens the listing.
func TestE2E_LsRecursive(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "dir/sub/a.txt", "a")
	putObject(t, client, bucket, "dir/b.txt", "b")

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "ls", "--recursive", "s3://"+bucket)
	if res.ExitCode != 0 {
		t.Fatalf("s6cmd ls failed: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	// Both nested and shallow keys must appear with --recursive.
	if !strings.Contains(res.Stdout, "dir/sub/a.txt") {
		t.Errorf("stdout = %q, want dir/sub/a.txt", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "dir/b.txt") {
		t.Errorf("stdout = %q, want dir/b.txt", res.Stdout)
	}
}
