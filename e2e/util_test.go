package e2e

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// defaultAccessKeyID/Secret are the static credentials we feed to s6cmd via
// env vars. gofakes3 does not validate them, but the AWS SDK requires them
// to be non-empty.
const (
	defaultAccessKeyID     = "s6cmd-test"
	defaultSecretAccessKey = "s6cmd-test-secret"
	defaultRegion          = "us-east-1"
)

// goBuildS6cmd compiles the s6cmd binary into a temp dir and sets
// s6cmdPath. It returns a cleanup function that removes the temp dir.
//
// The build uses -mod=vendor so it does not need network access.
func goBuildS6cmd() func() {
	tmpdir, err := os.MkdirTemp("", "s6cmd-e2e-")
	if err != nil {
		panic(err)
	}
	binary := "s6cmd"
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	s6cmdPath = filepath.Join(tmpdir, binary)

	// workdir is the project root (the parent of the e2e package dir).
	workdir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	workdir = filepath.Dir(workdir)

	cmd := exec.Command("go", "build", "-mod=vendor", "-o", s6cmdPath, ".")
	cmd.Dir = workdir
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		panic(fmt.Sprintf("failed to build s6cmd: %s", err))
	}
	if err := os.Chmod(s6cmdPath, 0o755); err != nil {
		panic(err)
	}
	return func() { os.RemoveAll(tmpdir) }
}

// s6cmdResult is the result of running the s6cmd binary.
type s6cmdResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// runS6cmd executes the s6cmd binary with the given args, pointing it at
// the given endpoint via --endpoint-url and --path-style. It returns the
// captured stdout, stderr, and exit code.
func runS6cmd(t *testing.T, workdir, endpoint string, args ...string) s6cmdResult {
	t.Helper()
	full := append([]string{"--endpoint-url", endpoint, "--path-style"}, args...)
	return runS6cmdRaw(t, workdir, full)
}

// runS6cmdRaw executes the s6cmd binary with the exact args provided,
// without injecting --endpoint-url or --path-style. Callers that need
// the standard e2e wiring should use runS6cmd; this is for cases that
// need to test auto/virtual addressing styles where the global flags
// must be controlled by the test itself.
func runS6cmdRaw(t *testing.T, workdir string, args []string) s6cmdResult {
	t.Helper()
	return runS6cmdRawStdin(t, workdir, "", args)
}

// runS6cmdRawStdin is runS6cmdRaw with the given string wired to the
// child's stdin. It is the single hardened execution path every e2e
// helper routes through: the stdin variants used to build their own
// environment with HOME=/tmp — a world-writable directory on the config
// search path, so anyone on the machine could plant /tmp/s6cmd.yaml and
// inject config into the tests.
func runS6cmdRawStdin(t *testing.T, workdir, stdin string, args []string) s6cmdResult {
	t.Helper()
	cmd := exec.Command(s6cmdPath, args...)
	cmd.Dir = workdir
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	// Inherit the parent env so things work in CI, but override the
	// credentials/region with our test values.
	env := os.Environ()
	env = append(env,
		"AWS_ACCESS_KEY_ID="+defaultAccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+defaultSecretAccessKey,
		"AWS_REGION="+defaultRegion,
		// Isolate the child from the developer machine: a real
		// ~/.aws/config (profiles, endpoint_url) or ambient AWS_ENDPOINT_*
		// env vars must never leak into an e2e run. Every test passes the
		// gofakes3 endpoint explicitly, so blanking these is safe — and it
		// guarantees that a flag-wiring regression fails against a dead
		// local default instead of sending requests to a real endpoint.
		"AWS_CONFIG_FILE="+os.DevNull,
		"AWS_SHARED_CREDENTIALS_FILE="+os.DevNull,
		"AWS_PROFILE=",
		"AWS_DEFAULT_PROFILE=",
		"AWS_ENDPOINT_URL=",
		"AWS_ENDPOINT_URL_S3=",
		"AWS_EC2_METADATA_DISABLED=true",
		// Point HOME at a private, empty temp dir so the config search
		// ($HOME/s6cmd.yaml) can never pick up a developer's real config
		// or a file planted in a world-writable directory.
		"HOME="+t.TempDir(),
		"S6CMD_CONFIG=",
	)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		// ExitError is expected when a command fails; surface its code.
		if exitErr, ok := err.(*exec.ExitError); ok {
			return s6cmdResult{stdout.String(), stderr.String(), exitErr.ExitCode()}
		}
		t.Fatalf("failed to run s6cmd: %v", err)
	}
	return s6cmdResult{stdout.String(), stderr.String(), 0}
}

// s3Client returns an S3 client configured against the given gofakes3
// endpoint, using static credentials and path-style addressing.
func s3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       defaultRegion,
		Credentials:  credentials.NewStaticCredentialsProvider(defaultAccessKeyID, defaultSecretAccessKey, ""),
		UsePathStyle: true,
	})
	return client
}

// createBucket creates a bucket and registers a cleanup that deletes it
// (and all objects in it) at the end of the test.
func createBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	_, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucket, err)
	}
	t.Cleanup(func() {
		// List and delete all objects, then delete the bucket.
		p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		})
		var objects []types.ObjectIdentifier
		for p.HasMorePages() {
			out, err := p.NextPage(t.Context())
			if err != nil {
				return
			}
			for _, o := range out.Contents {
				objects = append(objects, types.ObjectIdentifier{
					Key: o.Key,
				})
			}
		}
		if len(objects) > 0 {
			_, _ = client.DeleteObjects(t.Context(), &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{Objects: objects},
			})
		}
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
	})
}

// putObject uploads content to the given bucket/key.
func putObject(t *testing.T, client *s3.Client, bucket, key, content string) {
	t.Helper()
	_, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject(%q, %q): %v", bucket, key, err)
	}
}

// enableVersioning turns on versioning for the bucket.
func enableVersioning(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	_, err := client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	if err != nil {
		t.Fatalf("PutBucketVersioning(%q): %v", bucket, err)
	}
}

// deleteObject deletes a key without a version id; on a versioned bucket
// this records a delete marker.
func deleteObject(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject(%q, %q): %v", bucket, key, err)
	}
}

// listVersionEntries returns the keys of every object version and every
// delete marker under the given prefix.
func listVersionEntries(t *testing.T, client *s3.Client, bucket, prefix string) (versions, markers []string) {
	t.Helper()
	p := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		out, err := p.NextPage(t.Context())
		if err != nil {
			t.Fatalf("ListObjectVersions(%q, %q): %v", bucket, prefix, err)
		}
		for _, v := range out.Versions {
			versions = append(versions, aws.ToString(v.Key))
		}
		for _, m := range out.DeleteMarkers {
			markers = append(markers, aws.ToString(m.Key))
		}
	}
	return versions, markers
}

// bucketExists reports whether the bucket exists.
func bucketExists(t *testing.T, client *s3.Client, bucket string) bool {
	t.Helper()
	_, err := client.HeadBucket(t.Context(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	return err == nil
}

// objectExists reports whether the object exists in the bucket.
func objectExists(t *testing.T, client *s3.Client, bucket, key string) bool {
	t.Helper()
	_, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true
	}
	// S3 returns 404 wrapped in NotFound; treat any error as "not found"
	// for test purposes.
	return false
}

// objectContent downloads the object and returns its body as a string.
func objectContent(t *testing.T, client *s3.Client, bucket, key string) string {
	t.Helper()
	out, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject(%q, %q): %v", bucket, key, err)
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	return buf.String()
}

// fileContent reads a local file, failing the test on error.
func fileContent(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return string(b)
}

// writeFile writes content to a local file, creating parent dirs.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}
