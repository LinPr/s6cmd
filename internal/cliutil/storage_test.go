package cliutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/LinPr/s6cmd/storage"
)

// isolateAWSEnv makes NewStorage/NewS3Client hermetic for the duration of a
// test: config.LoadDefaultConfig reads the process environment and the
// shared config/credentials files, so a developer's real ~/.aws/config
// (e.g. a default profile with endpoint_url set) or AWS_ENDPOINT_URL* env
// vars would otherwise leak into the resolved client options.
//
// t.Setenv is incompatible with t.Parallel, so tests calling this helper
// must not be parallel.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "nonexistent-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "nonexistent-credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_DEFAULT_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_SDK_LOAD_CONFIG", "0")
}

// TestNewStorageDryRun verifies the DryRun plumbing added to CommonFlags:
// when set, both stores built by NewStorage must no-op every mutating
// operation. The S3 side is asserted on the wire (a local HTTP server
// counts requests; a dry-run Put/Copy/Delete must send none), the local
// side on the filesystem (Delete must leave the file in place).
func TestNewStorageDryRun(t *testing.T) {
	isolateAWSEnv(t)

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx := context.Background()
	flags := CommonFlags{
		EndpointURL:   srv.URL,
		PathStyle:     true,
		NoSignRequest: true,
		Region:        "us-east-1",
		DryRun:        true,
	}
	store, err := NewStorage(ctx, flags)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	src, err := storage.NewStorageURL("s3://dryrun-bucket/src.txt")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	dst, err := storage.NewStorageURL("s3://dryrun-bucket/dst.txt")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}

	if err := store.Put(ctx, strings.NewReader("data"), dst, storage.Metadata{}, 1, 5*1024*1024); err != nil {
		t.Errorf("dry-run Put: %v", err)
	}
	if err := store.Copy(ctx, src, dst, storage.Metadata{}); err != nil {
		t.Errorf("dry-run Copy: %v", err)
	}
	if err := store.Delete(ctx, dst); err != nil {
		t.Errorf("dry-run Delete: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("dry-run S3 operations sent %d requests, want 0", got)
	}

	// Local store: Delete must be a no-op too.
	localFile := filepath.Join(t.TempDir(), "keep.txt")
	if err := os.WriteFile(localFile, []byte("keep"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	localURL, err := storage.NewStorageURL(localFile)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Delete(ctx, localURL); err != nil {
		t.Errorf("dry-run local Delete: %v", err)
	}
	if _, err := os.Stat(localFile); err != nil {
		t.Errorf("dry-run local Delete removed the file: %v", err)
	}

	// Control: without DryRun the same Delete must reach the server,
	// proving the flag (not some other short-circuit) suppressed the
	// requests above.
	flags.DryRun = false
	live, err := NewStorage(ctx, flags)
	if err != nil {
		t.Fatalf("NewStorage (live): %v", err)
	}
	_ = live.Delete(ctx, dst) // response body is fake; only the wire counts
	if got := requests.Load(); got == 0 {
		t.Error("control Delete without DryRun sent no request; the test server wiring is broken")
	}
}
