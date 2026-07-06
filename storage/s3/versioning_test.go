package s3store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinPr/s6cmd/storage"
)

// =========================================================================
// listObjectVersions tests
// =========================================================================

// versionsListingXML is a canned ListObjectVersions response with two object
// versions and one delete marker for the key "logs/a.log". The prefix in the
// request ("logs/") differs from the key so the test can catch the bug where
// the emitted URL kept the prefix as its Path.
const versionsListingXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult>
  <Name>bucket</Name>
  <Prefix>logs/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Version>
    <Key>logs/a.log</Key>
    <VersionId>v1</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2024-01-01T00:00:00Z</LastModified>
    <ETag>&quot;etag1&quot;</ETag>
    <Size>3</Size>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <Version>
    <Key>logs/a.log</Key>
    <VersionId>v2</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2024-01-02T00:00:00Z</LastModified>
    <ETag>&quot;etag2&quot;</ETag>
    <Size>4</Size>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <DeleteMarker>
    <Key>logs/a.log</Key>
    <VersionId>v3</VersionId>
    <IsLatest>true</IsLatest>
    <LastModified>2024-01-03T00:00:00Z</LastModified>
  </DeleteMarker>
</ListVersionsResult>`

// TestListObjectVersions_KeysAndMarkers verifies that a version listing
// emits one object per version AND per delete marker, that the emitted
// URL's Path is the listed key (not the request prefix), and that markers
// are flagged IsDeleteMarker.
func TestListObjectVersions_KeysAndMarkers(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("versions") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, versionsListingXML)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 0)

	u, err := storage.NewStorageURL("s3://bucket/logs/", storage.WithAllVersions(true))
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, false))
	if len(got) != 3 {
		t.Fatalf("List versions: want 3 objects (2 versions + 1 marker), got %d (%+v)", len(got), got)
	}
	var markers int
	seen := map[string]bool{}
	for _, o := range got {
		if o.Err != nil {
			t.Fatalf("List versions: unexpected error: %v", o.Err)
		}
		if o.StorageURL.Path != "logs/a.log" {
			t.Errorf("Path: want %q, got %q", "logs/a.log", o.StorageURL.Path)
		}
		if o.StorageURL.VersionID == "" {
			t.Errorf("VersionID empty for %+v", o)
		}
		seen[o.StorageURL.VersionID] = true
		if o.IsDeleteMarker {
			markers++
			if o.StorageURL.VersionID != "v3" {
				t.Errorf("delete marker VersionID: want %q, got %q", "v3", o.StorageURL.VersionID)
			}
		}
	}
	if markers != 1 {
		t.Errorf("want 1 delete marker, got %d", markers)
	}
	for _, v := range []string{"v1", "v2", "v3"} {
		if !seen[v] {
			t.Errorf("missing version %q in listing (got %v)", v, seen)
		}
	}
}

// =========================================================================
// calculateChunks tests
// =========================================================================

// TestCalculateChunks_FlushesOnBucketChange verifies that a chunk never
// spans buckets: a bucket change in the URL stream flushes the current
// chunk so keys are deleted against the bucket they belong to.
func TestCalculateChunks_FlushesOnBucketChange(t *testing.T) {
	t.Parallel()

	mk := func(bucket, key string) *storage.StorageURL {
		u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
		if err != nil {
			t.Fatalf("NewStorageURL: %v", err)
		}
		return u
	}
	urls := make(chan *storage.StorageURL, 4)
	urls <- mk("b1", "k1")
	urls <- mk("b1", "k2")
	urls <- mk("b2", "k3")
	urls <- mk("b1", "k4")
	close(urls)

	var chunks []chunk
	for c := range calculateChunks(urls) {
		chunks = append(chunks, c)
	}
	if len(chunks) != 3 {
		t.Fatalf("want 3 chunks, got %d (%+v)", len(chunks), chunks)
	}
	wantBuckets := []string{"b1", "b2", "b1"}
	wantSizes := []int{2, 1, 1}
	for i, c := range chunks {
		if c.Bucket != wantBuckets[i] {
			t.Errorf("chunk %d bucket: want %q, got %q", i, wantBuckets[i], c.Bucket)
		}
		if len(c.Keys) != wantSizes[i] {
			t.Errorf("chunk %d size: want %d, got %d", i, wantSizes[i], len(c.Keys))
		}
	}
}

// =========================================================================
// MultiDelete error-path tests
// =========================================================================

// TestMultiDelete_ChunkErrorContinues verifies that a DeleteObjects request
// error for one chunk emits an error result and processing continues with
// the remaining chunks (previously the consumer returned, leaving the
// producer goroutines blocked and later chunks undeleted).
func TestMultiDelete_ChunkErrorContinues(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			if strings.HasPrefix(r.URL.Path, "/bad-bucket") {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, s3ErrorXML("AccessDenied", "simulated failure"))
				return
			}
			// Quiet response: no <Deleted> entries at all.
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 0)

	urls := make(chan *storage.StorageURL, 4)
	for _, s := range []string{
		"s3://bad-bucket/k1",
		"s3://bad-bucket/k2",
		"s3://good-bucket/k3",
		"s3://good-bucket/k4",
	} {
		u, err := storage.NewStorageURL(s)
		if err != nil {
			t.Fatalf("NewStorageURL: %v", err)
		}
		urls <- u
	}
	close(urls)

	var errCount, okCount int
	var okKeys []string
	for o := range store.MultiDelete(context.Background(), urls) {
		if o.Err != nil {
			errCount++
			continue
		}
		okCount++
		okKeys = append(okKeys, o.StorageURL.Path)
	}
	if errCount != 1 {
		t.Errorf("want 1 chunk error, got %d", errCount)
	}
	// The good-bucket chunk must still be processed, and its successes must
	// be reported even though the (quiet) response carried no <Deleted>
	// entries.
	if okCount != 2 {
		t.Errorf("want 2 successes from the good bucket, got %d (%v)", okCount, okKeys)
	}
}

// TestMultiDelete_PerKeyErrors verifies that per-key <Error> entries in the
// DeleteObjects response are surfaced as error results and excluded from the
// derived success set.
func TestMultiDelete_PerKeyErrors(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
  <Error><Key>k1</Key><Code>AccessDenied</Code><Message>denied</Message></Error>
</DeleteResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 0)

	urls := make(chan *storage.StorageURL, 2)
	for _, s := range []string{"s3://bucket/k1", "s3://bucket/k2"} {
		u, err := storage.NewStorageURL(s)
		if err != nil {
			t.Fatalf("NewStorageURL: %v", err)
		}
		urls <- u
	}
	close(urls)

	var failed, ok []string
	for o := range store.MultiDelete(context.Background(), urls) {
		if o.Err != nil {
			failed = append(failed, o.StorageURL.Path)
			if !strings.Contains(o.Err.Error(), "AccessDenied") {
				t.Errorf("error should carry the S3 code, got %v", o.Err)
			}
			continue
		}
		ok = append(ok, o.StorageURL.Path)
	}
	if len(failed) != 1 || failed[0] != "k1" {
		t.Errorf("failed keys: want [k1], got %v", failed)
	}
	if len(ok) != 1 || ok[0] != "k2" {
		t.Errorf("ok keys: want [k2], got %v", ok)
	}
}

// =========================================================================
// Delete version forwarding
// =========================================================================

// TestDelete_VersionID verifies that Delete forwards the URL's VersionID as
// the versionId query parameter (without it, deleting a specific version
// silently creates a delete marker instead).
func TestDelete_VersionID(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "del-version", "obj.txt"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, []byte("x"), nil)

	u, err := storage.NewStorageURL("s3://"+bucket+"/"+key, storage.WithVersion("v123"))
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Delete(context.Background(), u); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	found := false
	for _, r := range backend.requests {
		if strings.HasPrefix(r, "DELETE /"+bucket+"/"+key) {
			found = true
			if !strings.Contains(r, "versionId=v123") {
				t.Errorf("DELETE request should carry versionId=v123, got %q", r)
			}
		}
	}
	if !found {
		t.Fatalf("no DELETE request recorded (requests: %v)", backend.requests)
	}
}

// =========================================================================
// Presign version forwarding
// =========================================================================

// TestPresign_VersionID verifies that Presign bakes the URL's VersionID into
// the presigned GET request.
func TestPresign_VersionID(t *testing.T) {
	// isolateAWSEnv uses t.Setenv, which cannot be used with t.Parallel;
	// see TestPresign for why the presigner needs real (fake) credentials
	// and why AWS_SDK_LOAD_CONFIG must not be relied on (SDK v2 ignores it).
	isolateAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "presign-test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "presign-test-secret")

	srv, backend := newMockS3Server(t)
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:     srv.URL,
		UsePathStyle: true,
		Region:       "us-east-1",
		MaxRetries:   1,
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	const bucket, key = "presign-ver", "file.txt"
	backend.makeBucket(t, bucket)

	u, err := storage.NewStorageURL("s3://"+bucket+"/"+key, storage.WithVersion("ver-42"))
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	presigned, err := store.Presign(context.Background(), u, 5*time.Minute)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	parsed, err := url.Parse(presigned)
	if err != nil {
		t.Fatalf("parse presigned: %v", err)
	}
	if got := parsed.Query().Get("versionId"); got != "ver-42" {
		t.Errorf("presigned URL versionId: want %q, got %q", "ver-42", got)
	}
}

// =========================================================================
// legacy DeleteObjects tests
// =========================================================================

// TestDeleteObjects_ChunksBatches verifies the legacy stringly-typed
// DeleteObjects splits key sets bigger than the S3 limit (1000) into
// multiple requests instead of sending one oversized request.
func TestDeleteObjects_ChunksBatches(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "legacy-del"
	backend.makeBucket(t, bucket)

	keys := make([]string, 1050)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj-%04d", i)
	}
	if err := store.DeleteObjects(context.Background(), bucket, keys); err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	var batchCalls int
	for _, r := range backend.requests {
		if strings.HasPrefix(r, "POST /"+bucket+"?") && strings.Contains(r, "delete") {
			batchCalls++
		}
	}
	if batchCalls != 2 {
		t.Fatalf("want 2 DeleteObjects requests (1000 + 50 keys), got %d", batchCalls)
	}
}

// TestDeleteObjects_JoinsBatchErrors verifies that a request-level failure
// on a later batch does not drop the per-key errors already accumulated
// from earlier batches: both must be visible in the returned error so
// callers (rb --force) can report every failed key.
func TestDeleteObjects_JoinsBatchErrors(t *testing.T) {
	t.Parallel()

	// Standalone handler: batch 1 returns a per-key <Error> entry, batch 2
	// fails at the request level with AccessDenied (a non-retryable code so
	// the test stays deterministic with MaxRetries=1).
	var batch int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		n := atomic.AddInt32(&batch, 1)
		w.Header().Set("Content-Type", "application/xml")
		if n == 1 {
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult><Error><Key>bad-key-1</Key><Code>InternalError</Code><Message>per-key failure</Message></Error></DeleteResult>`)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>request-level failure</Message></Error>`)
	})
	srvErr := httptest.NewServer(mux)
	defer srvErr.Close()

	store := newS3Store(t, srvErr)

	// 1001 keys force two batches (deleteObjectsMax = 1000).
	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj-%04d", i)
	}
	err := store.DeleteObjects(context.Background(), "join-bucket", keys)
	if err == nil {
		t.Fatal("expected DeleteObjects to fail")
	}
	if !strings.Contains(err.Error(), "bad-key-1") {
		t.Errorf("error should carry the per-key failure from batch 1, got %v", err)
	}
	if !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("error should carry the request-level failure from batch 2, got %v", err)
	}
}

// TestDeleteObjects_SendsContentMD5 pins the withContentMD5 option: several
// S3-compatible services (Aliyun OSS, older MinIO) reject DeleteObjects
// requests carrying the SDK's default x-amz-checksum-crc32 header with
// MissingArgument and require the legacy Content-MD5 instead. Verified
// against real Aliyun OSS (oss-cn-hangzhou) in 2026-07.
func TestDeleteObjects_SendsContentMD5(t *testing.T) {
	t.Parallel()

	var gotMD5, gotCRC32, gotAlgo string
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotMD5 = r.Header.Get("Content-Md5")
		gotCRC32 = r.Header.Get("X-Amz-Checksum-Crc32")
		gotAlgo = r.Header.Get("X-Amz-Sdk-Checksum-Algorithm")
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store := newS3Store(t, srv)
	if err := store.DeleteObjects(context.Background(), "md5-bucket", []string{"a.txt"}); err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if gotMD5 == "" {
		t.Error("DeleteObjects request must carry a Content-MD5 header for OSS/MinIO compatibility")
	}
	if gotCRC32 != "" || gotAlgo != "" {
		t.Errorf("flexible checksum headers must be replaced by Content-MD5, got crc32=%q algo=%q", gotCRC32, gotAlgo)
	}
}

// TestDeleteObjects_DryRun verifies the legacy DeleteObjects honours the
// store's dry-run mode like every other mutating operation.
func TestDeleteObjects_DryRun(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv, func(o *S3Option) { o.DryRun = true })

	const bucket, key = "dryrun-del", "keep.txt"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, []byte("x"), nil)

	if err := store.DeleteObjects(context.Background(), bucket, []string{key}); err != nil {
		t.Fatalf("DeleteObjects dry-run: %v", err)
	}
	if _, ok := backend.objects[bucket][key]; !ok {
		t.Fatalf("dry-run DeleteObjects must not delete the object")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, r := range backend.requests {
		if strings.HasPrefix(r, "POST /"+bucket+"?") && strings.Contains(r, "delete") {
			t.Fatalf("dry-run DeleteObjects must not issue requests, got %q", r)
		}
	}
}

// =========================================================================
// ListObjects (V1) pagination
// =========================================================================

// TestListObjectsV1_PaginatesWithoutDelimiter verifies the manual V1
// pagination: when Delimiter is unset S3 omits NextMarker, so the store must
// fall back to the last Contents key as the next Marker instead of silently
// truncating after the first page.
func TestListObjectsV1_PaginatesWithoutDelimiter(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv, func(o *S3Option) { o.UseListObjectsV1 = true })

	const bucket = "list-v1"
	backend.makeBucket(t, bucket)
	keys := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for _, k := range keys {
		backend.putTestObject(t, bucket, k, []byte("x"), nil)
	}
	// Force 2-key pages so the listing needs 3 requests.
	backend.mu.Lock()
	backend.listMaxKeys = 2
	backend.mu.Unlock()

	// The wildcard form clears the delimiter, which is the case where S3
	// omits NextMarker.
	u, err := storage.NewStorageURL("s3://" + bucket + "/*")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, false))
	if len(got) != len(keys) {
		t.Fatalf("V1 List: want %d objects, got %d", len(keys), len(got))
	}
	seen := map[string]bool{}
	for _, o := range got {
		if o.Err != nil {
			t.Fatalf("V1 List: unexpected error: %v", o.Err)
		}
		seen[o.StorageURL.Path] = true
	}
	for _, k := range keys {
		if !seen[k] {
			t.Errorf("V1 List: missing key %q", k)
		}
	}
}

// TestListObjectsV1_NonAdvancingMarkerAborts verifies the infinite-loop
// guard in the manual V1 pagination: a server that keeps reporting
// IsTruncated=true while re-emitting the same page (so the marker derived
// from the last Contents key never advances) must produce an error object
// and terminate the listing instead of looping forever.
func TestListObjectsV1_NonAdvancingMarkerAborts(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv, func(o *S3Option) { o.UseListObjectsV1 = true })

	const bucket = "list-v1-stuck"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, "a.txt", []byte("x"), nil)
	backend.putTestObject(t, bucket, "b.txt", []byte("x"), nil)
	backend.mu.Lock()
	backend.listV1NonAdvancing = true
	backend.mu.Unlock()

	u, err := storage.NewStorageURL("s3://" + bucket + "/*")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}

	// The channel must terminate (i.e. the range must complete); a
	// regression here would hang the test until the suite timeout.
	var errs []error
	n := 0
	for o := range store.List(context.Background(), u, false) {
		if o.Err != nil {
			errs = append(errs, o.Err)
			continue
		}
		n++
		if n > 10 {
			t.Fatalf("listing did not stop on a non-advancing marker (already emitted %d objects)", n)
		}
	}
	if len(errs) == 0 {
		t.Fatalf("expected an error object for the non-advancing marker, got none (objects: %d)", n)
	}
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "non-advancing marker") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a non-advancing marker error, got %v", errs)
	}
}

// TestCopy_KeyWithSpaces verifies a server-side copy of a key containing
// spaces resolves the right source object. Before EscapedPath switched to
// url.PathEscape, spaces became "+" and the copy hit the wrong key.
func TestCopy_KeyWithSpaces(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "copy-spaces"
	backend.makeBucket(t, bucket)
	content := []byte("copy me")
	backend.putTestObject(t, bucket, "dir/a b.txt", content, nil)

	src, err := storage.NewStorageURL("s3://" + bucket + "/dir/a b.txt")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	dst, err := storage.NewStorageURL("s3://" + bucket + "/dst.txt")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Copy(context.Background(), src, dst, storage.Metadata{}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	got, ok := backend.objects[bucket]["dst.txt"]
	if !ok {
		t.Fatalf("Copy: dst not present")
	}
	if string(got) != string(content) {
		t.Fatalf("Copy content: want %q, got %q", content, got)
	}
}
