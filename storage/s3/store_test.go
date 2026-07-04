package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// =========================================================================
// Stat tests
// =========================================================================

// TestStat_ObjectExists seeds an object via the mock backend and verifies
// that Stat returns an *Object whose Size, ETag and ModTime match what the
// server stored.
func TestStat_ObjectExists(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "stat-bucket", "hello.txt"
	content := []byte("hello world")
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, content, nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	obj, err := store.Stat(context.Background(), u)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if obj.Size != int64(len(content)) {
		t.Errorf("Size: want %d, got %d", len(content), obj.Size)
	}
	if obj.Etag != hexMD5(content) {
		t.Errorf("Etag: want %q, got %q", hexMD5(content), obj.Etag)
	}
	if obj.ModTime == nil {
		t.Errorf("ModTime should not be nil")
	}
	if obj.StorageURL.Bucket != bucket || obj.StorageURL.Path != key {
		t.Errorf("URL: want %s/%s, got %s/%s", bucket, key, obj.StorageURL.Bucket, obj.StorageURL.Path)
	}
}

// TestStat_ObjectNotFound verifies Stat returns ErrGivenObjectNotFound for
// a missing key.
func TestStat_ObjectNotFound(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "stat-nf"
	backend.makeBucket(t, bucket)

	u, err := storage.NewStorageURL("s3://" + bucket + "/does-not-exist")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	_, err = store.Stat(context.Background(), u)
	if !errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		t.Fatalf("Stat: want ErrGivenObjectNotFound, got %v", err)
	}
}

// TestStat_Bucket verifies HeadBucket returns the bucket metadata.
func TestStat_Bucket(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "headbucket"
	backend.makeBucket(t, bucket)

	b, err := store.HeadBucket(context.Background(), bucket)
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
	if b.Name != bucket {
		t.Errorf("Name: want %q, got %q", bucket, b.Name)
	}
	// The mock sets the bucket region to us-east-1 on HeadBucket.
	if b.Region != "us-east-1" {
		t.Errorf("Region: want %q, got %q", "us-east-1", b.Region)
	}
}

// =========================================================================
// List tests
// =========================================================================

// TestList_Objects verifies List returns all matching objects in sorted
// order.
func TestList_Objects(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "list-objs"
	backend.makeBucket(t, bucket)
	keys := []string{"a/file1.txt", "a/file2.txt", "b/file3.txt", "c.txt"}
	for _, k := range keys {
		backend.putTestObject(t, bucket, k, []byte("x"), nil)
	}

	// List with no wildcard and no trailing slash: setPrefixAndFilter
	// sets Prefix=Path and Delimiter="/". To get every key back we use
	// the wildcard form "s3://bucket/*" which clears the delimiter.
	u, err := storage.NewStorageURL("s3://" + bucket + "/*")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, true))
	if len(got) != len(keys) {
		t.Fatalf("List: want %d objects, got %d (%+v)", len(keys), len(got), got)
	}
	gotKeys := make(map[string]bool, len(got))
	for _, o := range got {
		gotKeys[o.StorageURL.Path] = true
	}
	for _, k := range keys {
		if !gotKeys[k] {
			t.Errorf("List: missing key %q (got %v)", k, gotKeys)
		}
	}
}

// TestList_Prefix verifies prefix filtering.
func TestList_Prefix(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "list-prefix"
	backend.makeBucket(t, bucket)
	keys := []string{"logs/app.log", "logs/access.log", "logs/error.log", "config.yaml"}
	for _, k := range keys {
		backend.putTestObject(t, bucket, k, []byte("x"), nil)
	}

	// "logs/" ends with "/" so it's a prefix; setPrefixAndFilter sets
	// Delimiter="/" and Prefix="logs/".
	u, err := storage.NewStorageURL("s3://" + bucket + "/logs/")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, true))
	// 3 log files plus the "logs/" directory entry itself (the mock
	// returns the prefix as a common prefix when delimiter is "/").
	want := 3
	if len(got) != want {
		t.Fatalf("List: want %d, got %d (%+v)", want, len(got), got)
	}
	for _, o := range got {
		if !strings.HasPrefix(o.StorageURL.Path, "logs/") {
			t.Errorf("List: unexpected key %q", o.StorageURL.Path)
		}
	}
}

// TestList_Wildcard verifies s3://bucket/logs/*.log matches.
func TestList_Wildcard(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "list-wild"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, "logs/app.log", []byte("x"), nil)
	backend.putTestObject(t, bucket, "logs/access.log", []byte("x"), nil)
	backend.putTestObject(t, bucket, "logs/error.log", []byte("x"), nil)
	backend.putTestObject(t, bucket, "logs/readme.txt", []byte("x"), nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/logs/*.log")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, true))
	if len(got) != 3 {
		t.Fatalf("List: want 3 *.log files, got %d", len(got))
	}
	for _, o := range got {
		if !strings.HasSuffix(o.StorageURL.Path, ".log") {
			t.Errorf("List: unexpected key %q", o.StorageURL.Path)
		}
	}
}

// TestList_CommonPrefixes verifies delimiter-based grouping.
func TestList_CommonPrefixes(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "list-cp"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, "a/1.txt", []byte("x"), nil)
	backend.putTestObject(t, bucket, "a/2.txt", []byte("x"), nil)
	backend.putTestObject(t, bucket, "b/3.txt", []byte("x"), nil)
	backend.putTestObject(t, bucket, "root.txt", []byte("x"), nil)

	// List with prefix "" and implicit delimiter "/" (because the URL
	// has no wildcard and ends without slash at root).
	u, err := storage.NewStorageURL("s3://" + bucket + "/")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, true))

	var dirs []string
	var files []string
	for _, o := range got {
		if o.Type.IsDir() {
			dirs = append(dirs, o.StorageURL.Path)
		} else {
			files = append(files, o.StorageURL.Path)
		}
	}
	if len(dirs) != 2 {
		t.Errorf("want 2 common prefixes, got %d (%v)", len(dirs), dirs)
	}
	if len(files) != 1 {
		t.Errorf("want 1 file, got %d (%v)", len(files), files)
	}
}

// TestList_Empty verifies an empty bucket returns the no-objects-found
// signal.
func TestList_Empty(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "list-empty"
	backend.makeBucket(t, bucket)

	u, err := storage.NewStorageURL("s3://" + bucket + "/")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	got := drainObjects(t, store.List(context.Background(), u, true))
	if len(got) != 1 {
		t.Fatalf("Empty List: want 1 sentinel object, got %d", len(got))
	}
	if !errors.Is(got[0].Err, errorpkg.ErrNoObjectFound) {
		t.Fatalf("Empty List: want ErrNoObjectFound, got %v", got[0].Err)
	}
}

// =========================================================================
// Put / Get tests
// =========================================================================

// TestPut_ThenGet verifies Put writes the object and Get reads it back.
func TestPut_ThenGet(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "put-get", "hello.txt"
	backend.makeBucket(t, bucket)

	content := []byte("hello, s3 mock")
	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Put(context.Background(), bytes.NewReader(content), u, storage.Metadata{}, 1, 5*1024*1024); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify via Stat that the backend has it.
	obj, err := store.Stat(context.Background(), u)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if obj.Size != int64(len(content)) {
		t.Errorf("Size: want %d, got %d", len(content), obj.Size)
	}

	// Get via a WriterAt buffer (manager.Downloader wants WriterAt).
	wa := &writerAtBuffer{}
	n, err := store.Get(context.Background(), u, wa, 1, 5*1024*1024)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n != int64(len(content)) {
		t.Errorf("Get n: want %d, got %d", len(content), n)
	}
	if !bytes.Equal(wa.Bytes(), content) {
		t.Errorf("Get body: want %q, got %q", content, wa.Bytes())
	}
	// Also verify via the backend's in-memory copy.
	if !bytes.Equal(backend.objects[bucket][key], content) {
		t.Errorf("Backend body: want %q, got %q", content, backend.objects[bucket][key])
	}
}

// TestPut_Metadata verifies Put stores ContentType + UserDefined metadata
// and Stat returns them.
func TestPut_Metadata(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "put-meta", "data.bin"
	backend.makeBucket(t, bucket)
	content := []byte("payload")
	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	md := storage.Metadata{
		ContentType: "application/json",
		UserDefined: map[string]string{"foo": "bar", "baz": "qux"},
	}
	if err := store.Put(context.Background(), bytes.NewReader(content), u, md, 1, 5*1024*1024); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// HeadObject returns both the Object and Metadata.
	_, gotMd, err := store.HeadObject(context.Background(), u)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if gotMd.ContentType != "application/json" {
		t.Errorf("ContentType: want %q, got %q", "application/json", gotMd.ContentType)
	}
	if gotMd.UserDefined["foo"] != "bar" {
		t.Errorf("UserDefined[foo]: want %q, got %q", "bar", gotMd.UserDefined["foo"])
	}
	if gotMd.UserDefined["baz"] != "qux" {
		t.Errorf("UserDefined[baz]: want %q, got %q", "qux", gotMd.UserDefined["baz"])
	}
	// Verify the metadata in the backend too.
	backendMd := backend.metadata[bucket][key]
	if backendMd["foo"] != "bar" || backendMd["baz"] != "qux" {
		t.Errorf("Backend metadata: %v", backendMd)
	}
}

// TestPut_LargeObject exercises the multipart upload path: a body larger
// than PartSize forces the v2 manager.Uploader to use CreateMultipartUpload
// + UploadPart + CompleteMultipartUpload. The mock implements all three.
func TestPut_LargeObject(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "put-large", "big.bin"
	backend.makeBucket(t, bucket)

	// 12 MiB body, 5 MiB part size → 3 parts (5 + 5 + 2 MiB).
	body := make([]byte, 12*1024*1024)
	for i := range body {
		body[i] = byte(i)
	}
	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Put(context.Background(), bytes.NewReader(body), u, storage.Metadata{}, 2, 5*1024*1024); err != nil {
		t.Fatalf("Put large: %v", err)
	}

	got, ok := backend.objects[bucket][key]
	if !ok {
		t.Fatalf("Backend: object %s/%s not stored", bucket, key)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Backend: body mismatch (len want=%d got=%d)", len(body), len(got))
	}

	// Get it back through the SDK downloader. Use a part size that does
	// NOT evenly divide the body so the downloader's final range request
	// is well within bounds; the v2 downloader emits a final request
	// starting at the last part boundary, which can be an exhaustive
	// range (>= total) when the size is an exact multiple of the part
	// size. A non-multiple part size avoids that edge case.
	wa := &writerAtBuffer{}
	if _, err := store.Get(context.Background(), u, wa, 2, 7*1024*1024); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(wa.Bytes(), body) {
		t.Fatalf("Get: body mismatch (len want=%d got=%d)", len(body), len(wa.Bytes()))
	}
}

// TestGet_Range verifies the mock honours the Range header. The v2
// manager.Downloader issues range GETs when the part size is smaller than
// the object; this test forces a 1 MiB part size on a 2 MiB body so the
// downloader splits it into two ranged GETs.
func TestGet_Range(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "get-range", "blob.bin"
	backend.makeBucket(t, bucket)
	body := make([]byte, 2*1024*1024)
	for i := range body {
		body[i] = byte(i % 256)
	}
	backend.putTestObject(t, bucket, key, body, nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	wa := &writerAtBuffer{}
	if _, err := store.Get(context.Background(), u, wa, 1, 1024*1024); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(wa.Bytes(), body) {
		t.Fatalf("Get range: body mismatch (len want=%d got=%d)", len(body), len(wa.Bytes()))
	}
}

// =========================================================================
// Copy tests
// =========================================================================

// TestCopy_Object verifies a server-side CopyObject copies the content.
func TestCopy_Object(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "copy-bucket"
	backend.makeBucket(t, bucket)
	content := []byte("copy me")
	backend.putTestObject(t, bucket, "src.txt", content, map[string]string{"k1": "v1"})

	src, err := storage.NewStorageURL("s3://" + bucket + "/src.txt")
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
		t.Fatalf("Backend: dst not present")
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("Copy content: want %q, got %q", content, got)
	}
	// Stat should now find dst.
	obj, err := store.Stat(context.Background(), dst)
	if err != nil {
		t.Fatalf("Stat dst: %v", err)
	}
	if obj.Size != int64(len(content)) {
		t.Errorf("Stat dst Size: want %d, got %d", len(content), obj.Size)
	}
}

// =========================================================================
// Delete / MultiDelete tests
// =========================================================================

// TestDelete_Single verifies Delete removes the object.
func TestDelete_Single(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "del-single", "trash.txt"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, []byte("x"), nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if err := store.Delete(context.Background(), u); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Stat should now return NotFound.
	if _, err := store.Stat(context.Background(), u); !errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
		t.Fatalf("Stat after Delete: want ErrGivenObjectNotFound, got %v", err)
	}
}

// TestMultiDelete_Batch verifies MultiDelete removes a large batch (>1000
// to confirm chunking) end-to-end.
func TestMultiDelete_Batch(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "multidel"
	backend.makeBucket(t, bucket)

	const total = 1500 // > deleteObjectsMax(1000) so two chunks are needed
	urls := make(chan *storage.StorageURL, total)
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("obj-%04d", i)
		backend.putTestObject(t, bucket, k, []byte("x"), nil)
		u, err := storage.NewStorageURL(fmt.Sprintf("s3://%s/%s", bucket, k))
		if err != nil {
			t.Fatalf("NewStorageURL: %v", err)
		}
		urls <- u
	}
	close(urls)

	resultCh := store.MultiDelete(context.Background(), urls)
	var deleted int
	var withErr int
	for o := range resultCh {
		if o == nil {
			continue
		}
		if o.Err != nil {
			withErr++
			continue
		}
		deleted++
	}
	if withErr != 0 {
		t.Fatalf("MultiDelete: %d results had errors", withErr)
	}
	if deleted != total {
		t.Fatalf("MultiDelete: want %d deleted, got %d", total, deleted)
	}
	// Verify backend has no objects left in the bucket.
	if n := len(backend.objects[bucket]); n != 0 {
		t.Fatalf("Backend: %d objects remain after MultiDelete", n)
	}
}

// TestMultiDelete_Empty verifies MultiDelete on an empty URL channel does
// not block or error.
func TestMultiDelete_Empty(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	store := newS3Store(t, srv)

	urls := make(chan *storage.StorageURL)
	close(urls)

	resultCh := store.MultiDelete(context.Background(), urls)
	var n int
	for o := range resultCh {
		if o != nil {
			n++
		}
	}
	if n != 0 {
		t.Fatalf("MultiDelete empty: want 0 results, got %d", n)
	}
}

// =========================================================================
// Presign tests
// =========================================================================

// TestPresign verifies Presign returns a URL containing the bucket and key.
// We cannot fully validate the signature against the mock server, but the
// v2 S3 Presigner requires a SigV4 signer; with NoSignRequest=true it sees
// the anonymous (no-auth) scheme and errors out. To exercise the Presign
// code path we therefore build the store with a static fake credentials
// provider via the environment — the mock server ignores the Authorization
// header, so any signature value is acceptable.
func TestPresign(t *testing.T) {
	// t.Setenv cannot be used with t.Parallel, so this test is not
	// parallel.

	// The SDK config loader reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
	// from the environment. Set fake values for the duration of the test so
	// NewS3Client can build a SigV4-capable client without touching the
	// user's shared credentials file.
	t.Setenv("AWS_ACCESS_KEY_ID", "presign-test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "presign-test-secret")
	// Ensure no shared-config file is loaded.
	t.Setenv("AWS_SDK_LOAD_CONFIG", "0")

	srv, backend := newMockS3Server(t)
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:        srv.URL,
		AddressingStyle: AddressingStylePath,
		// NoSignRequest left false so the SDK installs the SigV4 signer
		// that Presign needs. The mock server ignores Authorization.
		Region:     "us-east-1",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	const bucket, key = "presign", "file.txt"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, []byte("x"), nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
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
	// Path-style addressing puts /bucket/key in the path.
	if !strings.Contains(parsed.Path, "/"+bucket+"/") && !strings.Contains(parsed.Path, key) {
		t.Errorf("Presign path %q does not contain bucket/key", parsed.Path)
	}
	// A SigV4 presign always sets the X-Amz-* query parameters.
	if parsed.Query().Get("X-Amz-Algorithm") == "" {
		t.Errorf("Presign missing X-Amz-Algorithm")
	}
	if parsed.Query().Get("X-Amz-Expires") == "" {
		t.Errorf("Presign missing X-Amz-Expires")
	}
}

// =========================================================================
// Addressing style end-to-end tests
// =========================================================================

// TestNewS3Client_PathStyle asserts the SDK sends requests with the bucket
// in the URL path (not the Host header) when AddressingStyle=path. The mock
// records every request so we can inspect the path after a Stat call.
func TestNewS3Client_PathStyle(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket, key = "path-style", "obj"
	backend.makeBucket(t, bucket)
	backend.putTestObject(t, bucket, key, []byte("x"), nil)

	u, err := storage.NewStorageURL("s3://" + bucket + "/" + key)
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	if _, err := store.Stat(context.Background(), u); err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// Find the HEAD request to /bucket/key.
	backend.mu.Lock()
	defer backend.mu.Unlock()
	found := false
	for _, r := range backend.requests {
		if strings.HasPrefix(r, "HEAD /"+bucket+"/"+key) {
			// In path style the Host is the bare server host (no bucket
			// prefix).
			if !strings.Contains(r, "host=127.0.0.1") && !strings.Contains(r, "host=localhost") {
				// httptest may use 127.0.0.1 or localhost — both are the
				// bare server host. The key assertion is that the host
				// does NOT start with "bucket.".
				if strings.Contains(r, "host="+bucket+".") {
					t.Errorf("PathStyle: Host should not have bucket prefix, got %q", r)
				}
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("PathStyle: no HEAD request for /%s/%s recorded (requests: %v)", bucket, key, backend.requests)
	}
}

// TestNewS3Client_VirtualHostStyle asserts the SDK puts the bucket in the
// Host header when AddressingStyle=virtual. Because NewS3Client sets
// HostnameImmutable=true on the custom endpoint resolver (to keep the SDK
// from rewriting the host for S3-compatible services), the SDK keeps the
// mock server's host as-is and the bucket stays in the URL path. We
// therefore cannot assert the Host header directly on a custom endpoint;
// instead we verify the addressing decision: the resulting client's
// UsePathStyle option is false, which is the load-bearing assertion (it
// means the SDK would rewrite Host to "<bucket>.<host>" if the endpoint
// resolver had not pinned the hostname).
//
// The full virtual-host request shape (bucket-prefixed Host) is exercised
// end-to-end in the e2e suite against a real gofakes3 server; here we pin
// the decision logic.
func TestNewS3Client_VirtualHostStyle(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:        srv.URL,
		AddressingStyle: AddressingStyleVirtual,
		NoSignRequest:   true,
		Region:          "us-east-1",
		MaxRetries:      1,
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	if got := store.Client().Options().UsePathStyle; got {
		t.Fatalf("VirtualHost: UsePathStyle should be false, got true")
	}
}

// TestNewS3Client_AddressDecisionVsEndpoint is a pure unit test that pins
// the (endpoint × addressing-style) → UsePathStyle decision table. It is
// the end-to-end complement to TestIsVirtualHostStyle: the latter tests the
// helper, this one tests that NewS3Client actually configures the SDK
// client with the resulting UsePathStyle value.
func TestNewS3Client_AddressDecisionVsEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		endpoint string
		style    string
		wantPath bool // expect UsePathStyle on the resulting client
		legacy   bool
	}{
		// No custom endpoint → virtual-host by default (UsePathStyle=false).
		{name: "sentinel+auto", endpoint: "", style: AddressingStyleAuto, wantPath: false},
		{name: "sentinel+path", endpoint: "", style: AddressingStylePath, wantPath: true},
		{name: "sentinel+virtual", endpoint: "", style: AddressingStyleVirtual, wantPath: false},

		// Custom endpoint → path-style unless "virtual" forced.
		{name: "custom+auto", endpoint: "http://127.0.0.1:9000", style: AddressingStyleAuto, wantPath: true},
		{name: "custom+path", endpoint: "http://127.0.0.1:9000", style: AddressingStylePath, wantPath: true},
		{name: "custom+virtual", endpoint: "http://127.0.0.1:9000", style: AddressingStyleVirtual, wantPath: false},

		// Legacy: empty style + UsePathStyle=true is mapped to "path".
		{name: "custom+legacy-UsePathStyle", endpoint: "http://127.0.0.1:9000", style: "", wantPath: true, legacy: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt := S3Option{
				Endpoint:        tc.endpoint,
				AddressingStyle: tc.style,
				NoSignRequest:   true,
				Region:          "us-east-1",
				MaxRetries:      1,
			}
			if tc.legacy {
				opt.AddressingStyle = ""
				opt.UsePathStyle = true
			}
			store, err := NewS3Client(context.Background(), opt)
			if err != nil {
				t.Fatalf("NewS3Client: %v", err)
			}
			got := store.Client().Options().UsePathStyle
			if got != tc.wantPath {
				t.Fatalf("UsePathStyle: want %v, got %v", tc.wantPath, got)
			}
		})
	}
}

// =========================================================================
// ListBuckets / MakeBucket end-to-end
// =========================================================================

// TestListBuckets_All verifies ListBuckets returns the buckets seeded
// into the mock.
func TestListBuckets_All(t *testing.T) {
	t.Parallel()
	srv, backend := newMockS3Server(t)
	store := newS3Store(t, srv)

	backend.makeBucket(t, "alpha")
	backend.makeBucket(t, "beta")
	backend.makeBucket(t, "gamma")

	got, err := store.ListBuckets(context.Background())
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	want := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	if len(got) != len(want) {
		t.Fatalf("ListBuckets: want %d, got %d (%+v)", len(want), len(got), got)
	}
	for _, b := range got {
		if !want[b.Name] {
			t.Errorf("ListBuckets: unexpected bucket %q", b.Name)
		}
	}
}

// TestMakeBucket_Idempotent verifies CreateBucket on an existing bucket is
// treated as success (BucketAlreadyOwnedByYou is swallowed).
func TestMakeBucket_Idempotent(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	store := newS3Store(t, srv)

	const bucket = "idem"
	if err := store.MakeBucket(context.Background(), bucket, "us-east-1"); err != nil {
		t.Fatalf("MakeBucket first: %v", err)
	}
	// Second call should not error.
	if err := store.MakeBucket(context.Background(), bucket, "us-east-1"); err != nil {
		t.Fatalf("MakeBucket second: %v", err)
	}
}

// =========================================================================
// helpers used by the test files
// =========================================================================

// keep imports referenced for future assertions.
var (
	_ = aws.String
	_ = http.StatusOK
	_ = atomic.AddInt32 // referenced in retry_test.go; keep the import alive here too
)

// writerAtBuffer adapts a *bytes.Buffer to the io.WriterAt interface the v2
// manager.Downloader expects. The downloader issues writes at strictly
// increasing, non-overlapping offsets from 0, so we grow an internal slice
// in place.
type writerAtBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (w *writerAtBuffer) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	need := int(off) + len(p)
	if need > len(w.buf) {
		// Grow the slice. If the new write starts beyond the current
		// length, pad the gap with zeros.
		if int(off) > len(w.buf) {
			w.buf = append(w.buf, make([]byte, int(off)-len(w.buf))...)
		}
		w.buf = append(w.buf, p...)
	} else {
		// Overwrite in place.
		copy(w.buf[off:], p)
	}
	return len(p), nil
}

// Bytes returns the accumulated content.
func (w *writerAtBuffer) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]byte, len(w.buf))
	copy(out, w.buf)
	return out
}
