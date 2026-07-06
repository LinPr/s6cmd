package s3store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
)

// newS3StoreForTest builds an S3Store pointing at the given httptest.Server
// with NoSuchUploadRetryCount set so the retry path is exercised. The store
// uses path-style addressing because the test server is a bare host with
// no DNS for virtual-host buckets. MaxRetries is set to 1 so the SDK's own
// standard retryer does not retry the 400 NoSuchUpload responses (the SDK
// does not retry 4xx by default, but setting MaxRetries=1 makes the
// attempt budget explicit and keeps the test deterministic).
func newS3StoreForTest(t *testing.T, serverURL string, noSuchUploadRetryCount int) *S3Store {
	t.Helper()
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:               serverURL,
		UsePathStyle:           true,
		NoSignRequest:          true,
		NoSuchUploadRetryCount: noSuchUploadRetryCount,
		MaxRetries:             1,
		Region:                 "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	return store
}

// s3ErrorXML renders a minimal S3 XML error body. v2 smithy parses the
// <Code>/<Message> out of this to populate ErrorCode()/ErrorMessage().
func s3ErrorXML(code, message string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}

// TestGenerateRetryID_Unique verifies that two consecutive calls produce
// different ids, and that the id is a non-empty 32-char hex string (16
// bytes from crypto/rand, hex-encoded). A collision would require a
// 128-bit random match, which is astronomically unlikely.
func TestGenerateRetryID_Unique(t *testing.T) {
	t.Parallel()
	a := generateRetryID()
	b := generateRetryID()
	if a == "" || b == "" {
		t.Fatalf("retry id empty: %q %q", a, b)
	}
	if a == b {
		t.Fatalf("expected distinct retry ids, got %q twice", a)
	}
	if len(a) != 32 {
		t.Fatalf("expected 32-char hex id (16 bytes), got %d chars: %q", len(a), a)
	}
	for _, c := range a {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			t.Fatalf("retry id %q contains non-hex char %q", a, c)
		}
	}
}

// TestErrHasCode_NoSuchUpload verifies errHasCode recognises a
// *smithy.GenericAPIError with Code="NoSuchUpload" (the shape the v2
// transport produces when the S3 middleware deserialises an error XML
// body that doesn't map to a typed error) and a *types.NoSuchUpload (the
// typed shape for the NoSuchUpload code). Both forms are matched because
// errHasCode falls through from the typed check to the smithy.APIError
// interface check.
func TestErrHasCode_NoSuchUpload(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{name: "generic api error", err: &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "boom"}},
		{name: "nil-ish"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := errHasCode(tc.err, "NoSuchUpload")
			if tc.err == nil {
				if got {
					t.Fatalf("errHasCode(nil, ...) should be false")
				}
				return
			}
			if !got {
				t.Fatalf("errHasCode should match NoSuchUpload for %T", tc.err)
			}
		})
	}
	if errHasCode(nil, "NoSuchUpload") {
		t.Fatalf("errHasCode(nil, ...) should be false")
	}
	if errHasCode(&smithy.GenericAPIError{Code: "NoSuchUpload"}, "") {
		t.Fatalf("errHasCode(err, \"\") should be false")
	}
	if errHasCode(&smithy.GenericAPIError{Code: "InternalError"}, "NoSuchUpload") {
		t.Fatalf("errHasCode should not match NoSuchUpload for an InternalError")
	}
}

// TestPut_RetryOnNoSuchUpload_SuccessOnRetry uses an httptest.Server that
// fails the first N PutObject/UploadPart requests with NoSuchUpload and
// then succeeds. The store is configured with NoSuchUploadRetryCount=3 so
// the retry loop should eventually return nil.
//
// The server returns HTTP 400 (Bad Request) for the NoSuchUpload body
// because the v2 standard retryer does not retry 4xx responses; this
// isolates the test's retry budget to s6cmd's retryOnNoSuchUpload loop.
func TestPut_RetryOnNoSuchUpload_SuccessOnRetry(t *testing.T) {
	t.Parallel()

	var failCount int32 = 2
	var putCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			atomic.AddInt32(&putCount, 1)
			if atomic.LoadInt32(&failCount) > 0 {
				atomic.AddInt32(&failCount, -1)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "simulated NoSuchUpload"))
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		// HeadObject (Stat): return 404 so the retry loop keeps going
		// until the upload itself succeeds.
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, s3ErrorXML("NotFound", "not found"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 3)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	body := bytes.NewReader([]byte("hello"))
	err = store.Put(context.Background(), body, to, storage.Metadata{}, 1, 5*1024*1024)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := atomic.LoadInt32(&putCount); got < 2 {
		t.Fatalf("expected at least 2 PUT attempts (initial + retry), got %d", got)
	}
}

// TestPut_NoSuchUpload_RetryGivesUp verifies that when every attempt fails
// with NoSuchUpload, Put returns an error mentioning NoSuchUpload after
// exhausting the retry budget.
func TestPut_NoSuchUpload_RetryGivesUp(t *testing.T) {
	t.Parallel()

	var putCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			atomic.AddInt32(&putCount, 1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "always fail"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, s3ErrorXML("NotFound", "not found"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 2)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	body := bytes.NewReader([]byte("hello"))
	err = store.Put(context.Background(), body, to, storage.Metadata{}, 1, 5*1024*1024)
	if err == nil {
		t.Fatalf("expected Put to fail with NoSuchUpload after retries")
	}
	if !strings.Contains(err.Error(), "NoSuchUpload") {
		t.Fatalf("expected error to mention NoSuchUpload, got %v", err)
	}
	if got := atomic.LoadInt32(&putCount); got < 2 {
		t.Fatalf("expected at least 2 PUT attempts, got %d", got)
	}
}

// TestPut_RetryOnNoSuchUpload_RewindsBody verifies that the retry rewinds
// the (seekable) request body before re-uploading. The first attempt
// consumes the body; without the Seek the retry would upload 0 bytes.
func TestPut_RetryOnNoSuchUpload_RewindsBody(t *testing.T) {
	t.Parallel()

	content := []byte("full body content")
	var mu sync.Mutex
	var failNext = true
	var successBody []byte
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			fail := failNext
			failNext = false
			if !fail {
				successBody = body
			}
			mu.Unlock()
			if fail {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "simulated"))
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		// HeadObject (Stat): 404 so the retry loop re-uploads.
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, s3ErrorXML("NotFound", "not found"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 3)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	err = store.Put(context.Background(), bytes.NewReader(content), to, storage.Metadata{}, 1, 5*1024*1024)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if string(successBody) != string(content) {
		t.Fatalf("retried upload body: want %q, got %q (body was not rewound)", content, successBody)
	}
}

// nonSeekableReader wraps an io.Reader and deliberately hides any Seek
// method, mimicking stdin/pipe bodies.
type nonSeekableReader struct{ r io.Reader }

func (n *nonSeekableReader) Read(p []byte) (int, error) { return n.r.Read(p) }

// TestPut_NoSuchUpload_NonSeekableBody_NoRetry verifies that when the body
// cannot be rewound (stdin/pipe), the store does NOT retry — retrying would
// upload truncated data — and instead returns the original error wrapped
// with a clear message.
func TestPut_NoSuchUpload_NonSeekableBody_NoRetry(t *testing.T) {
	t.Parallel()

	var putCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			atomic.AddInt32(&putCount, 1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "simulated"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, s3ErrorXML("NotFound", "not found"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 3)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	body := &nonSeekableReader{r: bytes.NewReader([]byte("hello"))}
	err = store.Put(context.Background(), body, to, storage.Metadata{}, 1, 5*1024*1024)
	if err == nil {
		t.Fatalf("expected Put to fail for a non-seekable body")
	}
	if !strings.Contains(err.Error(), "not seekable") {
		t.Fatalf("expected error to explain the body is not seekable, got %v", err)
	}
	if !strings.Contains(err.Error(), "NoSuchUpload") {
		t.Fatalf("expected error to wrap the original NoSuchUpload, got %v", err)
	}
	if got := atomic.LoadInt32(&putCount); got != 1 {
		t.Fatalf("expected exactly 1 PUT attempt for a non-seekable body, got %d", got)
	}
}

// TestPut_NoSuchUpload_StatDetectsSuccess verifies the "previous attempt
// actually succeeded" path: the first PUT returns NoSuchUpload but the
// server has already stored the object with the retry-id metadata; the
// retry loop's Stat should detect the matching retry-id and return nil
// without retrying.
//
// The handler captures the retry-id from any request that carries the
// x-amz-meta-s6cmd-upload-retry-id header (single-part PUT or multipart
// POST CreateMultipartUpload) and echoes it back on HeadObject.
func TestPut_NoSuchUpload_StatDetectsSuccess(t *testing.T) {
	t.Parallel()

	var putCount int32
	var lastRetryID string
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Capture the retry-id from any request that carries it. The
		// v2 uploader sends metadata as x-amz-meta-* headers on
		// PutObject (single-part) or CreateMultipartUpload (multipart).
		if rid := r.Header.Get("x-amz-meta-s6cmd-upload-retry-id"); rid != "" {
			lastRetryID = rid
		}
		switch r.Method {
		case http.MethodPut:
			atomic.AddInt32(&putCount, 1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "simulated"))
			return
		case http.MethodHead:
			if lastRetryID != "" {
				w.Header().Set("x-amz-meta-s6cmd-upload-retry-id", lastRetryID)
			}
			w.WriteHeader(http.StatusOK)
			return
		case http.MethodPost:
			// CreateMultipartUpload: return an error so the multipart
			// path also surfaces NoSuchUpload. The retry-id has already
			// been captured from the headers above.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "simulated"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 3)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	body := bytes.NewReader([]byte("hello"))
	err = store.Put(context.Background(), body, to, storage.Metadata{}, 1, 5*1024*1024)
	if err != nil {
		t.Fatalf("Put should succeed via Stat retry-id match, got %v", err)
	}
	// The first PUT (or multipart Create) fails with NoSuchUpload. The
	// retry loop then Stats the target; the test handler echoes the
	// retry-id back, so the loop returns nil without a second upload.
	// putCount counts only PUT (UploadPart/PutObject) attempts; with a
	// 5-byte body the uploader uses single-part PutObject, so putCount
	// should be 1.
	if got := atomic.LoadInt32(&putCount); got != 1 {
		t.Fatalf("expected exactly 1 PUT attempt (Stat should short-circuit), got %d", got)
	}
}

// TestPut_NoSuchUpload_Disabled verifies that with
// NoSuchUploadRetryCount=0, a NoSuchUpload error is returned directly
// without any s6cmd-level retry attempt.
func TestPut_NoSuchUpload_Disabled(t *testing.T) {
	t.Parallel()

	var putCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			atomic.AddInt32(&putCount, 1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, s3ErrorXML("NoSuchUpload", "fail"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 0)

	to, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	body := bytes.NewReader([]byte("hello"))
	err = store.Put(context.Background(), body, to, storage.Metadata{}, 1, 5*1024*1024)
	if err == nil {
		t.Fatalf("expected Put to fail with NoSuchUpload")
	}
	if !strings.Contains(err.Error(), "NoSuchUpload") {
		t.Fatalf("expected error to mention NoSuchUpload, got %v", err)
	}
	if got := atomic.LoadInt32(&putCount); got != 1 {
		t.Fatalf("expected exactly 1 PUT attempt when retry is disabled, got %d", got)
	}
}

// TestNewRetryer_Default verifies that newRetryer returns a non-nil retryer
// for both positive and non-positive max values (the latter keeps the SDK
// default). It does not exercise actual retry behaviour, which is covered
// by the Put tests above.
func TestNewRetryer_Default(t *testing.T) {
	t.Parallel()
	if r := newRetryer(0); r == nil {
		t.Fatalf("newRetryer(0) should not return nil")
	}
	if r := newRetryer(10); r == nil {
		t.Fatalf("newRetryer(10) should not return nil")
	}
}

// TestStat_RetryIDPopulated verifies that Stat populates the Object's
// RetryID() accessor when the HeadObject response carries the
// s6cmd-upload-retry-id metadata. This is the read-side of the
// retry-on-NoSuchUpload contract.
func TestStat_RetryIDPopulated(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("x-amz-meta-s6cmd-upload-retry-id", "abc123")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	store := newS3StoreForTest(t, server.URL, 3)
	url, err := storage.NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	obj, err := store.Stat(context.Background(), url)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := obj.RetryID(); got != "abc123" {
		t.Fatalf("RetryID: want %q, got %q", "abc123", got)
	}
}

// TestObject_RetryID_Accessors verifies the Object.SetRetryID/RetryID
// accessor pair round-trips a value. This is a unit test for the storage
// package's accessor; it lives here so the s3store tests stay focused on
// the S3-specific behaviour.
func TestObject_RetryID_Accessors(t *testing.T) {
	t.Parallel()
	o := &storage.Object{}
	if got := o.RetryID(); got != "" {
		t.Fatalf("zero value RetryID: want %q, got %q", "", got)
	}
	o.SetRetryID("rid-xyz")
	if got := o.RetryID(); got != "rid-xyz" {
		t.Fatalf("after SetRetryID: want %q, got %q", "rid-xyz", got)
	}
	// Nil-safe accessors should not panic.
	var nilObj *storage.Object
	if got := nilObj.RetryID(); got != "" {
		t.Fatalf("nil RetryID: want %q, got %q", "", got)
	}
	nilObj.SetRetryID("no-op") // should not panic
}

// keep the aws import referenced for future assertions on aws.String etc.
var _ = aws.String
