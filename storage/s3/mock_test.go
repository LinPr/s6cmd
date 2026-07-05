package s3store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinPr/s6cmd/storage"
)

// mockS3 is a minimal in-memory S3 backend used by the storage/s3 unit
// tests. It implements just enough of the S3 XML API for Stat/List/Put/Get/
// Copy/Delete/MultiDelete/HeadBucket/ListBuckets to be exercised through the
// real aws-sdk-go-v2 transport. Multipart upload is supported in a simplified
// form (CreateMultipartUpload → UploadPart → CompleteMultipartUpload) so the
// v2 manager.Uploader's multipart path can be tested against large objects.
//
// The handler is safe for concurrent use: httptest fires a goroutine per
// request and the v2 SDK's uploader/downloader run concurrent part requests.
type mockS3 struct {
	mu sync.Mutex

	// objects maps bucket/key → content.
	objects map[string]map[string][]byte
	// metadata maps bucket/key → metadata. Keys are stored in their
	// original (header-case-preserved) form; lookups are case-insensitive
	// on read.
	metadata map[string]map[string]map[string]string
	// contentType maps bucket/key → Content-Type.
	contentType map[string]map[string]string
	// modTime maps bucket/key → last modification time.
	modTime map[string]map[string]time.Time
	// buckets records bucket existence and creation time.
	buckets map[string]time.Time
	// multipart uploads indexed by upload id.
	multipart map[string]*mockMultipart

	// requests records every request (method + path + host) seen by the
	// handler. Tests assert addressing style by inspecting this slice.
	requests []string

	// srvURL is the base URL of the httptest server; set by
	// newMockS3Server so putTestObject can reach the mock without the
	// caller having to thread the *httptest.Server around.
	srvURL string
}

// mockMultipart is a single in-flight multipart upload.
type mockMultipart struct {
	bucket   string
	key      string
	metadata map[string]string
	parts    map[int][]byte
	created  time.Time
}

// newMockS3 returns an empty mockS3 backend.
func newMockS3() *mockS3 {
	return &mockS3{
		objects:     map[string]map[string][]byte{},
		metadata:    map[string]map[string]map[string]string{},
		contentType: map[string]map[string]string{},
		modTime:     map[string]map[string]time.Time{},
		buckets:     map[string]time.Time{},
		multipart:   map[string]*mockMultipart{},
	}
}

// keyPath returns the S3 object key from a request path, given the bucket
// that the SDK has already placed in the path (path-style) or host
// (virtual-host style, where the path begins with "/").
func (m *mockS3) keyFromRequest(r *http.Request, bucket string) string {
	p := strings.TrimPrefix(r.URL.Path, "/"+bucket)
	// Strip the leading "/" that remains after the bucket segment, but only
	// when it is a separator. A request to the bucket root yields an empty
	// key here, which is correct for ListBuckets / ListObjects.
	p = strings.TrimPrefix(p, "/")
	return p
}

// bucketFromRequest resolves the bucket name for either addressing style.
// For path-style the SDK puts the bucket in the first path segment; for
// virtual-host style the bucket is the first label of the Host header.
func (m *mockS3) bucketFromRequest(r *http.Request) string {
	// httptest's server URL is http://127.0.0.1:PORT. For virtual-host style
	// the SDK rewrites Host to "bucket.127.0.0.1:PORT" (which DNS resolves to
	// the loopback under modern Linux) — but we cannot rely on that in CI.
	// Instead the test suite uses path-style for the bulk of its assertions
	// and uses Host-header inspection for the addressing-style end-to-end
	// check (see TestNewS3Client_VirtualHostStyle). For the path-style path,
	// the bucket is the first non-empty segment of r.URL.Path.
	p := strings.Trim(r.URL.Path, "/")
	if p == "" {
		return ""
	}
	if idx := strings.IndexByte(p, '/'); idx >= 0 {
		return p[:idx]
	}
	return p
}

// recordRequest appends a one-line summary of the request to m.requests.
// The format is "METHOD path host=<Host>".
func (m *mockS3) recordRequest(r *http.Request) {
	m.mu.Lock()
	m.requests = append(m.requests, fmt.Sprintf("%s %s host=%s", r.Method, r.URL.Path, r.Host))
	m.mu.Unlock()
}

// ServeHTTP implements http.Handler.
func (m *mockS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.recordRequest(r)

	// GET / with no path → ListBuckets.
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		m.handleListBuckets(w, r)
		return
	}

	bucket := m.bucketFromRequest(r)
	key := m.keyFromRequest(r, bucket)

	switch {
	case r.Method == http.MethodHead && bucket != "" && key == "":
		// HeadBucket: path-style "/bucket" with no key.
		m.handleHeadBucket(w, r, bucket)
		return
	case r.Method == http.MethodPut && bucket != "" && key == "":
		// CreateBucket.
		m.handleCreateBucket(w, r, bucket)
		return
	case r.Method == http.MethodDelete && bucket != "" && key == "":
		// DeleteBucket.
		m.handleDeleteBucket(w, r, bucket)
		return
	}

	// Query-string dispatch for the operations that key off ?delete,
	// ?uploads, ?uploadId=.
	q := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		switch {
		case key == "" && q.Has("list-type"):
			m.handleListObjectsV2(w, r, bucket)
		case key == "":
			m.handleListObjectsV2(w, r, bucket) // bare GET /bucket defaults to V2
		default:
			m.handleGetObject(w, r, bucket, key)
		}
	case http.MethodHead:
		m.handleHeadObject(w, r, bucket, key)
	case http.MethodPut:
		if q.Get("uploadId") != "" {
			m.handleUploadPart(w, r, bucket, key, q.Get("uploadId"))
			return
		}
		if cs := r.Header.Get("x-amz-copy-source"); cs != "" {
			m.handleCopyObject(w, r, bucket, key, cs)
			return
		}
		m.handlePutObject(w, r, bucket, key)
	case http.MethodPost:
		switch {
		case q.Has("delete"):
			m.handleDeleteObjects(w, r, bucket)
		case q.Has("uploads"):
			// CreateMultipartUpload is a POST with ?uploads.
			m.handleCreateMultipartUpload(w, r, bucket, key)
		case q.Get("uploadId") != "":
			m.handleCompleteMultipartUpload(w, r, bucket, key, q.Get("uploadId"))
		default:
			http.Error(w, "not implemented", http.StatusNotImplemented)
		}
	case http.MethodDelete:
		m.handleDeleteObject(w, r, bucket, key)
	default:
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

// --- ListBuckets ---

func (m *mockS3) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	type bucketXML struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	type result struct {
		XMLName xml.Name   `xml:"ListAllMyBucketsResult"`
		Buckets []bucketXML `xml:"Buckets>Bucket"`
	}

	res := result{}
	for name, t := range m.buckets {
		res.Buckets = append(res.Buckets, bucketXML{
			Name:         name,
			CreationDate: t.UTC().Format(time.RFC3339),
		})
	}
	// Sort for deterministic output.
	sort.Slice(res.Buckets, func(i, j int) bool { return res.Buckets[i].Name < res.Buckets[j].Name })

	writeXML(w, http.StatusOK, res)
}

// --- HeadBucket ---

func (m *mockS3) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	m.mu.Lock()
	_, ok := m.buckets[bucket]
	m.mu.Unlock()
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket does not exist")
		return
	}
	w.Header().Set("x-amz-bucket-region", "us-east-1")
	w.WriteHeader(http.StatusOK)
}

// --- CreateBucket / DeleteBucket ---

func (m *mockS3) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; exists {
		writeS3Error(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "bucket already exists")
		return
	}
	m.buckets[bucket] = time.Now().UTC()
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; !exists {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket does not exist")
		return
	}
	if objs, ok := m.objects[bucket]; ok && len(objs) > 0 {
		writeS3Error(w, http.StatusConflict, "BucketNotEmpty", "bucket is not empty")
		return
	}
	delete(m.buckets, bucket)
	w.WriteHeader(http.StatusNoContent)
}

// --- ListObjectsV2 ---

func (m *mockS3) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.buckets[bucket]; !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket does not exist")
		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")

	type contentXML struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type commonPrefixXML struct {
		Prefix string `xml:"Prefix"`
	}
	type result struct {
		XMLName        xml.Name         `xml:"ListBucketResult"`
		Name           string           `xml:"Name"`
		Prefix         string           `xml:"Prefix"`
		Delimiter      string           `xml:"Delimiter,omitempty"`
		IsTruncated    bool             `xml:"IsTruncated"`
		KeyCount       int              `xml:"KeyCount"`
		Contents       []contentXML     `xml:"Contents"`
		CommonPrefixes []commonPrefixXML `xml:"CommonPrefixes"`
	}

	res := result{Name: bucket, Prefix: prefix, Delimiter: delimiter}
	objs := m.objects[bucket]
	keys := make([]string, 0, len(objs))
	for k := range objs {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	common := map[string]struct{}{}
	for _, k := range keys {
		if delimiter != "" {
			// Compute the common prefix: everything after the prefix up to
			// and including the next delimiter.
			rest := strings.TrimPrefix(k, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+1]
				common[cp] = struct{}{}
				continue
			}
		}
		res.Contents = append(res.Contents, contentXML{
			Key:          k,
			LastModified: m.modTime[bucket][k].UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf(`"%x"`, md5.Sum(objs[k])),
			Size:         int64(len(objs[k])),
			StorageClass: "STANDARD",
		})
	}
	for cp := range common {
		res.CommonPrefixes = append(res.CommonPrefixes, commonPrefixXML{Prefix: cp})
	}
	sort.Slice(res.CommonPrefixes, func(i, j int) bool {
		return res.CommonPrefixes[i].Prefix < res.CommonPrefixes[j].Prefix
	})
	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
	writeXML(w, http.StatusOK, res)
}

// --- HeadObject ---

func (m *mockS3) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	objs, ok := m.objects[bucket]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NotFound", "bucket not found")
		return
	}
	content, ok := objs[key]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "key not found")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.Header().Set("Content-Type", m.contentType[bucket][key])
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(content)))
	w.Header().Set("Last-Modified", m.modTime[bucket][key].UTC().Format(http.TimeFormat))
	for k, v := range m.metadata[bucket][key] {
		// Canonicalise to lower-case "x-amz-meta-<key>" so the SDK's
		// case-insensitive header lookup matches. The suffix itself may
		// be in any case; we preserve it as stored (lowercased).
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.WriteHeader(http.StatusOK)
}

// --- GetObject ---

func (m *mockS3) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m.mu.Lock()
	objs, ok := m.objects[bucket]
	if !ok {
		m.mu.Unlock()
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket not found")
		return
	}
	content, ok := objs[key]
	m.mu.Unlock()
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "key not found")
		return
	}
	// Support optional Range request. The v2 manager.Downloader uses
	// ranges for multipart downloads.
	rangeHdr := r.Header.Get("Range")
	if rangeHdr != "" {
		start, end, ok := parseRange(rangeHdr, len(content))
		if !ok {
			// S3 returns 200 with the full body for an exhaustive range
			// (start >= total), which the v2 downloader issues when the
			// object size is an exact multiple of the part size. Treat
			// any unsatisfiable range as a full-body GET to keep the
			// mock forgiving.
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			w.Header().Set("Content-Type", m.contentType[bucket][key])
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
		w.Header().Set("Content-Type", m.contentType[bucket][key])
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.Header().Set("Content-Type", m.contentType[bucket][key])
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(content)))
	w.Header().Set("Last-Modified", m.modTime[bucket][key].UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

// isExhaustiveRange reports whether the range header asks for bytes starting
// at or beyond the object size (e.g. "bytes=10485760-" on a 10485760-byte
// object). S3 treats this as a full-body GET rather than an error.
func isExhaustiveRange(h string, total int) bool {
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	spec := strings.TrimPrefix(h, prefix)
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 || parts[1] != "" {
		return false
	}
	var s int
	if _, err := fmt.Sscanf(parts[0], "%d", &s); err != nil {
		return false
	}
	return s >= total
}

// parseRange parses a single "bytes=start-end" range header. Returns ok=false
// if the range cannot be satisfied at all (malformed or start beyond total).
// An end beyond the object size is clamped to total-1, matching S3's
// behaviour for "bytes=N-M" where M exceeds the object size.
func parseRange(h string, total int) (start, end int, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(h, prefix) {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, prefix)
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	var s, e int
	if parts[0] == "" {
		// suffix range: bytes=-N → last N bytes
		var n int
		if _, err := fmt.Sscanf(parts[1], "%d", &n); err != nil {
			return 0, 0, false
		}
		if n == 0 || n > total {
			return 0, 0, false
		}
		s = total - n
		e = total - 1
	} else {
		if _, err := fmt.Sscanf(parts[0], "%d", &s); err != nil {
			return 0, 0, false
		}
		if parts[1] == "" {
			e = total - 1
		} else if _, err := fmt.Sscanf(parts[1], "%d", &e); err != nil {
			return 0, 0, false
		}
	}
	if s < 0 || s >= total {
		return 0, 0, false
	}
	// Clamp end to the last byte. S3 returns 206 for "bytes=N-M" with M
	// beyond the object size, delivering only the bytes that exist.
	if e >= total {
		e = total - 1
	}
	if s > e {
		return 0, 0, false
	}
	return s, e, true
}

// --- PutObject ---

func (m *mockS3) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[bucket]; !ok {
		// S3 would NoSuchBucket; some compatible stores auto-create. We
		// auto-create to keep tests that Put before MakeBucket simple.
		m.buckets[bucket] = time.Now().UTC()
	}
	if m.objects[bucket] == nil {
		m.objects[bucket] = map[string][]byte{}
		m.metadata[bucket] = map[string]map[string]string{}
		m.contentType[bucket] = map[string]string{}
		m.modTime[bucket] = map[string]time.Time{}
	}
	m.objects[bucket][key] = body
	m.contentType[bucket][key] = r.Header.Get("Content-Type")
	m.modTime[bucket][key] = time.Now().UTC()
	// Capture user metadata headers (x-amz-meta-*). The SDK lowercases
	// header keys via canonicalisation but preserves the suffix; we store
	// them with the lowercased suffix so lookups are case-insensitive.
	md := map[string]string{}
	for h, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(h), "x-amz-meta-") && len(vals) > 0 {
			md[strings.ToLower(strings.TrimPrefix(strings.ToLower(h), "x-amz-meta-"))] = vals[0]
		}
	}
	m.metadata[bucket][key] = md
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(body)))
	w.WriteHeader(http.StatusOK)
}

// --- CopyObject ---

func (m *mockS3) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey, copySource string) {
	src := strings.TrimPrefix(copySource, "/")
	srcBucket, srcKey, ok := splitBucketKey(src)
	if !ok {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "bad copy-source")
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	srcObjs, ok := m.objects[srcBucket]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "source bucket not found")
		return
	}
	content, ok := srcObjs[srcKey]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "source key not found")
		return
	}
	if _, ok := m.buckets[dstBucket]; !ok {
		m.buckets[dstBucket] = time.Now().UTC()
	}
	if m.objects[dstBucket] == nil {
		m.objects[dstBucket] = map[string][]byte{}
		m.metadata[dstBucket] = map[string]map[string]string{}
		m.contentType[dstBucket] = map[string]string{}
		m.modTime[dstBucket] = map[string]time.Time{}
	}
	m.objects[dstBucket][dstKey] = content
	// Copy metadata from source.
	if srcMd, ok := m.metadata[srcBucket][srcKey]; ok {
		md := map[string]string{}
		for k, v := range srcMd {
			md[k] = v
		}
		m.metadata[dstBucket][dstKey] = md
	}
	m.contentType[dstBucket][dstKey] = m.contentType[srcBucket][srcKey]
	m.modTime[dstBucket][dstKey] = time.Now().UTC()

	type copyResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}
	writeXML(w, http.StatusOK, copyResult{
		ETag:         fmt.Sprintf(`"%x"`, md5.Sum(content)),
		LastModified: m.modTime[dstBucket][dstKey].UTC().Format(time.RFC3339),
	})
}

// splitBucketKey splits "bucket/key" into (bucket, key). The leading slash
// (already stripped) is not accepted.
func splitBucketKey(s string) (bucket, key string, ok bool) {
	idx := strings.IndexByte(s, '/')
	if idx <= 0 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}

// --- DeleteObject / DeleteObjects ---

func (m *mockS3) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[bucket]; !ok {
		// S3 returns NoSuchBucket; some stores return 204 anyway. We return
		// 204 to keep the v2 retryer from making this a hard failure.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	delete(m.objects[bucket], key)
	delete(m.metadata[bucket], key)
	delete(m.contentType[bucket], key)
	delete(m.modTime[bucket], key)
	w.WriteHeader(http.StatusNoContent)
}

// deleteRequest is the XML body of a DeleteObjects request. The namespace
// is declared on the root element by the SDK, so we accept it here.
type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func (m *mockS3) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	type deleted struct {
		Key string `xml:"Key"`
	}
	type errEntry struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	type result struct {
		XMLName xml.Name   `xml:"DeleteResult"`
		Deleted []deleted  `xml:"Deleted"`
		Errors  []errEntry `xml:"Error"`
	}
	res := result{}
	if _, ok := m.buckets[bucket]; ok {
		for _, o := range req.Objects {
			delete(m.objects[bucket], o.Key)
			delete(m.metadata[bucket], o.Key)
			delete(m.contentType[bucket], o.Key)
			delete(m.modTime[bucket], o.Key)
			// The SDK uses Quiet=true, so it does NOT parse <Deleted>
			// entries — only <Error> entries are surfaced. To make the
			// success path observable to MultiDelete's result channel
			// (which iterates output.Deleted), we emit one <Deleted>
			// entry per key regardless of Quiet. The SDK ignores the
			// extra entries when Quiet was requested.
			res.Deleted = append(res.Deleted, deleted{Key: o.Key})
		}
	} else {
		// Bucket missing — report errors per key.
		for _, o := range req.Objects {
			res.Errors = append(res.Errors, errEntry{
				Key:     o.Key,
				Code:    "NoSuchBucket",
				Message: "bucket does not exist",
			})
		}
	}
	writeXML(w, http.StatusOK, res)
}

// --- Multipart upload (simplified) ---

func (m *mockS3) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := fmt.Sprintf("upload-%d", time.Now().UnixNano())
	m.mu.Lock()
	defer m.mu.Unlock()
	md := map[string]string{}
	for h, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(h), "x-amz-meta-") && len(vals) > 0 {
			md[strings.ToLower(strings.TrimPrefix(strings.ToLower(h), "x-amz-meta-"))] = vals[0]
		}
	}
	m.multipart[uploadID] = &mockMultipart{
		bucket:   bucket,
		key:      key,
		metadata: md,
		parts:    map[int][]byte{},
		created:  time.Now().UTC(),
	}
	type result struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}
	writeXML(w, http.StatusOK, result{Bucket: bucket, Key: key, UploadID: uploadID})
}

func (m *mockS3) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mu, ok := m.multipart[uploadID]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchUpload", "upload not found")
		return
	}
	var partNum int
	if _, err := fmt.Sscanf(r.URL.Query().Get("partNumber"), "%d", &partNum); err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "bad part number")
		return
	}
	mu.parts[partNum] = body
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(body)))
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	// The CompleteMultipartUpload body lists parts; we ignore the XML and
	// just concatenate parts in numerical order.
	_, _ = io.Copy(io.Discard, r.Body)
	m.mu.Lock()
	defer m.mu.Unlock()
	mu, ok := m.multipart[uploadID]
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchUpload", "upload not found")
		return
	}
	nums := make([]int, 0, len(mu.parts))
	for n := range mu.parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	var buf bytes.Buffer
	for _, n := range nums {
		buf.Write(mu.parts[n])
	}
	content := buf.Bytes()
	if m.objects[bucket] == nil {
		m.objects[bucket] = map[string][]byte{}
		m.metadata[bucket] = map[string]map[string]string{}
		m.contentType[bucket] = map[string]string{}
		m.modTime[bucket] = map[string]time.Time{}
	}
	m.objects[bucket][key] = content
	m.metadata[bucket][key] = mu.metadata
	m.contentType[bucket][key] = r.Header.Get("Content-Type")
	m.modTime[bucket][key] = time.Now().UTC()
	delete(m.multipart, uploadID)
	type result struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}
	writeXML(w, http.StatusOK, result{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     fmt.Sprintf(`"%x"`, md5.Sum(content)),
	})
}

// --- helpers ---

// writeXML marshals v as XML and writes it with the given status.
func writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(v)
}

// writeS3Error renders a minimal S3 XML error body.
func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	type errBody struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	writeXML(w, status, errBody{Code: code, Message: message})
}

// --- test helpers ---

// newMockS3Server starts an httptest.Server backed by a fresh mockS3 and
// returns both. The server is registered for cleanup via t.Cleanup.
func newMockS3Server(t *testing.T) (*httptest.Server, *mockS3) {
	t.Helper()
	backend := newMockS3()
	srv := httptest.NewServer(backend)
	backend.srvURL = srv.URL
	t.Cleanup(srv.Close)
	return srv, backend
}

// newS3Store builds an S3Store pointing at the given httptest.Server using
// path-style addressing, anonymous credentials and a small retry budget.
// It is the shared fixture for the Stat/List/Put/Get/Copy/Delete/MultiDelete
// tests.
func newS3Store(t *testing.T, srv *httptest.Server, opts ...func(*S3Option)) *S3Store {
	t.Helper()
	opt := S3Option{
		Endpoint:         srv.URL,
		UsePathStyle:     true,
		NoSignRequest:    true,
		Region:           "us-east-1",
		MaxRetries:       1,
		UseListObjectsV1: false,
	}
	for _, fn := range opts {
		fn(&opt)
	}
	store, err := NewS3Client(context.Background(), opt)
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	return store
}

// putTestObject is a convenience that PUTs an object via the mock store's
// HTTP API. It is used to seed fixtures without going through the SDK.
func (m *mockS3) putTestObject(t *testing.T, bucket, key string, content []byte, metadata map[string]string) {
	t.Helper()
	url := fmt.Sprintf("%s/%s/%s", m.serverURL(), bucket, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("putTestObject: %v", err)
	}
	for k, v := range metadata {
		req.Header.Set("x-amz-meta-"+k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("putTestObject: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("putTestObject: status %d for %s/%s", resp.StatusCode, bucket, key)
	}
}

// serverURL returns the mock server's base URL. It is stored on the mockS3
// by newMockS3Server for the convenience of putTestObject.
func (m *mockS3) serverURL() string { return m.srvURL }

// makeBucket ensures the bucket exists in the mock backend.
func (m *mockS3) makeBucket(t *testing.T, bucket string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, m.serverURL()+"/"+bucket, nil)
	if err != nil {
		t.Fatalf("makeBucket: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("makeBucket: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		t.Fatalf("makeBucket: status %d", resp.StatusCode)
	}
}

// drainObjects collects every Object from ch into a slice, failing the test
// if any Object carries an Err.
func drainObjects(t *testing.T, ch <-chan *storage.Object) []*storage.Object {
	t.Helper()
	var out []*storage.Object
	for o := range ch {
		if o == nil {
			continue
		}
		out = append(out, o)
	}
	return out
}

// hexMD5 returns the hex-encoded MD5 of b, matching the unquoted ETag the
// mock server sets.
func hexMD5(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
