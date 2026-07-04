// Package storage implements operations for s3 and fs.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store is an interface for storage operations that is common
// to local filesystem and remote object storage. It is implemented by
// both *s3store.S3Store and *fsstore.FileStore (verified at compile time
// in those packages). The aggregate Storage struct delegates to the
// appropriate Store based on the URL scheme.
//
// The interface is named Store (not Storage) because the aggregate
// dispatcher struct is already named Storage in this package; Go does not
// allow an interface and a struct to share a name.
type Store interface {
	// Stat returns the Object structure describing object. If src is not
	// found, ErrGivenObjectNotFound is returned.
	Stat(ctx context.Context, target *StorageURL) (*Object, error)

	// List the objects and directories/prefixes in the src.
	List(ctx context.Context, target *StorageURL, followSymlinks bool) <-chan *Object

	// Delete deletes the given src.
	Delete(ctx context.Context, target *StorageURL) error

	// MultiDelete deletes all items returned from given urls in batches.
	MultiDelete(ctx context.Context, urls <-chan *StorageURL) <-chan *Object

	// Copy src to dst, optionally setting the given metadata. Src and dst
	// arguments are of the same type. If src is a remote type, server side
	// copying will be used.
	Copy(ctx context.Context, src, dst *StorageURL, metadata Metadata) error
}

// S3Extension is the optional interface that S3-only stores implement. The
// aggregate Storage uses it to forward S3-specific operations (bucket
// management, multipart upload/download, presigning, ...) without having to
// import the concrete s3store package. This keeps the dependency direction
// one-way: storage/s3 imports storage, never the reverse.
type S3Extension interface {
	ListBuckets(ctx context.Context) ([]Bucket, error)
	MakeBucket(ctx context.Context, bucket, region string) error
	RemoveBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) (*Bucket, error)
	HeadObject(ctx context.Context, url *StorageURL) (*Object, *Metadata, error)
	Get(ctx context.Context, from *StorageURL, to io.WriterAt, concurrency int, partSize int64) (int64, error)
	Put(ctx context.Context, reader io.Reader, to *StorageURL, metadata Metadata, concurrency int, partSize int64) error
	Presign(ctx context.Context, url *StorageURL, expire time.Duration) (string, error)
	Read(ctx context.Context, src *StorageURL) (io.ReadCloser, error)
	Select(ctx context.Context, url *StorageURL, query *SelectQuery, resultCh chan<- json.RawMessage) error

	// Legacy stringly-typed helpers used by the backwards-compatible
	// wrappers on Storage.
	ListObjectKeysRecursive(ctx context.Context, bucket, prefix string) ([]string, error)
	DeleteObjects(ctx context.Context, bucket string, keys []string) error
	HeadObjectOutput(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error)

	// SetBucketVersioning sets the versioning state of the bucket
	// (Enabled/Suspended). The status string is forwarded as-is to the
	// S3 PutBucketVersioning API.
	SetBucketVersioning(ctx context.Context, status, bucket string) error
	// GetBucketVersioning returns the versioning status of the bucket
	// (Enabled/Suspended/"").
	GetBucketVersioning(ctx context.Context, bucket string) (string, error)
}

// SelectQuery is the parameter bundle passed to S3Extension.Select. It
// lets the call site (cmd/select) build it without having to import the
// s3 SDK types directly. The InputSerialization/OutputSerialization
// pointers are constructed by the s3store from the format/compression/
// header fields, which keeps the aws-sdk-go-v2 types out of the storage
// package's public surface.
type SelectQuery struct {
	// ExpressionType is "SQL" (the only value S3 supports). It is sent
	// verbatim to the SDK as ExpressionType.
	ExpressionType string
	// Expression is the SQL query string.
	Expression string
	// InputFormat is one of "json", "csv", "parquet".
	InputFormat string
	// InputContentStructure carries the JSON structure ("lines"/"document")
	// for JSON input, or the CSV delimiter for CSV input. It is ignored for
	// parquet.
	InputContentStructure string
	// FileHeaderInfo is the CSV FileHeaderInfo value ("USE"/"IGNORE"/"NONE").
	FileHeaderInfo string
	// OutputFormat is one of "json", "csv". When empty the input format is
	// used.
	OutputFormat string
	// CompressionType is "GZIP"/"BZIP2"/"NONE"/"". Empty means "NONE" on
	// the wire.
	CompressionType string
}

// Metadata is the per-object metadata used by Copy/Put operations.
type Metadata struct {
	ACL                string
	CacheControl       string
	Expires            string
	StorageClass       string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	EncryptionMethod   string
	EncryptionKeyID    string

	UserDefined map[string]string

	// MetadataDirective is used to specify whether the metadata is copied from
	// the source object or replaced with metadata provided when copying S3
	// objects. If MetadataDirective is not set, it defaults to "COPY".
	Directive string
}

// ObjectType is the type of Object.
type ObjectType struct {
	mode os.FileMode
}

// NewObjectType constructs an ObjectType from an os.FileMode. It is the
// only way for sub-packages to build an ObjectType since the underlying
// field is unexported.
func NewObjectType(mode os.FileMode) ObjectType {
	return ObjectType{mode: mode}
}

// String returns the string representation of ObjectType.
func (o ObjectType) String() string {
	switch mode := o.mode; {
	case mode.IsRegular():
		return "file"
	case mode.IsDir():
		return "directory"
	case mode&os.ModeSymlink != 0:
		return "symlink"
	}
	return ""
}

// MarshalJSON returns the stringer of ObjectType as a marshalled json.
func (o ObjectType) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

// IsDir checks if the object is a directory.
func (o ObjectType) IsDir() bool {
	return o.mode.IsDir()
}

// IsSymlink checks if the object is a symbolic link.
func (o ObjectType) IsSymlink() bool {
	return o.mode&os.ModeSymlink != 0
}

// IsRegular checks if the object is a regular file.
func (o ObjectType) IsRegular() bool {
	return o.mode.IsRegular()
}

// StorageClass represents the storage used to store an object.
type StorageClass string

// Object is a generic type which contains metadata for storage items.
type Object struct {
	StorageURL   *StorageURL  `json:"key,omitempty"`
	Etag         string       `json:"etag,omitempty"`
	ModTime      *time.Time   `json:"last_modified,omitempty"`
	Type         ObjectType   `json:"type,omitempty"`
	Size         int64        `json:"size,omitempty"`
	StorageClass StorageClass `json:"storage_class,omitempty"`
	Err          error        `json:"error,omitempty"`
	retryID      string

	// the VersionID field exist only for JSON Marshall, it must not be used for
	// any other purpose. URL.VersionID must be used instead.
	VersionID string `json:"version_id,omitempty"`
}

// String returns the string representation of Object.
func (o *Object) String() string {
	if o.StorageURL != nil {
		return o.StorageURL.String()
	}
	return ""
}

// RetryID returns the per-upload retry id stamped on the object's metadata
// (s6cmd-upload-retry-id). It is populated by Stat when the S3Store is
// configured with NoSuchUploadRetryCount > 0 so Put's retry-on-NoSuchUpload
// loop can compare it against the id it sent. The accessor is unexported
// outside the storage package via SetRetryID to keep callers from stamping
// it themselves.
func (o *Object) RetryID() string {
	if o == nil {
		return ""
	}
	return o.retryID
}

// SetRetryID sets the per-upload retry id. It is exported so the s3 store
// (which lives in a separate package) can populate the field via the
// accessor without the storage package exporting a writable field. Tests
// can also use it to construct fixtures.
func (o *Object) SetRetryID(v string) {
	if o == nil {
		return
	}
	o.retryID = v
}

// Bucket is a container for storage objects.
type Bucket struct {
	CreationDate time.Time `json:"created_at"`
	Name         string    `json:"name"`
	// Region is the bucket's region, as returned by HeadBucket via the
	// BucketRegion response header. It is empty for backends that do not
	// report a region (e.g. ListBuckets on some S3-compatible services).
	Region string `json:"region,omitempty"`
}

// Storage is the aggregate dispatcher that holds both the remote (S3) and
// local (filesystem) stores. It implements the Store interface by
// dispatching to the appropriate backend based on the URL scheme, and also
// exposes S3-specific forwarding methods (ListBuckets/MakeBucket/HeadBucket/
// HeadObject/Get/Put/Presign) via the S3Extension interface.
//
// Note: the Store interface above and the Storage struct below share a
// name. This is intentional — the struct is the canonical implementation
// of the interface for the in-process aggregate, and the two are used
// together (the struct satisfies the interface, callers pass *Storage
// around). The interface exists primarily so the s3 and fs sub-packages
// can be tested against a mock without importing each other.
type Storage struct {
	remote Store
	local  Store

	// remoteS3 is non-nil when the remote backend is an S3Extension. It is
	// populated by NewStorage so the forwarding methods can call S3-specific
	// operations without a per-call type assertion.
	remoteS3 S3Extension
}

// NewStorage wraps the given remote and local Storage implementations into a
// single aggregate. Construction of the concrete stores (s3store.NewS3Client,
// fsstore.NewFileStore) lives in internal/cliutil so that the storage
// package never imports its sub-packages, avoiding an import cycle.
func NewStorage(remote, local Store) *Storage {
	s := &Storage{remote: remote, local: local}
	if ext, ok := remote.(S3Extension); ok {
		s.remoteS3 = ext
	}
	return s
}

// --- Store interface dispatch ---

// ClientFor returns the concrete Storage implementation for the given URL.
// Remote URLs dispatch to the S3 store, local URLs to the filesystem store.
func (s *Storage) ClientFor(url *StorageURL) Store {
	if url.IsRemote() {
		return s.remote
	}
	return s.local
}

// Stat retrieves metadata for the given target.
func (s *Storage) Stat(ctx context.Context, target *StorageURL) (*Object, error) {
	return s.ClientFor(target).Stat(ctx, target)
}

// List returns a channel of Objects matching the target.
func (s *Storage) List(ctx context.Context, target *StorageURL, followSymlinks bool) <-chan *Object {
	return s.ClientFor(target).List(ctx, target, followSymlinks)
}

// Delete removes the given target.
func (s *Storage) Delete(ctx context.Context, target *StorageURL) error {
	return s.ClientFor(target).Delete(ctx, target)
}

// MultiDelete deletes every URL read from the input channel in batches.
func (s *Storage) MultiDelete(ctx context.Context, urls <-chan *StorageURL) <-chan *Object {
	first, ok := <-urls
	if !ok {
		ch := make(chan *Object)
		close(ch)
		return ch
	}
	store := s.ClientFor(first)

	merged := make(chan *StorageURL)
	go func() {
		defer close(merged)
		merged <- first
		for u := range urls {
			merged <- u
		}
	}()

	return store.MultiDelete(ctx, merged)
}

// Copy copies src to dst, optionally applying metadata.
func (s *Storage) Copy(ctx context.Context, src, dst *StorageURL, metadata Metadata) error {
	if src.IsRemote() {
		return s.remote.Copy(ctx, src, dst, metadata)
	}
	return s.local.Copy(ctx, src, dst, metadata)
}

// --- S3Extension forwarding methods ---
//
// These forward to remoteS3 when it is non-nil. When the remote backend is
// not an S3Extension (e.g. a mock), they return an error indicating the
// operation is unsupported.

func (s *Storage) s3ext() (S3Extension, error) {
	if s.remoteS3 == nil {
		return nil, errors.New("s3 operations are not available on this storage backend")
	}
	return s.remoteS3, nil
}

// ListBuckets lists the buckets in the current account.
func (s *Storage) ListBuckets(ctx context.Context) ([]Bucket, error) {
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	return ext.ListBuckets(ctx)
}

// MakeBucket creates an S3 bucket. When region is "us-east-1" (or empty,
// which the SDK treats as us-east-1) the CreateBucketConfiguration is
// omitted so S3 returns no InvalidLocationConstraint error.
func (s *Storage) MakeBucket(ctx context.Context, bucket, region string) error {
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.MakeBucket(ctx, bucket, region)
}

// RemoveBucket deletes an S3 bucket. The bucket must be empty.
func (s *Storage) RemoveBucket(ctx context.Context, bucket string) error {
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.RemoveBucket(ctx, bucket)
}

// HeadBucket fetches bucket metadata.
func (s *Storage) HeadBucket(ctx context.Context, bucket string) (*Bucket, error) {
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	return ext.HeadBucket(ctx, bucket)
}

// HeadObject fetches object metadata for the given URL.
func (s *Storage) HeadObject(ctx context.Context, url *StorageURL) (*Object, *Metadata, error) {
	ext, err := s.s3ext()
	if err != nil {
		return nil, nil, err
	}
	return ext.HeadObject(ctx, url)
}

// Get downloads the object at the given URL into w using the multipart
// downloader with the requested concurrency and part size.
func (s *Storage) Get(ctx context.Context, from *StorageURL, to io.WriterAt, concurrency int, partSize int64) (int64, error) {
	ext, err := s.s3ext()
	if err != nil {
		return 0, err
	}
	return ext.Get(ctx, from, to, concurrency, partSize)
}

// Put uploads the given reader to the URL using the multipart uploader.
func (s *Storage) Put(ctx context.Context, reader io.Reader, to *StorageURL, metadata Metadata, concurrency int, partSize int64) error {
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.Put(ctx, reader, to, metadata, concurrency, partSize)
}

// Presign returns a presigned GET URL for the given object valid for expire.
func (s *Storage) Presign(ctx context.Context, url *StorageURL, expire time.Duration) (string, error) {
	ext, err := s.s3ext()
	if err != nil {
		return "", err
	}
	return ext.Presign(ctx, url, expire)
}

// Select runs an S3 Select query against the object at url, streaming each
// decoded record (a json.RawMessage) onto resultCh. It returns when the
// SelectObjectContent event stream is drained or an error occurs. The
// caller must drain resultCh concurrently to avoid blocking the SDK's
// event-stream reader goroutine.
func (s *Storage) Select(ctx context.Context, url *StorageURL, query *SelectQuery, resultCh chan<- json.RawMessage) error {
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.Select(ctx, url, query, resultCh)
}

// --- Backwards-compatible thin wrappers ---
//
// These exist so existing cmd/* call sites (which still pass bucket/key
// strings) keep compiling. New code should prefer the StorageURL-based
// methods above.

// DownloadFile downloads an S3 object to a local path (or "-" for stdout).
func (s *Storage) DownloadFile(ctx context.Context, bucketName, objectKey, localFile string) error {
	url, err := NewStorageURL("s3://" + bucketName + "/" + objectKey)
	if err != nil {
		return err
	}
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	if localFile == "" || localFile == "-" {
		resp, err := ext.Read(ctx, url)
		if err != nil {
			return err
		}
		defer resp.Close()
		_, err = io.Copy(os.Stdout, resp)
		return err
	}
	if err := os.MkdirAll(filePathDir(localFile), 0o755); err != nil {
		return err
	}
	f, err := os.Create(localFile)
	if err != nil {
		return err
	}
	defer f.Close()
	const defaultPartSize = 10 * 1024 * 1024
	_, err = ext.Get(ctx, url, f, manager.DefaultDownloadConcurrency, defaultPartSize)
	return err
}

// UploadFile uploads a local file to S3.
func (s *Storage) UploadFile(ctx context.Context, fileName, bucketName, objectKey string) (*manager.UploadOutput, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	url, err := NewStorageURL("s3://" + bucketName + "/" + objectKey)
	if err != nil {
		return nil, err
	}
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	if err := ext.Put(ctx, f, url, Metadata{}, manager.DefaultUploadConcurrency, manager.DefaultUploadPartSize); err != nil {
		return nil, err
	}
	return &manager.UploadOutput{Location: url.String()}, nil
}

// CopyS3Object performs a server-side CopyObject.
func (s *Storage) CopyS3Object(ctx context.Context, sourceBucket, sourceKey, destinationBucket, destinationKey string) error {
	src, err := NewStorageURL("s3://" + sourceBucket + "/" + sourceKey)
	if err != nil {
		return err
	}
	dst, err := NewStorageURL("s3://" + destinationBucket + "/" + destinationKey)
	if err != nil {
		return err
	}
	return s.remote.Copy(ctx, src, dst, Metadata{})
}

// ListS3Keys returns all object keys under (bucket, prefix) recursively.
// Kept for cmd/get/cp/rm/sync/du/tree/rb/mv which still use the stringly-typed
// API.
func (s *Storage) ListS3Keys(ctx context.Context, bucketName, prefix string) ([]string, error) {
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	return ext.ListObjectKeysRecursive(ctx, bucketName, prefix)
}

// DeleteS3Keys deletes the given keys from the bucket in batches of 1000.
func (s *Storage) DeleteS3Keys(ctx context.Context, bucketName string, objectKeys []string) error {
	if len(objectKeys) == 0 {
		return nil
	}
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.DeleteObjects(ctx, bucketName, objectKeys)
}

// DeleteBucket removes a bucket.
func (s *Storage) DeleteBucket(ctx context.Context, bucketName string) error {
	return s.RemoveBucket(ctx, bucketName)
}

// HeadObjectOutput returns the raw HeadObjectOutput for a bucket/key pair.
// Deprecated: use HeadObject with a *StorageURL.
func (s *Storage) HeadObjectOutput(ctx context.Context, bucketName, objectKey string) (*s3.HeadObjectOutput, error) {
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	return ext.HeadObjectOutput(ctx, bucketName, objectKey)
}

// SetBucketVersioning sets the versioning state of the bucket. status must be
// "Enabled" or "Suspended" (case-insensitive; callers should normalize).
func (s *Storage) SetBucketVersioning(ctx context.Context, status, bucket string) error {
	ext, err := s.s3ext()
	if err != nil {
		return err
	}
	return ext.SetBucketVersioning(ctx, status, bucket)
}

// GetBucketVersioning returns the versioning status of the bucket
// ("Enabled", "Suspended" or "").
func (s *Storage) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	ext, err := s.s3ext()
	if err != nil {
		return "", err
	}
	return ext.GetBucketVersioning(ctx, bucket)
}

// UploadFromStdin uploads os.Stdin to the given bucket/key.
func (s *Storage) UploadFromStdin(ctx context.Context, bucketName, objectKey string) (*manager.UploadOutput, error) {
	url, err := NewStorageURL("s3://" + bucketName + "/" + objectKey)
	if err != nil {
		return nil, err
	}
	ext, err := s.s3ext()
	if err != nil {
		return nil, err
	}
	stdinReader := &stdin{file: os.Stdin}
	if err := ext.Put(ctx, stdinReader, url, Metadata{}, manager.DefaultUploadConcurrency, manager.DefaultUploadPartSize); err != nil {
		return nil, err
	}
	return &manager.UploadOutput{Location: url.String()}, nil
}

// stdin adapts os.File to io.Reader so the SDK does not attempt to Seek on
// stdin (which is not seekable).
type stdin struct {
	file *os.File
}

func (s *stdin) Read(p []byte) (n int, err error) { return s.file.Read(p) }

// filePathDir is a tiny helper to avoid importing path/filepath here.
func filePathDir(p string) string {
	if p == "" {
		return ""
	}
	idx := strings.LastIndexByte(p, '/')
	if idx < 0 {
		return "."
	}
	if idx == 0 {
		return "/"
	}
	return p[:idx]
}
