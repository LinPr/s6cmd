package s3store

// S3Option stores configuration for the S3 storage backend. Field status is
// annotated so callers know what is wired up vs. what is still a TODO.
type S3Option struct {
	// Region is the AWS region to use. When empty, NewS3Client will attempt
	// to auto-detect via manager.GetBucketRegion if Bucket is set, and fall
	// back to us-east-1.
	//
	// Status: implemented.
	Region string

	// UsePathStyle forces path-style addressing (s3.amazonaws.com/bucket
	// instead of bucket.s3.amazonaws.com). Required for MinIO and many
	// S3-compatible services.
	//
	// This field is kept for backwards compatibility. AddressingStyle takes
	// precedence when set: an empty AddressingStyle plus UsePathStyle=true
	// is treated as AddressingStyle="path". New callers should set
	// AddressingStyle directly.
	//
	// Status: implemented (legacy).
	UsePathStyle bool

	// Profile selects a named profile from the shared credentials file.
	//
	// Status: implemented.
	Profile string

	// Endpoint overrides the default service endpoint URL. Used for
	// MinIO/OSS/COS/GCS and other S3-compatible services.
	//
	// Status: implemented.
	Endpoint string

	// NoVerifySSL disables TLS certificate verification.
	//
	// Status: implemented.
	NoVerifySSL bool

	// DryRun makes mutating operations (Put/Copy/Delete/MakeBucket/...)
	// no-ops on the client side.
	//
	// Status: implemented.
	DryRun bool

	// NoSignRequest sends anonymous (unsigned) requests. Used for public
	// buckets.
	//
	// Status: implemented.
	NoSignRequest bool

	// UseListObjectsV1 selects the legacy ListObjects API instead of
	// ListObjectsV2. Some S3-compatible services do not implement V2.
	//
	// Status: implemented.
	UseListObjectsV1 bool

	// RequestPayer, when set, is sent as RequestPayer on every supporting
	// request to acknowledge requester-pays buckets.
	//
	// Status: implemented.
	RequestPayer string

	// MaxRetries is the maximum number of attempts the SDK retryer will
	// make for a retriable request. A non-positive value leaves the SDK
	// default (3) in place. The retryer is the v2 standard retryer
	// (retry.NewStandard) extended with extra retryable error codes
	// (InternalError, RequestTimeTooSkewed, SlowDown, plus
	// connection-reset/connection-timed-out string matches) and with the
	// token errors (ExpiredToken/ExpiredTokenException/InvalidToken)
	// explicitly excluded.
	//
	// Status: implemented.
	MaxRetries int

	// NoSuchUploadRetryCount enables retry-on-NoSuchUpload with the given
	// number of attempts. When the multipart uploader returns
	// NoSuchUpload, the store Stats the target and compares the
	// `s6cmd-upload-retry-id` metadata against the value that was sent
	// with the upload; a match means a previous attempt actually succeeded
	// (despite the error) and is treated as success, otherwise the upload is
	// retried up to NoSuchUploadRetryCount times. A non-positive value
	// disables the retry path; Put is a single attempt in that case.
	//
	// Status: implemented.
	NoSuchUploadRetryCount int

	// CredentialFile overrides the shared credentials/config file path the
	// SDK loads. It is wired through config.WithSharedConfigFiles so the
	// named file replaces both the default ~/.aws/credentials and
	// ~/.aws/config sources. Profile (when set) is still honoured via
	// config.WithSharedConfigProfile.
	//
	// Status: implemented.
	CredentialFile string

	// AddressingStyle selects "path", "virtual" or "auto" addressing.
	//   - "path": force path-style (https://endpoint/bucket/key).
	//   - "virtual": force virtual-host style (https://bucket.endpoint/key).
	//   - "auto" or "": endpoint-derived — AWS default endpoint uses
	//     virtual-host, custom endpoints use path-style.
	//
	// AddressingStyle takes precedence over the legacy UsePathStyle field.
	//
	// Status: implemented.
	AddressingStyle string

	// UseAccelerate enables S3 Transfer Acceleration. When nil/false the
	// value is auto-detected from the endpoint (an s3-accelerate.amazonaws.com
	// endpoint enables acceleration). Set explicitly to override.
	//
	// Status: implemented.
	UseAccelerate bool

	// bucket is an internal hint used by NewS3Client to probe the bucket
	// region via manager.GetBucketRegion when Region is empty. It is set
	// by higher-level constructors (e.g. storage.NewRemoteClient).
	bucket string
}
