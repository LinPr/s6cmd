package s3store

// S3Option stores configuration for the S3 storage backend. Field status is
// annotated so callers know what is wired up vs. what is still a TODO.
type S3Option struct {
	// Region is the AWS region to use. When empty, the SDK-resolved region
	// (environment / shared config) is used, falling back to us-east-1.
	//
	// Status: implemented.
	Region string

	// UsePathStyle selects the S3 addressing style.
	//   - true:  force path-style addressing (https://endpoint/bucket/key).
	//           Required for MinIO, Alibaba OSS, Tencent COS, GCS and most
	//           S3-compatible services.
	//   - false: virtual-host addressing (https://bucket.endpoint/key),
	//           which is the AWS S3 default.
	//
	// When PathStyleExplicit is false (the user did not set --path-style
	// via flag/env/config) and a custom Endpoint is configured, path-style
	// is used regardless of this field — see resolveUsePathStyle.
	//
	// Status: implemented.
	UsePathStyle bool

	// PathStyleExplicit reports that UsePathStyle was set explicitly by
	// the user (command-line flag, environment variable or config file).
	// An explicit value — true or false — is forwarded to the SDK verbatim;
	// an unset flag defaults to path-style when a custom Endpoint is
	// configured (the s5cmd/mc behaviour) and to the SDK default
	// (virtual-host) otherwise.
	//
	// Status: implemented.
	PathStyleExplicit bool

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
	// make for a retriable request (--retry-count). A positive value
	// installs the v2 standard retryer (retry.NewStandard) with that
	// attempt budget, extended with extra retryable error codes
	// (InternalError, RequestTimeTooSkewed, SlowDown, plus
	// connection-timed-out string matches) and with the token errors
	// (ExpiredToken/ExpiredTokenException/InvalidToken) explicitly
	// excluded. A non-positive value keeps the SDK's own retry resolution
	// (AWS_MAX_ATTEMPTS / AWS_RETRY_MODE / shared-config max_attempts,
	// defaulting to 3 attempts) and layers the same extra codes and
	// deny-list on top of the resolved retryer.
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

	// UseAccelerate enables S3 Transfer Acceleration. When false the value
	// is auto-detected from the endpoint (an s3-accelerate.amazonaws.com
	// endpoint enables acceleration). Set explicitly to force it on.
	//
	// Status: implemented.
	UseAccelerate bool
}
