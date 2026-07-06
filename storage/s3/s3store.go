package s3store

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	// feature/s3/manager is deprecated in favour of feature/s3/transfermanager,
	// but the replacement is still v0.x and lacks Range-request support and
	// GetBucketRegion, so we deliberately stay on manager for now. Revisit the
	// migration once transfermanager reaches feature parity.
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// defaultRegion is used when no region can be inferred from the user, the
// environment or the bucket itself. AWS treats us-east-1 as the canonical
// default region.
const defaultRegion = "us-east-1"

// transferAccelEndpoint is the Amazon S3 Transfer Acceleration endpoint.
// When the user supplies it we let the SDK own the endpoint (set the parsed
// URL back to the sentinel) and enable UseAccelerate.
const transferAccelEndpoint = "s3-accelerate.amazonaws.com"

// sentinelURL is the zero value of url.URL. parseEndpoint returns it for an
// empty input so downstream code can distinguish "no endpoint supplied"
// (AWS default) from a custom endpoint.
var sentinelURL = url.URL{}

// parseEndpoint parses the given endpoint URL. An empty endpoint yields the
// sentinel, signalling "use the AWS SDK default endpoint". A parse error is
// reported with the original input so the caller can surface a useful
// message.
func parseEndpoint(endpoint string) (url.URL, error) {
	if endpoint == "" {
		return sentinelURL, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return sentinelURL, fmt.Errorf("parse endpoint %q: %v", endpoint, err)
	}
	return *u, nil
}

// supportsTransferAcceleration reports whether endpoint points at the S3
// Transfer Acceleration hostname. When true the caller should set
// s3.Options.UseAccelerate = true and stop overriding the endpoint (the SDK
// derives the correct accelerate URL from the bucket name).
func supportsTransferAcceleration(endpoint url.URL) bool {
	return endpoint.Hostname() == transferAccelEndpoint
}

// S3Store is the storage.Storage implementation for S3.
type S3Store struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	presigner  *s3.PresignClient

	// dryRun, when true, makes mutating operations no-ops.
	dryRun bool
	// useListObjectsV1 selects the legacy ListObjects API instead of
	// ListObjectsV2 (useful for services that do not implement V2).
	useListObjectsV1 bool
	// requestPayerFlag, when non-empty, is sent as RequestPayer on every
	// request that supports it.
	requestPayerFlag string
	// noSuchUploadRetryCount caps the number of times Put retries an upload
	// that failed with NoSuchUpload. See Put/retryOnNoSuchUpload.
	noSuchUploadRetryCount int
}

// metadataKeyRetryID is the object metadata key that carries the per-upload
// retry id. On a NoSuchUpload error the store Stats the target and compares
// the value of this metadata to the id sent with the upload: a match means
// a previous attempt actually succeeded despite the error, so Put returns
// nil instead of retrying.
const metadataKeyRetryID = "s6cmd-upload-retry-id"

// classifyExtraRetryable classifies s6cmd's additional retry rules on top
// of the SDK defaults: the extra retryable error codes (InternalError,
// RequestTimeTooSkewed, SlowDown, plus "connection timed out" message
// matches) return TrueTernary, the token errors (ExpiredToken,
// ExpiredTokenException, InvalidToken) — which must NOT be retried even if
// the standard rules would allow it — return FalseTernary, and everything
// else returns UnknownTernary so the wrapped retryer decides.
func classifyExtraRetryable(err error) aws.Ternary {
	if err == nil {
		return aws.UnknownTernary
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return aws.UnknownTernary
	}
	switch apiErr.ErrorCode() {
	case "InternalError", "RequestTimeTooSkewed", "SlowDown":
		return aws.TrueTernary
	case "ExpiredToken", "ExpiredTokenException", "InvalidToken":
		return aws.FalseTernary
	}
	// "connection reset"/"connection timed out" are not separate
	// error codes; they appear inside the error message. The v2
	// standard retryer already handles "connection reset" via
	// RetryableConnectionError, so we only add the timed-out
	// variant here.
	if strings.Contains(apiErr.ErrorMessage(), "connection timed out") {
		return aws.TrueTernary
	}
	return aws.UnknownTernary
}

// newRetryer builds the SDK v2 retryer used when the user set an explicit
// --retry-count. It starts from retry.NewStandard (which already covers
// throttling, 5xx, RequestTimeout, connection errors, etc.) and layers on
// the classifyExtraRetryable rules.
//
// MaxAttempts is set via retry.StandardOptions. A non-positive max leaves
// the SDK default in place. Callers must construct a fresh retryer per
// client (aws.Retryer implementations carry token-bucket state that must
// not be shared).
func newRetryer(max int) aws.Retryer {
	std := retry.NewStandard(func(o *retry.StandardOptions) {
		if max > 0 {
			o.MaxAttempts = max
		}
		// Append the extra retryable codes. StandardOptions already
		// seeds Retryables with DefaultRetryables, so we append rather
		// than replace.
		o.Retryables = append(o.Retryables, retry.IsErrorRetryableFunc(classifyExtraRetryable))
	})
	return std
}

// extendedRetryer wraps the config-resolved retryer (which already honours
// AWS_MAX_ATTEMPTS / AWS_RETRY_MODE / shared-config max_attempts) with the
// classifyExtraRetryable rules. It is installed when the user did NOT set
// an explicit --retry-count, so the SDK's own env/config retry resolution
// stays authoritative for the attempt budget while the s6cmd-specific
// retryable codes and the token-error deny-list still apply.
type extendedRetryer struct {
	aws.Retryer
}

func (r *extendedRetryer) IsErrorRetryable(err error) bool {
	switch classifyExtraRetryable(err) {
	case aws.TrueTernary:
		return true
	case aws.FalseTernary:
		return false
	}
	return r.Retryer.IsErrorRetryable(err)
}

// resolveUsePathStyle implements the addressing policy:
//
//   - An explicit --path-style (true or false, from flag/env/config —
//     option.PathStyleExplicit) always wins. Explicit false keeps
//     virtual-host addressing for custom endpoints that support it
//     (e.g. Aliyun OSS).
//   - A custom endpoint with the flag unset defaults to path-style, the
//     s5cmd/mc behaviour: virtual-host addressing would inject the bucket
//     into the endpoint hostname ("mybucket.minio.local"), which almost
//     never resolves for MinIO-style named endpoints.
//   - No custom endpoint keeps the SDK default (virtual-host), unless the
//     caller opted into path-style programmatically.
func resolveUsePathStyle(option S3Option, customEndpoint bool) bool {
	if option.PathStyleExplicit {
		return option.UsePathStyle
	}
	if customEndpoint {
		return true
	}
	return option.UsePathStyle
}

// NewS3Client builds an S3Store from the given options. If Region is empty
// the SDK-resolved region is used, falling back to us-east-1.
//
// Addressing: see resolveUsePathStyle.
//   - true  → path-style (https://endpoint/bucket/key)
//   - false → virtual-host (https://bucket.endpoint/key), the AWS default
//
// A custom endpoint is installed as s3.Options.BaseEndpoint; the addressing
// style above applies to custom endpoints too (with UsePathStyle=false the
// SDK injects the bucket into the endpoint hostname).
//
// A transfer-acceleration endpoint enables s3.Options.UseAccelerate and the
// SDK owns the endpoint (the parsed URL is reset to the sentinel).
func NewS3Client(ctx context.Context, option S3Option) (*S3Store, error) {
	endpointURL, err := parseEndpoint(option.Endpoint)
	if err != nil {
		return nil, err
	}

	// Transfer acceleration: let the SDK own the endpoint. Keeping the
	// parsed accelerate URL would cause bucket operations to fail because
	// the SDK derives the accelerate hostname itself from the bucket name.
	useAccelerate := supportsTransferAcceleration(endpointURL)
	if useAccelerate {
		endpointURL = sentinelURL
	}

	var optFns []func(*config.LoadOptions) error
	if option.Region != "" {
		optFns = append(optFns, config.WithRegion(option.Region))
	}
	// NoSignRequest sends anonymous requests, so credential sources are
	// irrelevant: skip the profile / shared-credentials wiring entirely.
	// An env-resolved AWS_PROFILE pointing at a missing profile would
	// otherwise fail config loading even though no credentials are needed.
	if option.Profile != "" && !option.NoSignRequest {
		optFns = append(optFns, config.WithSharedConfigProfile(option.Profile))
	}
	if option.CredentialFile != "" && !option.NoSignRequest {
		optFns = append(optFns, config.WithSharedConfigFiles([]string{option.CredentialFile}))
	}
	// Retry wiring. Only an explicit --retry-count (>0) replaces the SDK
	// retryer wholesale: config.WithRetryer would otherwise silently
	// disable the SDK's own AWS_MAX_ATTEMPTS / AWS_RETRY_MODE /
	// shared-config max_attempts resolution. When the flag is unset
	// (MaxRetries<=0), the config-resolved retryer is kept and the extra
	// retryable codes + token-error deny-list are layered on top in the
	// s3.NewFromConfig options closure below. The closure returns a fresh
	// retryer per invocation because aws.Retryer carries token-bucket
	// state that must not be shared between clients.
	if option.MaxRetries > 0 {
		optFns = append(optFns, config.WithRetryer(func() aws.Retryer { return newRetryer(option.MaxRetries) }))
	}

	// HTTP client: always start from the SDK's BuildableClient so the SDK
	// connection-pool tuning survives (a bare http.DefaultTransport clone
	// keeps MaxIdleConnsPerHost=2, which cripples concurrent transfers).
	// No overall client timeout — large transfers can legitimately run for
	// hours — but cap the wait for response headers.
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxIdleConns = 100
		tr.MaxIdleConnsPerHost = 100
		tr.ResponseHeaderTimeout = 30 * time.Second
		if option.NoVerifySSL {
			if tr.TLSClientConfig == nil {
				tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
			} else {
				tr.TLSClientConfig.InsecureSkipVerify = true
			}
		}
	})
	optFns = append(optFns, config.WithHTTPClient(httpClient))

	conf, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}

	if conf.Region == "" {
		conf.Region = defaultRegion
	}

	client := s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = resolveUsePathStyle(option, endpointURL != sentinelURL)
		o.UseAccelerate = useAccelerate || option.UseAccelerate
		switch {
		case option.MaxRetries > 0:
			// Explicit --retry-count: pin RetryMaxAttempts too, so an
			// ambient AWS_MAX_ATTEMPTS cannot re-wrap the retryer with a
			// different budget in finalizeRetryMaxAttempts — the explicit
			// flag has the highest priority.
			o.RetryMaxAttempts = option.MaxRetries
		default:
			// Flag unset: keep the config-resolved retryer (env / shared
			// config attempt budget and mode) and layer the extra
			// retryable codes + token-error deny-list on top.
			o.Retryer = &extendedRetryer{Retryer: o.Retryer}
		}
		if option.NoSignRequest {
			o.Credentials = aws.AnonymousCredentials{}
		}
		// Custom endpoint (non-accelerate): BaseEndpoint replaces the
		// deprecated EndpointResolverWithOptions. Virtual-host addressing
		// (UsePathStyle=false) injects the bucket into the endpoint
		// hostname; path-style keeps the host as-is with /bucket/key.
		if endpointURL != sentinelURL {
			o.BaseEndpoint = aws.String(endpointURL.String())
			// S3-compatible services often reject the SDK's default CRC32
			// request checksums, so only checksum when the operation
			// requires it. AWS-default behavior is kept when no custom
			// endpoint is configured.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})

	uploader := manager.NewUploader(client)
	downloader := manager.NewDownloader(client)
	presigner := s3.NewPresignClient(client)

	return &S3Store{
		client:                 client,
		uploader:               uploader,
		downloader:             downloader,
		presigner:              presigner,
		dryRun:                 option.DryRun,
		useListObjectsV1:       option.UseListObjectsV1,
		requestPayerFlag:       option.RequestPayer,
		noSuchUploadRetryCount: option.NoSuchUploadRetryCount,
	}, nil
}

// requestPayer returns the RequestPayer value to send on supporting
// requests, or the zero value (omitted by the SDK) when unset.
func (s *S3Store) requestPayer() types.RequestPayer {
	if s.requestPayerFlag == "" {
		return ""
	}
	return types.RequestPayer(s.requestPayerFlag)
}

// Client returns the underlying S3 client. It is exposed so that commands
// needing direct access to the paginator constructors (e.g. cmd/du building a
// ListObjectsV2Paginator in-place to accumulate Size fields) can do so without
// each command re-implementing the client configuration. Callers must not
// mutate the client.
func (s *S3Store) Client() *s3.Client {
	return s.client
}

// Credentials is a small static-credentials helper used by callers that
// build their own config (kept for backwards compatibility).
type Credentials struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

func (c *Credentials) GetAccessKeyID() string     { return c.AccessKeyId }
func (c *Credentials) GetAccessKeySecret() string { return c.AccessKeySecret }
func (c *Credentials) GetSecurityToken() string   { return c.SecurityToken }

// NewAwsS3Provider wraps the given credentials as a static credentials
// provider.
func NewAwsS3Provider(credential *Credentials) credentials.StaticCredentialsProvider {
	return credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     credential.AccessKeyId,
			SecretAccessKey: credential.AccessKeySecret,
			SessionToken:    credential.SecurityToken,
		},
	}
}

// NewEnvironmentVariableCredentials reads OSS_* env vars and returns a
// Credentials value. It is kept for legacy callers.
func NewEnvironmentVariableCredentials() (*Credentials, error) {
	accessID := os.Getenv("OSS_ACCESS_KEY_ID")
	if accessID == "" {
		return nil, errors.New("access key id is empty")
	}
	accessKey := os.Getenv("OSS_ACCESS_KEY_SECRET")
	if accessKey == "" {
		return nil, errors.New("access key secret is empty")
	}
	token := os.Getenv("OSS_SESSION_TOKEN")
	return &Credentials{
		AccessKeyId:     accessID,
		AccessKeySecret: accessKey,
		SecurityToken:   token,
	}, nil
}

// errNotFound reports whether err is an S3 NotFound / NoSuchKey response.
func errNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var noKey *types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	// some S3-compatible services return a generic smithy.APIError with
	// code "NotFound" or "NoSuchKey" instead of the typed variant.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

// statObjectNotFound returns the canonical ErrGivenObjectNotFound for the
// given URL when the underlying Head/Get returned a not-found error.
func statObjectNotFound(url *storage.StorageURL, err error) error {
	if err == nil {
		return nil
	}
	if errNotFound(err) {
		return errorpkg.ErrGivenObjectNotFound
	}
	return err
}

// trimEtag strips the surrounding quotes that S3 wraps around ETag values.
func trimEtag(v string) string { return strings.Trim(v, `"`) }

// withContentMD5 is a per-call option that replaces the SDK's flexible
// checksum middleware (which sends x-amz-checksum-crc32) with the legacy
// Content-MD5 header. DeleteObjects REQUIRES a payload checksum, and several
// S3-compatible services (Aliyun OSS, older MinIO/Ceph) only accept
// Content-MD5 there, rejecting the CRC32 header with MissingArgument.
// Amazon S3 accepts Content-MD5 on DeleteObjects too (it was the required
// header for years), so applying it unconditionally is safe for both.
func withContentMD5(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		// Best-effort removal: the IDs are stable in the vendored SDK, but a
		// missing middleware just means there is nothing to replace.
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
}
