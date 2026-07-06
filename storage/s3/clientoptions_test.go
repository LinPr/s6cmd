package s3store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
)

// isolateAWSEnv makes NewS3Client hermetic for the duration of a test:
// config.LoadDefaultConfig reads the process environment and the shared
// config/credentials files, so a developer's real ~/.aws/config (e.g. a
// default profile with endpoint_url set) or AWS_ENDPOINT_URL* env vars
// would otherwise leak into the resolved client options. The shared config
// paths are pointed at nonexistent files under t.TempDir() and the
// endpoint/profile env vars are blanked.
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
	t.Setenv("AWS_MAX_ATTEMPTS", "")
	t.Setenv("AWS_RETRY_MODE", "")
	// Note: AWS_SDK_LOAD_CONFIG is a v1-only knob that SDK v2 ignores; the
	// isolation above comes from the nonexistent shared config files and
	// the blanked env vars, not from this line. It is kept only so a
	// v1-based tool spawned by a test would also be isolated.
	t.Setenv("AWS_SDK_LOAD_CONFIG", "0")
}

// newClientOptions builds a client from the given option and returns the
// resolved retryer for inspection. Region is pinned and NoSignRequest is
// forced so the tests never touch the user's shared credentials files.
func newClientOptions(t *testing.T, option S3Option) aws.Retryer {
	t.Helper()
	option.Region = "us-east-1"
	option.NoSignRequest = true
	store, err := NewS3Client(context.Background(), option)
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	return store.Client().Options().Retryer
}

// TestNewS3Client_BaseEndpoint pins that a custom endpoint is installed as
// s3.Options.BaseEndpoint (the modern replacement for the deprecated
// EndpointResolverWithOptions) and that no BaseEndpoint is set when the
// endpoint option is empty (the SDK owns the AWS default endpoint).
func TestNewS3Client_BaseEndpoint(t *testing.T) {
	isolateAWSEnv(t)

	const endpoint = "http://127.0.0.1:9000"
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:      endpoint,
		NoSignRequest: true,
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	got := store.Client().Options().BaseEndpoint
	if got == nil || *got != endpoint {
		t.Fatalf("BaseEndpoint: want %q, got %v", endpoint, got)
	}

	store, err = NewS3Client(context.Background(), S3Option{
		NoSignRequest: true,
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	if got := store.Client().Options().BaseEndpoint; got != nil {
		t.Fatalf("BaseEndpoint should be nil without a custom endpoint, got %q", *got)
	}
}

// TestNewS3Client_AnonymousCredentials verifies NoSignRequest results in
// unsigned requests, even when ambient credentials are present in the
// environment.
//
// NewS3Client sets o.Credentials = aws.AnonymousCredentials{} (the
// documented anonymous marker), but the resolved options cannot be asserted
// structurally: s3.New runs ignoreAnonymousAuth after all option functions,
// which maps AnonymousCredentials back to a nil provider so the auth
// resolver selects the smithy anonymous scheme. The observable contract is
// therefore behavioral: no Authorization header on the wire.
func TestNewS3Client_AnonymousCredentials(t *testing.T) {
	isolateAWSEnv(t)
	// Ambient credentials that NoSignRequest must override.
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")

	var lastAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := storage.NewStorageURL("s3://anon-bucket/key")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}

	// NoSignRequest=true: the request must carry no Authorization header.
	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:      srv.URL,
		UsePathStyle:  true,
		NoSignRequest: true,
		Region:        "us-east-1",
		MaxRetries:    1,
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	if _, err := store.Stat(context.Background(), u); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if lastAuth != "" {
		t.Errorf("NoSignRequest: request should be unsigned, got Authorization %q", lastAuth)
	}

	// Control: without NoSignRequest the ambient credentials sign the
	// request with SigV4.
	store, err = NewS3Client(context.Background(), S3Option{
		Endpoint:     srv.URL,
		UsePathStyle: true,
		Region:       "us-east-1",
		MaxRetries:   1,
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	if _, err := store.Stat(context.Background(), u); err != nil {
		t.Fatalf("Stat (signed): %v", err)
	}
	if lastAuth == "" {
		t.Errorf("signed control request is missing the Authorization header")
	}
}

// TestNewS3Client_ChecksumBehavior pins that request/response checksums are
// relaxed to "when required" only for custom endpoints (S3-compatible
// services often reject the SDK's CRC32 defaults) and left at the AWS
// default otherwise.
func TestNewS3Client_ChecksumBehavior(t *testing.T) {
	isolateAWSEnv(t)

	store, err := NewS3Client(context.Background(), S3Option{
		Endpoint:      "http://127.0.0.1:9000",
		NoSignRequest: true,
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	opts := store.Client().Options()
	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Errorf("custom endpoint: RequestChecksumCalculation = %v, want WhenRequired", opts.RequestChecksumCalculation)
	}
	if opts.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Errorf("custom endpoint: ResponseChecksumValidation = %v, want WhenRequired", opts.ResponseChecksumValidation)
	}

	store, err = NewS3Client(context.Background(), S3Option{
		NoSignRequest: true,
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	opts = store.Client().Options()
	if opts.RequestChecksumCalculation == aws.RequestChecksumCalculationWhenRequired {
		t.Errorf("default endpoint: RequestChecksumCalculation should keep the AWS default, got WhenRequired")
	}
	if opts.ResponseChecksumValidation == aws.ResponseChecksumValidationWhenRequired {
		t.Errorf("default endpoint: ResponseChecksumValidation should keep the AWS default, got WhenRequired")
	}
}

// TestNewS3Client_RetryerAlwaysInstalled verifies the s6cmd retry rules
// apply even at default settings (MaxRetries<=0): the extra retryable codes
// (SlowDown et al.) must be honoured and the token errors denied, layered
// on top of the config-resolved retryer rather than replacing it. It also
// pins the MaxAttempts wiring: SDK default (3) when MaxRetries<=0 and no
// env/config override, the explicit value otherwise.
func TestNewS3Client_RetryerAlwaysInstalled(t *testing.T) {
	isolateAWSEnv(t)

	r := newClientOptions(t, S3Option{}) // MaxRetries unset
	if r == nil {
		t.Fatal("Retryer should be installed at default settings")
	}
	if !r.IsErrorRetryable(&smithy.GenericAPIError{Code: "SlowDown", Message: "slow down"}) {
		t.Errorf("default retryer should retry SlowDown")
	}
	if r.IsErrorRetryable(&smithy.GenericAPIError{Code: "ExpiredToken", Message: "expired"}) {
		t.Errorf("default retryer must not retry ExpiredToken")
	}
	if got := r.MaxAttempts(); got != 3 {
		t.Errorf("MaxAttempts with MaxRetries unset: want SDK default 3, got %d", got)
	}

	r = newClientOptions(t, S3Option{MaxRetries: 7})
	if got := r.MaxAttempts(); got != 7 {
		t.Errorf("MaxAttempts with MaxRetries=7: want 7, got %d", got)
	}
	if !r.IsErrorRetryable(&smithy.GenericAPIError{Code: "SlowDown", Message: "slow down"}) {
		t.Errorf("explicit retryer should retry SlowDown")
	}
	if r.IsErrorRetryable(&smithy.GenericAPIError{Code: "ExpiredToken", Message: "expired"}) {
		t.Errorf("explicit retryer must not retry ExpiredToken")
	}
}

// TestNewS3Client_RetryerHonorsEnvWhenFlagUnset verifies that with
// --retry-count unset (MaxRetries<=0) the SDK's own retry resolution stays
// authoritative: AWS_MAX_ATTEMPTS / AWS_RETRY_MODE from the environment
// must reach the client, with the s6cmd extra retryable codes and the
// token-error deny-list layered on top. Installing config.WithRetryer
// unconditionally used to silently disable this resolution.
func TestNewS3Client_RetryerHonorsEnvWhenFlagUnset(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_MAX_ATTEMPTS", "7")
	t.Setenv("AWS_RETRY_MODE", "standard")

	r := newClientOptions(t, S3Option{}) // MaxRetries unset
	if got := r.MaxAttempts(); got != 7 {
		t.Errorf("MaxAttempts with AWS_MAX_ATTEMPTS=7 and flag unset: want 7, got %d", got)
	}
	// The layered rules must survive the env-resolved retryer.
	if !r.IsErrorRetryable(&smithy.GenericAPIError{Code: "SlowDown", Message: "slow down"}) {
		t.Errorf("env-resolved retryer should retry SlowDown")
	}
	if r.IsErrorRetryable(&smithy.GenericAPIError{Code: "ExpiredToken", Message: "expired"}) {
		t.Errorf("env-resolved retryer must not retry ExpiredToken")
	}
}

// TestNewS3Client_ExplicitRetryCountBeatsEnv verifies the priority order:
// an explicit --retry-count (MaxRetries>0) wins over an ambient
// AWS_MAX_ATTEMPTS, mirroring the flag>env>config precedence of every
// other option.
func TestNewS3Client_ExplicitRetryCountBeatsEnv(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_MAX_ATTEMPTS", "7")

	r := newClientOptions(t, S3Option{MaxRetries: 5})
	if got := r.MaxAttempts(); got != 5 {
		t.Errorf("MaxAttempts with --retry-count=5 and AWS_MAX_ATTEMPTS=7: want 5 (flag wins), got %d", got)
	}
}

// TestNewS3Client_RetryerNotShared verifies that two clients get distinct
// retryer instances (a shared retry.Standard would share token-bucket
// state across clients).
func TestNewS3Client_RetryerNotShared(t *testing.T) {
	isolateAWSEnv(t)

	a := newClientOptions(t, S3Option{MaxRetries: 5})
	b := newClientOptions(t, S3Option{MaxRetries: 5})
	if a == b {
		t.Fatal("each client should get a fresh retryer instance")
	}
}

// TestNewS3Client_UseAccelerateOption verifies the explicit UseAccelerate
// option is honoured, OR-ed with the endpoint auto-detection.
func TestNewS3Client_UseAccelerateOption(t *testing.T) {
	isolateAWSEnv(t)

	cases := []struct {
		name    string
		option  S3Option
		wantAcc bool
	}{
		{name: "default off", option: S3Option{}, wantAcc: false},
		{name: "explicit option", option: S3Option{UseAccelerate: true}, wantAcc: true},
		{name: "accelerate endpoint", option: S3Option{Endpoint: "https://s3-accelerate.amazonaws.com"}, wantAcc: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := tc.option
			opt.Region = "us-east-1"
			opt.NoSignRequest = true
			store, err := NewS3Client(context.Background(), opt)
			if err != nil {
				t.Fatalf("NewS3Client: %v", err)
			}
			if got := store.Client().Options().UseAccelerate; got != tc.wantAcc {
				t.Fatalf("UseAccelerate: want %v, got %v", tc.wantAcc, got)
			}
			// The accelerate endpoint must be handed back to the SDK, not
			// pinned as BaseEndpoint.
			if tc.option.Endpoint != "" && store.Client().Options().BaseEndpoint != nil {
				t.Fatalf("accelerate endpoint should not be pinned as BaseEndpoint")
			}
		})
	}
}
