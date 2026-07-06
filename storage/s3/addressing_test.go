package s3store

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestParseEndpoint covers the empty/sentinel path, valid URLs and the
// well-known special endpoints. parseEndpoint is the foundation of the
// addressing decision; anything it returns flows into
// supportsTransferAcceleration.
func TestParseEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		wantSent bool // expect the sentinel (zero) URL?
		wantHost string
		wantErr  bool
	}{
		{name: "empty", endpoint: "", wantSent: true},
		{name: "local minio", endpoint: "http://localhost:9000", wantHost: "localhost"},
		{name: "minio with path", endpoint: "http://minio.local:9000", wantHost: "minio.local"},
		{name: "transfer accel", endpoint: "https://s3-accelerate.amazonaws.com", wantHost: "s3-accelerate.amazonaws.com"},
		{name: "gcs", endpoint: "https://storage.googleapis.com", wantHost: "storage.googleapis.com"},
		{name: "custom oss", endpoint: "https://oss-cn-hangzhou.aliyuncs.com", wantHost: "oss-cn-hangzhou.aliyuncs.com"},
		{name: "garbage", endpoint: "://not-a-url", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseEndpoint(tc.endpoint)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
				if got != sentinelURL {
					t.Fatalf("on error parseEndpoint should return the sentinel, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSent {
				if got != sentinelURL {
					t.Fatalf("expected sentinel URL for empty endpoint, got %v", got)
				}
				return
			}
			if got.Hostname() != tc.wantHost {
				t.Fatalf("hostname: want %q, got %q", tc.wantHost, got.Hostname())
			}
		})
	}
}

// TestSupportsTransferAcceleration locks the hostname match for the
// accelerate endpoint. Any other hostname (including AWS regional S3) must
// return false so the SDK keeps the user-supplied endpoint.
func TestSupportsTransferAcceleration(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"https://s3-accelerate.amazonaws.com": true,
		"http://s3-accelerate.amazonaws.com":  true,
		// Regional S3 endpoints are NOT accelerate endpoints.
		"https://s3.us-east-1.amazonaws.com": false,
		"https://s3.amazonaws.com":           false,
		// Custom endpoints are not accelerate.
		"http://localhost:9000":                false,
		"https://storage.googleapis.com":       false,
		"https://oss-cn-hangzhou.aliyuncs.com": false,
	}
	for endpoint, want := range cases {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			u, err := url.Parse(endpoint)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := supportsTransferAcceleration(*u)
			if got != want {
				t.Fatalf("supportsTransferAcceleration(%s) = %v, want %v", endpoint, got, want)
			}
		})
	}

	// The sentinel (empty endpoint) must NOT be flagged as accelerate.
	if supportsTransferAcceleration(sentinelURL) {
		t.Fatal("sentinel URL must not report transfer acceleration")
	}
}

// TestResolveUsePathStyle pins the addressing policy decision table: an
// explicit --path-style always wins, an unset flag defaults to path-style
// with a custom endpoint (s5cmd/mc behaviour) and to the SDK default
// (virtual-host) otherwise.
func TestResolveUsePathStyle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		usePathStyle   bool
		explicit       bool
		customEndpoint bool
		want           bool
	}{
		{name: "no-endpoint-unset", want: false},
		{name: "no-endpoint-programmatic-true", usePathStyle: true, want: true},
		{name: "no-endpoint-explicit-true", usePathStyle: true, explicit: true, want: true},
		{name: "no-endpoint-explicit-false", explicit: true, want: false},
		{name: "endpoint-unset-defaults-to-path", customEndpoint: true, want: true},
		{name: "endpoint-explicit-true", usePathStyle: true, explicit: true, customEndpoint: true, want: true},
		{name: "endpoint-explicit-false-keeps-virtual-host", explicit: true, customEndpoint: true, want: false},
		{name: "endpoint-programmatic-true", usePathStyle: true, customEndpoint: true, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt := S3Option{UsePathStyle: tc.usePathStyle, PathStyleExplicit: tc.explicit}
			if got := resolveUsePathStyle(opt, tc.customEndpoint); got != tc.want {
				t.Fatalf("resolveUsePathStyle(explicit=%v, usePathStyle=%v, customEndpoint=%v) = %v, want %v",
					tc.explicit, tc.usePathStyle, tc.customEndpoint, got, tc.want)
			}
		})
	}
}

// captureHTTPClient is an aws.HTTPClient that records the request it was
// asked to send and returns a canned 200 response without touching the
// network. It lets the request-shape tests below use a HOSTNAME endpoint
// (which never resolves in DNS) safely.
type captureHTTPClient struct {
	req *http.Request
}

func (c *captureHTTPClient) Do(r *http.Request) (*http.Response, error) {
	c.req = r
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    r,
	}, nil
}

// TestNewS3Client_AddressingRequestShape asserts the actual request shape
// produced for a custom HOSTNAME endpoint (not an IP, so bucket-in-host
// rewrites are observable):
//
//   - flag unset → path-style: the bucket goes into the URL path and the
//     endpoint host is left untouched (the pre-BaseEndpoint HEAD behaviour
//     that MinIO-style named endpoints depend on);
//   - explicit --path-style=false → virtual-host: the SDK injects the
//     bucket into the endpoint hostname.
//
// The per-operation HTTPClient override captures the request before any
// network I/O, so the unresolvable hostname is never dialed.
func TestNewS3Client_AddressingRequestShape(t *testing.T) {
	isolateAWSEnv(t)

	const endpoint = "http://minio.test:9000"
	const bucket, key = "mybucket", "obj.txt"

	cases := []struct {
		name     string
		option   S3Option
		wantHost string
		wantPath string
	}{
		{
			name: "custom endpoint, flag unset: bucket in path, host untouched",
			option: S3Option{
				Endpoint:      endpoint,
				NoSignRequest: true,
				Region:        "us-east-1",
				MaxRetries:    1,
			},
			wantHost: "minio.test:9000",
			wantPath: "/" + bucket + "/" + key,
		},
		{
			name: "custom endpoint, explicit --path-style=false: bucket in host",
			option: S3Option{
				Endpoint:          endpoint,
				UsePathStyle:      false,
				PathStyleExplicit: true,
				NoSignRequest:     true,
				Region:            "us-east-1",
				MaxRetries:        1,
			},
			wantHost: bucket + ".minio.test:9000",
			wantPath: "/" + key,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewS3Client(context.Background(), tc.option)
			if err != nil {
				t.Fatalf("NewS3Client: %v", err)
			}
			capture := &captureHTTPClient{}
			_, err = store.Client().HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}, func(o *s3.Options) { o.HTTPClient = capture })
			if err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			if capture.req == nil {
				t.Fatal("no request captured")
			}
			if got := capture.req.URL.Host; got != tc.wantHost {
				t.Errorf("request host: want %q, got %q", tc.wantHost, got)
			}
			if got := capture.req.URL.Path; got != tc.wantPath {
				t.Errorf("request path: want %q, got %q", tc.wantPath, got)
			}
		})
	}
}
