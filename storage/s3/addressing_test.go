package s3store

import (
	"net/url"
	"testing"
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
