package s3store

import (
	"net/url"
	"testing"
)

// TestParseEndpoint covers the empty/sentinel path, valid URLs and the
// well-known special endpoints. parseEndpoint is the foundation of the
// addressing-style decision; anything it returns flows into
// isVirtualHostStyle and supportsTransferAcceleration.
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
		"http://localhost:9000":                 false,
		"https://storage.googleapis.com":        false,
		"https://oss-cn-hangzhou.aliyuncs.com":  false,
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

// TestIsGoogleEndpoint pins the GCS hostname match.
func TestIsGoogleEndpoint(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"https://storage.googleapis.com": true,
		"http://storage.googleapis.com":  true,
		"https://s3.amazonaws.com":       false,
		"http://localhost:9000":          false,
	}
	for endpoint, want := range cases {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			u, err := url.Parse(endpoint)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := isGoogleEndpoint(*u)
			if got != want {
				t.Fatalf("isGoogleEndpoint(%s) = %v, want %v", endpoint, got, want)
			}
		})
	}

	if isGoogleEndpoint(sentinelURL) {
		t.Fatal("sentinel URL must not be flagged as GCS")
	}
}

// TestForcedVirtualHostStyle verifies that only the explicit "virtual"
// value forces virtual-host addressing; "path", "auto" and the empty
// string leave the decision to the endpoint-derived rule.
func TestForcedVirtualHostStyle(t *testing.T) {
	t.Parallel()

	custom, _ := url.Parse("http://localhost:9000")
	cases := []struct {
		name    string
		endpoint url.URL
		style   string
		want    bool
	}{
		{name: "virtual", endpoint: *custom, style: AddressingStyleVirtual, want: true},
		{name: "path", endpoint: *custom, style: AddressingStylePath, want: false},
		{name: "auto", endpoint: *custom, style: AddressingStyleAuto, want: false},
		{name: "empty style", endpoint: *custom, style: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := forcedVirtualHostStyle(tc.endpoint, tc.style)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestIsVirtualHostStyle is the load-bearing decision table. Every
// (endpoint × addressing-style) combination the production code can see is
// pinned here so regressions in the precedence logic surface as test
// failures, not runtime surprises against MinIO/AWS.
func TestIsVirtualHostStyle(t *testing.T) {
	t.Parallel()

	custom, _ := url.Parse("http://localhost:9000")
	gcs, _ := url.Parse("https://storage.googleapis.com")

	tests := []struct {
		name     string
		endpoint url.URL
		style    string
		want     bool
	}{
		// AWS default endpoint (sentinel): virtual-host unless "path" forced.
		{name: "sentinel+auto", endpoint: sentinelURL, style: AddressingStyleAuto, want: true},
		{name: "sentinel+empty", endpoint: sentinelURL, style: "", want: true},
		{name: "sentinel+path", endpoint: sentinelURL, style: AddressingStylePath, want: false},
		{name: "sentinel+virtual", endpoint: sentinelURL, style: AddressingStyleVirtual, want: true},

		// Custom endpoint: default path-style (MinIO/OSS/COS/GCS).
		{name: "custom+auto", endpoint: *custom, style: AddressingStyleAuto, want: false},
		{name: "custom+empty", endpoint: *custom, style: "", want: false},
		{name: "custom+path", endpoint: *custom, style: AddressingStylePath, want: false},
		{name: "custom+virtual", endpoint: *custom, style: AddressingStyleVirtual, want: true},

		// GCS endpoint: same as custom — path-style by default, virtual only
		// when explicitly forced.
		{name: "gcs+auto", endpoint: *gcs, style: AddressingStyleAuto, want: false},
		{name: "gcs+empty", endpoint: *gcs, style: "", want: false},
		{name: "gcs+path", endpoint: *gcs, style: AddressingStylePath, want: false},
		{name: "gcs+virtual", endpoint: *gcs, style: AddressingStyleVirtual, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isVirtualHostStyle(tc.endpoint, tc.style)
			if got != tc.want {
				t.Fatalf("isVirtualHostStyle(%v, %q) = %v, want %v",
					tc.endpoint, tc.style, got, tc.want)
			}
		})
	}
}

// TestNewS3ClientAddressingInvalid verifies that an unknown addressing style
// is rejected at client construction time rather than silently misrouted.
func TestNewS3ClientAddressingInvalid(t *testing.T) {
	t.Parallel()

	_, err := NewS3Client(t.Context(), S3Option{AddressingStyle: "bogus"})
	if err == nil {
		t.Fatal("expected an error for an invalid addressing style")
	}
}
