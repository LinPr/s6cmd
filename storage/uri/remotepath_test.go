package uri_test

import (
	"testing"

	"github.com/LinPr/s6cmd/storage/uri"
)

func TestParseS3Uri(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		s string
	}{
		{
			s: "s3://my-bucket/my-key",
		}, {
			s: "s3://my-bucket/my-key/with/slash",
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := uri.ParseS3Uri(tt.s)
			if (err != nil) != false {
				t.Errorf("ParseS3Uri() error = %v, wantErr %v", err, false)
				return
			}
			t.Logf("got: %v", got)
		})
	}
}
