package uri

import (
	"fmt"
	"strings"
)

type S3Url struct {
	scheme string
	bucket string
	key    string
}

func (rp S3Url) GetScheme() string {
	return rp.scheme
}

func (rp S3Url) GetKey() string {
	return rp.key
}

func (rp S3Url) GetBucket() string {
	return rp.bucket
}

func ParseS3Url(s string) (*S3Url, error) {
	scheme, rest, found := strings.Cut(s, "://")
	if !found {
		return nil, fmt.Errorf("invalid s3 url: %s", s)
	}
	if scheme != "s3" {
		return nil, fmt.Errorf("invalid scheme: %s", scheme)
	}
	bucket, key, _ := strings.Cut(rest, "/")

	return &S3Url{
		scheme: scheme,
		bucket: bucket,
		key:    key,
	}, nil
}
