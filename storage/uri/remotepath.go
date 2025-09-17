package uri

import (
	"fmt"
	"strings"
)

type S3Uri struct {
	scheme string
	bucket string
	key    string
}

func (rp S3Uri) GetScheme() string {
	return rp.scheme
}

func (rp S3Uri) GetKey() string {
	return rp.key
}

func (rp S3Uri) GetBucket() string {
	return rp.bucket
}

func (rp S3Uri) GetPath() string {
	return fmt.Sprintf("%s/%s", rp.bucket, rp.key)
}

func ParseS3Uri(s string) (*S3Uri, error) {
	if !strings.HasPrefix(s, "s3://") {
		return nil, fmt.Errorf("s3 url must start with s3://")
	}

	scheme, s3path, _ := strings.Cut(s, "://")
	bucket, key, _ := strings.Cut(s3path, "/")

	return &S3Uri{
		scheme: scheme,
		bucket: bucket,
		key:    key,
	}, nil
}
