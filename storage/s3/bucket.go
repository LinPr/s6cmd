package s3store

import (
	"context"
	"errors"

	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ListBuckets lists the buckets in the current account, returning the
// storage.Bucket form so it satisfies the S3Extension interface.
func (s *S3Store) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	raw, err := s.ListBucketsRaw(ctx)
	if err != nil {
		return nil, err
	}
	buckets := make([]storage.Bucket, 0, len(raw))
	for _, b := range raw {
		buckets = append(buckets, storage.Bucket{
			CreationDate: aws.ToTime(b.CreationDate),
			Name:         aws.ToString(b.Name),
		})
	}
	return buckets, nil
}

// ListBucketsRaw returns the raw SDK Bucket slice. It is kept for callers
// (e.g. cmd/ls) that read the SDK's CreationDate / Name fields directly.
func (s *S3Store) ListBucketsRaw(ctx context.Context) ([]types.Bucket, error) {
	var buckets []types.Bucket
	paginator := s3.NewListBucketsPaginator(s.client, &s3.ListBucketsInput{})
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, output.Buckets...)
	}
	return buckets, nil
}

// BucketExists reports whether the given bucket exists and is owned by the
// caller. Errors other than NotFound are returned verbatim.
func (s *S3Store) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		return true, nil
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return false, nil
	}
	return false, err
}

// CreateBucket creates a bucket in the given region. When region is empty or
// "us-east-1", the CreateBucketConfiguration is omitted so S3 does not
// reject the request with InvalidLocationConstraint.
func (s *S3Store) CreateBucket(ctx context.Context, name, region string) error {
	if s.dryRun {
		return nil
	}
	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}
	if region != "" && region != defaultRegion {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	if _, err := s.client.CreateBucket(ctx, input); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		switch {
		case errors.As(err, &owned):
			return nil
		case errors.As(err, &exists):
			return nil
		}
		return err
	}
	return nil
}

// MakeBucket is the Storage-interface-compatible alias for CreateBucket.
func (s *S3Store) MakeBucket(ctx context.Context, name, region string) error {
	return s.CreateBucket(ctx, name, region)
}

// DeleteBucket deletes a bucket. The bucket must be empty.
func (s *S3Store) DeleteBucket(ctx context.Context, bucketName string) error {
	if s.dryRun {
		return nil
	}
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

// RemoveBucket is the Storage-interface-compatible alias for DeleteBucket.
func (s *S3Store) RemoveBucket(ctx context.Context, bucketName string) error {
	return s.DeleteBucket(ctx, bucketName)
}

// HeadBucket fetches bucket metadata. It returns storage.Bucket with the
// region populated from the HeadBucketOutput's BucketRegion header (which
// the SDK exposes on the v2 HeadBucketOutput type). Callers needing the
// raw SDK type can use HeadBucketOutput below.
func (s *S3Store) HeadBucket(ctx context.Context, bucketName string) (*storage.Bucket, error) {
	out, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, err
	}
	b := &storage.Bucket{Name: bucketName}
	if out != nil {
		b.Region = aws.ToString(out.BucketRegion)
	}
	return b, nil
}

// HeadBucketOutput returns the raw HeadBucketOutput for the given bucket.
// It is kept for callers (e.g. cmd/stat) that read the SDK's BucketRegion
// field directly.
func (s *S3Store) HeadBucketOutput(ctx context.Context, bucketName string) (*s3.HeadBucketOutput, error) {
	return s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
}

// SetBucketVersioning sets the versioning state of the bucket. status must be
// "Enabled" or "Suspended" (case-insensitive); the caller is responsible for
// normalizing case before calling when interfacing with users.
func (s *S3Store) SetBucketVersioning(ctx context.Context, status, bucket string) error {
	if s.dryRun {
		return nil
	}
	_, err := s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(status),
		},
	})
	return err
}

// GetBucketVersioning returns the versioning status of the bucket. The
// returned string is one of "Enabled", "Suspended" or "" (when the bucket
// has never been configured for versioning, the S3 API omits Status).
func (s *S3Store) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	out, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}
	if out == nil {
		return "", nil
	}
	return string(out.Status), nil
}
