package cliutil

import (
	"context"

	"github.com/LinPr/s6cmd/storage"
	fsstore "github.com/LinPr/s6cmd/storage/fs"
	s3store "github.com/LinPr/s6cmd/storage/s3"
)

// NewStorage builds the aggregate Storage from the common flags. It is the
// only place in the codebase that imports both storage/s3 and storage/fs,
// which is what breaks the would-be import cycle (storage/s3 imports
// storage; storage does not import storage/s3).
func NewStorage(ctx context.Context, flags CommonFlags) (*storage.Storage, error) {
	remote, err := s3store.NewS3Client(ctx, s3OptFromFlags(flags))
	if err != nil {
		return nil, err
	}
	local := fsstore.NewFileStore(ctx, fsstore.LocalOption{DryRun: flags.DryRun})
	st := storage.NewStorage(remote, local)
	// The aggregate's own dry-run flag guards the path-based convenience
	// wrappers (DownloadFile) that bypass the backends' no-ops.
	st.SetDryRun(flags.DryRun)
	return st, nil
}

// NewS3Client returns the bare S3Store. It is kept for cmd/ls/mb/stat which
// still call S3 methods directly. New code should use NewStorage + the
// forwarding methods on *storage.Storage instead.
func NewS3Client(ctx context.Context, flags CommonFlags) (*s3store.S3Store, error) {
	return s3store.NewS3Client(ctx, s3OptFromFlags(flags))
}

// s3OptFromFlags translates the shared CommonFlags into an S3Option.
// UsePathStyle/PathStyleExplicit drive the addressing policy: an explicit
// --path-style (true or false) wins, an unset flag defaults to path-style
// when a custom endpoint is configured (MinIO/OSS/COS/GCS) and to the AWS
// virtual-host default otherwise.
func s3OptFromFlags(flags CommonFlags) s3store.S3Option {
	return s3store.S3Option{
		UsePathStyle:           flags.PathStyle,
		PathStyleExplicit:      flags.PathStyleSet,
		DryRun:                 flags.DryRun,
		Region:                 flags.Region,
		Profile:                flags.Profile,
		Endpoint:               flags.EndpointURL,
		NoVerifySSL:            flags.NoVerifySSL,
		MaxRetries:             flags.RetryCount,
		NoSuchUploadRetryCount: flags.NoSuchUploadRetryCount,
		CredentialFile:         flags.CredentialsFile,
		NoSignRequest:          flags.NoSignRequest,
		UseListObjectsV1:       flags.UseListObjectsV1,
	}
}
