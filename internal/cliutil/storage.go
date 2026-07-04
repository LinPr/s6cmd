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
	local := fsstore.NewFileStore(ctx, fsstore.LocalOption{DryRun: false})
	return storage.NewStorage(remote, local), nil
}

// NewS3Client returns the bare S3Store. It is kept for cmd/ls/mb/stat which
// still call S3 methods directly. New code should use NewStorage + the
// forwarding methods on *storage.Storage instead.
func NewS3Client(ctx context.Context, flags CommonFlags) (*s3store.S3Store, error) {
	return s3store.NewS3Client(ctx, s3OptFromFlags(flags))
}

// s3OptFromFlags translates the shared CommonFlags into an S3Option.
//
// AddressingStyle takes precedence over the legacy PathStyle flag. When the
// user only set --path-style (no --addressing-style), we map it onto
// AddressingStyle=path so the rest of the code only has one knob to read.
func s3OptFromFlags(flags CommonFlags) s3store.S3Option {
	addressing := flags.AddressingStyle
	if addressing == "" && flags.PathStyle {
		addressing = s3store.AddressingStylePath
	}
	return s3store.S3Option{
		AddressingStyle:        addressing,
		UsePathStyle:           flags.PathStyle, // kept for callers that still read it directly
		Region:                 flags.Region,
		Profile:                flags.Profile,
		Endpoint:               flags.EndpointURL,
		NoVerifySSL:            flags.NoVerifySSL,
		MaxRetries:             flags.RetryCount,
		NoSuchUploadRetryCount: flags.NoSuchUploadRetryCount,
		CredentialFile:         flags.CredentialsFile,
		NoSignRequest:          flags.NoSignRequest,
	}
}
