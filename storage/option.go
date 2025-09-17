package storage

import (
	fsstore "github.com/LinPr/s6cmd/storage/fs"
	s3store "github.com/LinPr/s6cmd/storage/s3"
)

type StorageOption struct {
	s3Option    s3store.S3Option
	localOption fsstore.LocalOption
}

func NewStorageOption(s3opt s3store.S3Option, localopt fsstore.LocalOption) *StorageOption {
	return &StorageOption{
		s3Option:    s3opt,
		localOption: localopt,
	}
}
