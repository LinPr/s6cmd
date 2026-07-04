package cliutil

import (
	"context"

	"github.com/LinPr/s6cmd/storage"
)

// DeleteS3KeysInBatches deletes the given keys from the bucket in batches of
// 1000 (the S3 DeleteObjects limit). It is the canonical implementation used
// by rm/rb/sync; those commands previously each carried their own copy.
func DeleteS3KeysInBatches(ctx context.Context, store *storage.Storage, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := store.DeleteS3Keys(ctx, bucket, keys[i:end]); err != nil {
			return err
		}
	}
	return nil
}
