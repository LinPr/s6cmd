package sync

const sync_examples = `Example 1: Sync local files to S3

         s6cmd sync ./local-dir/ s3://bucket/prefix/

Example 2: Sync S3 to local, deleting extra destination files

         s6cmd sync --delete s3://bucket/prefix/ ./local-dir/

Example 3: Sync S3 to S3 with 8 concurrent workers

         s6cmd sync --jobs 8 s3://bucket/prefix/ s3://other-bucket/prefix/
`
