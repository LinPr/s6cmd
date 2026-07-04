package presign

const presign_examples = `Example 1: Print a presigned URL for a remote object

         s6cmd presign s3://bucket/prefix/object

Example 2: Print a presigned URL with a custom expiration

         s6cmd presign --expire 24h s3://bucket/prefix/object

Example 3: Print a presigned URL for a specific object version

         s6cmd presign --version-id VERSION_ID s3://bucket/prefix/object
`
