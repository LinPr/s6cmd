package head

const head_examples = `Example 1: Print a remote object's metadata as JSON

         s6cmd head s3://bucket/prefix/object

Example 2: Check if a remote bucket exists

         s6cmd head s3://bucket

Example 3: Print a remote object's metadata with a specific version

         s6cmd head --version-id VERSION_ID s3://bucket/prefix/object

Example 4: Print metadata for an object whose key contains glob characters

         s6cmd head --raw "s3://bucket/prefix/file*.txt"
`
