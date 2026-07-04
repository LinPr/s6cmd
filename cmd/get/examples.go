package get

const get_examples = `Example 1: Download a single object

         s6cmd get s3://bucket/object.txt ./local.txt

Example 2: Download all objects under a prefix recursively

         s6cmd get --recursive s3://bucket/prefix/ ./local-dir/

Example 3: Download with 8 concurrent workers

         s6cmd get --recursive --jobs 8 s3://bucket/prefix/ ./local-dir/
`
