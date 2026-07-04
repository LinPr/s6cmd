package mv

const mv_examples = `Example 1: Move a single object to another bucket

         s6cmd mv s3://bucket/object.txt s3://other-bucket/object.txt

Example 2: Move all objects under a prefix recursively

         s6cmd mv --recursive s3://bucket/prefix/ s3://other-bucket/prefix/

Example 3: Move local files to S3

         s6cmd mv --recursive ./local-dir/ s3://bucket/prefix/
`
