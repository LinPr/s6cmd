package put

const put_examples = `Example 1: Upload a single file

         s6cmd put ./local.txt s3://bucket/object.txt

Example 2: Upload a directory recursively

         s6cmd put --recursive ./local-dir/ s3://bucket/prefix/

Example 3: Upload from stdin

         s6cmd put - s3://bucket/object.txt
`
