package du

const du_examples = `Example 1: Show disk usage of all objects in a bucket

         s6cmd du s3://bucket/

Example 2: Show disk usage of all objects that match a wildcard, grouped by storage class

         s6cmd du --group s3://bucket/prefix/obj*.gz

Example 3: Show disk usage of all objects in a bucket but exclude py files

         s6cmd du --exclude "*.py" s3://bucket/

Example 4: Human-readable sizes

         s6cmd du --humanize s3://bucket/
`
