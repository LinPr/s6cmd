package rm

const rm_examples = `Example 1: Remove a single object

         s6cmd rm s3://bucket/object.txt

Example 2: Remove all objects under a prefix recursively

         s6cmd rm --recursive s3://bucket/prefix/

Example 3: Remove all objects matching a wildcard

         s6cmd rm "s3://bucket/prefix/*.tmp"

Example 4: Dry-run — show what would be removed

         s6cmd rm --dryRun --recursive s3://bucket/prefix/
`
