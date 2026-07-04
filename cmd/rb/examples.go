package rb

const rb_examples = `Example 1: Remove an empty bucket

         s6cmd rb s3://bucketname

Example 2: Remove a bucket after emptying it

         s6cmd rb --force s3://bucketname

Example 3: Dry-run — show what would be removed

         s6cmd rb --dryRun s3://bucketname
`
