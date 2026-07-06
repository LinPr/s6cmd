package rb

const rb_examples = `Example 1: Remove an empty bucket

         s6cmd rb s3://bucketname

Example 2: Remove a bucket after emptying it (prompts unless --yes)

         s6cmd rb --force --yes s3://bucketname

Example 3: Dry-run — show what would be removed

         s6cmd rb --dry-run s3://bucketname
`
