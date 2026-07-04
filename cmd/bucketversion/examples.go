package bucketversion

const bucketversion_examples = `Example 1: Get the bucket versioning status

         s6cmd bucket-version s3://bucketname

Example 2: Enable bucket versioning

         s6cmd bucket-version --set Enabled s3://bucketname

Example 3: Suspend bucket versioning

         s6cmd bucket-version --set Suspended s3://bucketname
`
