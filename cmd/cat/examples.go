package cat

const cat_examples = `Example 1: Print a remote object's content to stdout

         s6cmd cat s3://bucket/prefix/object

Example 2: Print a specific version of a remote object to stdout

         s6cmd cat --version-id VERSION_ID s3://bucket/prefix/object

Example 3: Concatenate objects matching a prefix or wildcard and print to stdout

         s6cmd cat "s3://bucket/prefix/*"

Example 4: Print an object whose key contains glob characters

         s6cmd cat --raw "s3://bucket/prefix/file*.txt"
`
