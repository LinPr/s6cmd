package run

const run_examples = `Example 1: Run the commands declared in a file in parallel

         s6cmd run commands.txt

Example 2: Read commands from standard input and execute in parallel

         cat commands.txt | s6cmd run

Example 3: A commands file with comments and blank lines

         # list buckets
         ls

         # copy a file
         cp local.txt s3://bucket/remote.txt
`
