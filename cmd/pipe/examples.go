package pipe

const pipe_examples = `Example 1: Stream stdin to a remote object

         echo "content" | gzip | s6cmd pipe s3://bucket/prefix/object.gz

Example 2: Attach arbitrary metadata to the streamed object

         cat flowers.png | gzip | s6cmd pipe --metadata "imageSize=6032x4032" s3://bucket/prefix/flowers.gz

Example 3: Download an object and stream it to a bucket

         curl https://example.com/index.html | s6cmd pipe s3://bucket/index.html

Example 4: Compress an object and stream it to a bucket

         gzip -c file | s6cmd pipe s3://bucket/file.gz

Example 5: Do not overwrite an existing object

         echo "content" | s6cmd pipe --no-clobber s3://bucket/prefix/object
`
