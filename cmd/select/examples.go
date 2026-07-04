package selectcmd

const select_examples = `Example 1: Run a SQL query over a CSV file with a header row

         s6cmd select csv --use-header USE \
             --query "SELECT s.id FROM S3Object s WHERE s.price > 100" \
             s3://bucket/data.csv

Example 2: Query a TSV file by setting --delimiter to a tab

         s6cmd select csv --delimiter "\t" --use-header USE \
             --query "SELECT s.name FROM S3Object s" \
             s3://bucket/data.tsv

Example 3: Run a SQL query over a JSON-Lines file

         s6cmd select json --query "SELECT s.id FROM s3object s WHERE s.id > 10" \
             s3://bucket/data.jsonl

Example 4: Run a SQL query over a JSON document (single object)

         s6cmd select json --structure document \
             --query "SELECT s.tracking_id FROM s3object[*].metadata s" \
             s3://bucket/metadata.json

Example 5: Run a SQL query over a Parquet object

         s6cmd select parquet --query "SELECT s.id FROM S3Object s" \
             s3://bucket/data.parquet

Example 6: Default fallback (no subcommand) treats the source as JSON-Lines

         s6cmd select --query "SELECT s.id FROM s3object s" s3://bucket/data.jsonl

Example 7: Change the output format to CSV

         s6cmd select json --output-format csv \
             --query "SELECT s.id FROM s3object s" \
             s3://bucket/data.jsonl
`
