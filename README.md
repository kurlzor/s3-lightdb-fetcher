# s3-lightdb-fetcher

Downloads the latest dump in a given S3 bucket.

Some environment variables must be set :

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- S3_FETCHER_BUCKET: The name of the bucket containing the dumps
- S3_FETCHER_BUCKET_REGION: The region of the bucket containing the dumps
- S3_FETCHER_DUMP_PREFIX: The prefix of the filename of the dumps
