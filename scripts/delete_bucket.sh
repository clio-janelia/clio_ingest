# Sets bucket to be publicly readable

# usage: bash set_bucket_public.sh <bucket name>
# Note: Must be run in directory with delete.json

gsutil lifecycle set delete.json gs://${1}

