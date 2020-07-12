# Sets bucket to be publicly readable

# usage: bash set_bucket_public.sh <bucket name>
# Note: Must be run in directory with cors.json

gsutil iam ch allUsers:objectViewer gs://${1}
gsutil cors set cors.json gs://${1}

