# Sets bucket for coldline storage 

# usage: bash set_bucket_coldline.sh <bucket name>
# Note: Must be run in directory with coldline.json

gsutil lifecycle set coldline.json gs://${1}

