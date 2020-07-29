# command <project id>

# set default project
gcloud config set project ${1}

gcloud composer environments delete emprocess \
    --location us-east4
