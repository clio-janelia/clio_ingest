# command <project id> <fiji cloud run> <image write cloud run>

# set default project
gcloud config set project ${1}

# create airflow
gcloud composer environments create emprocess --location us-east4 \
	--zone us-east4-c \
	--machine-type n1-standard-2 \
	--image-version composer-1.10.0-airflow-1.10.6 \
	--airflow-configs=celery-worker_concurrency=12,scheduler-max_threads=4,core-dag_concurrency=128,core-parallelism=128 \
	--env-variables=emproccess_version=0.1 \
	--python-version 3

# set http_requests pool to at least 32
gcloud composer environments run emprocess --location us-east4 \
	pool -- -s http_requests 128 httppool 

# set connectionsn ALIGN_CLOUD_RUN and IMG_WRITE
gcloud composer environments run emprocess --location us-east4 \
	connections -- -a --conn_id ALIGN_CLOUD_RUN --conn_type http --conn_host ${2}

gcloud composer environments run emprocess --location us-east4 \
	connections -- -a --conn_id IMG_WRITE --conn_type http --conn_host ${3}

# add dag
gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess/__init__.py \
    --destination emprocess

gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess/align.py \
    --destination emprocess

gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess/cloudrun_operator.py \
    --destination emprocess

gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess/fiji_script.py \
    --destination emprocess

gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess/pyramid.py \
    --destination emprocess

gcloud composer environments storage dags import \
    --environment emprocess \
    --location us-east4 \
    --source emprocess.py 
