# emwrite_docker 

This directory creates a docker containers that writes aligned images and neuroglancer
datasets.  It is compatible with Google cloud run to enable highly concurrent batch processing.

## Local installation instructions (for local testing)

(to install this container using Google please see "Deploying on cloud run" below)

This is a docker container that can be built locally using

	% docker build . -t emwrite

and the container launched with

	% docker run -e "PORT=8080" -p 8080:8080 -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/FILE_NAME.json:ro  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/FILE_NAME.json emwrite

This will start up a web client that listens to 127.0.0.1:8080.  The [GOOGLE_APPLICATION_CREDENTIALS](https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually) is an environment variable
that allows you to use google cloud storage locally.  The -v and -e options can be omitted if you are not using this feature.

## Using emwrite for cloud headless commands

To run emwrite through the web service simply post a JSON (configuration details below):

	% curl -X POST -H "Content-type: application/json" --data-binary @examples/config.json 127.0.0.1:8080/[end point]

The supported endpoints are:

* alignedslice (write aligned image into a single png and a set of temporary tiles for future scale pyramids)

```json
{
	"img": "name of image assumed to be at dest/raw",
	"transform": "[1 0 0 1 0 0] -- array string of each column in the affine matrix",
	"bbox": "[width, height] -- string of new bounding boxx",
	"dest": "destination bucket for aligned images",
	"dest-tmp": "destination bucket for temporary tiled images"
}
```

* ngmeta (write ng meta data)

```json
{
	"dest": "destination bucket for ng volumes",
	"minz": 0,
	"maxz": 1234,
	"shard-size": 1024,
	"bbox": "[width, height] -- string of per image bounding bbox",
	"writeRaw": "True -- string value for boolean indicating whether raw+jpeg should be written or just jpeg"
}
```

* ngshard (write ng shard -- the "start" value gives the offset in 1024x1024x1024 space for x, y, and z)

```json
{
	"dest": "destination bucket for ng volumes",
	"source": "bucket containing temporary tiled images",
	"start": [1, 2, 1],
	"shard-size": 1024,
	"minz": 0,
	"bbox": "[width, height] -- string of per image bounding bbox",
	"maxz": 1234
	"writeRaw": "True -- string value for boolean indicating whether raw+jpeg should be written or just jpeg"
}
```

## Deploying on cloud run

Create a google cloud account and install gcloud.

Build the container and store in cloud.

	% gcloud builds submit --tag gcr.io/[PROJECT_ID]/emwrite

If a container already exists, one can build faster from cache with this command
([PROJECT_ID] should be replaced in the YAML with the appropriate project id):

	% gcloud builds submit --config cloudbuild.yaml

Alternatively, one can use docker to build and deploy, which is many cases could be
more convenient since the locally tested image can just be uploaded.  The following
links gcloud with docker, builds a container, and uploads:

	% configure docker with gcloud: gcloud auth configure-docker
	$ docker build . -t gcr.io/flyem-private/emwrite
	$ docker push  gcr.io/flyem-private/emwrite

Once the container is built, deploy to cloud run with the following command.
The instance is created with a maximum 2GB of memory and sets the concurrency to 1
per instance.  Two cores are specified
as one core does not perform well in limited tests.  One should make
the endpoint private to avoid unauthorized use.

	% gcloud run deploy --memory 2048Mi --concurrency 1 --cpu 2 --image gcr.io/[PROJECT_ID]/emwrite --platform managed 

## Invoking cloud run

The resulting endpoint can be invoked through its HTTP api.  One must specify
a bearer token for authentication.  For convenience, when testing the API with curl
one can make an alias that is authenticated with the following:

	% alias gcurl='curl --header "Authorization: Bearer $(gcloud auth print-identity-token)"'

To write a single slice aligned slice (must have load the sample image into a directory dest-bucket/raw/):

% gcurl -H "Content-Type: application/json" -X POST --data-binary @example/align_config.json  https://[CLOUD RUN ADDR]/alignedslice
