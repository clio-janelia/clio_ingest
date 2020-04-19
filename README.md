# emprocssing_workflow

This package defines an Apache Airflow workflow to ingest and process data
collected by electron microscopy (EM).  The goal is to produce a push-button workflow
to handle smaller (but still computationally challenging) <10TB-sized datasets
 produced using high-quality FIB-SEM imaging.

![overview](resources/airflow_workflow.png)

Currently, the workflow aligns a stack of images and writes the scale pyramids
for fast, interactive viewing using the web-tool [neuroglancer](https://github.com/google/neuroglancer).
The major steps are higlighted in the image above.  The workflow is designed to work well with Google Composer, which is a managed Airflow
service and can be minimally provisioned.  The bulk of computation used by the workflow
leverages Google Cloud Run, which are stateless containers, where 100s can be spawned
in a "serverless" manner.

The documentation below explains how to install and use the service.  At the end, there is some discussion
on the underlying architecture and design decisions.

## Installation and configuration

The easiest way to use this workflow is to deploy to Google Composer (details below).  Google Composer
is a service that runs Apache Airflow that makes configuration very easy.  Furthermore, emprocessing_workflow
requires access to google cloud storage (potential future work to support other storage solutions) making
integration easier if using Composer.
However, one can relatively easily install and manage Apache Airflow.  Since
most of the compute should leverage the Cloud Run containers for alignment and pyramid
generation, there is no significant performance advantage for deploying to Composer versus another
solution.

For local deployment, install "gcloud" from Google and ensure that the account is configured to
a Google project id with billable account information.

To install Airflow and related components

	% pip install airflow
	% pip install google_cloud_storage httplib2 google_uth_httplib2 google-api-python-client # to enable google drivers

The em_processing workflow calls web applications that perform alignment
and writing tasks.  The alignment is done using the fiji Docker container that can be hosted 
on Google Cloud Run (or run locally for testing).  See [fiji_cloudrun](https://github.com/janelia-flyem/fiji_cloudrun)
for instructions.  Similarly, the writing of aligned EM data and scale pyramids
is done by the emwrite container, instructions in this package under emwrite_docker/.

To launch the server on localhost:8080 and scheduler:

	% airflow webserver -p 8080
	% airflow scheduler

By navigating to localhost:8080, one can see a dashboard with several example DAG workfows.
Several variables must be set to enable the em_processing DAG to be executed.

* Under Admin->Connections create ALIGN_CLOUD_RUN conn_id pointing to the http server running fiji
* Under Admin->Connections create IMG_WRITE conn_id pointing to the http server running emwrite
* Under Admin->Variables create "em_processing_configs" and set the value to the specifc workflows supported (see
example for defining new workflows in "Running a workflow").  There is a separate configuration or workflow for each image dataset and for each significant code version.
* Under Admin->Pools create "http_requests" and set to 500 if using Google Cloud Run or the capacity
of whatever is serving the alignment and writing web services.
* (local use) Setup email to enable Airflow to send notifications.
Modfiy the airflow.cfg "smtp" section by setting smtp_user and smtp_password.  For example, to use
your gmail address, set smtp_host to "smtp.gmail.com" and set "smtp_password" to the key
retrieved using [https://security.google.com/settings/security/apppasswords](https://security.google.com/settings/security/apppasswords).  Since this puts the key in a plain text file, it might not be the most secure
solution.  For information on how to do this on Google Cloud Composer, see [https://cloud.google.com/composer/docs/how-to/managing/creating#notification](https://cloud.google.com/composer/docs/how-to/managing/creating#notification). 

Apache Airflow polls the "dag" directory periodically, updating the workflows available for scheduling.
Copy emprocess.py and ./emprocess to the AIRFLOW_DIR/dags/.  Once a variable is properly set for "em_processing_configs" a workflow should appear.  By default, Composer activates any uploaded workflow.  For a local instance,
the specific DAG must be activated, which can be done through the web UI.

### Google Composer deployment

Deploying this application on Google Composer is straightforward if the user already has a google
cloud project account.  Creating a new environment can be done easily by following the instructions
in [https://cloud.google.com/composer/docs/quickstart](https://cloud.google.com/composer/docs/quickstart).
Once created, the settings should be configured as described above.  Composer allows users to interact
with the environment using the familiar Airflow web browser.  Environment variables can be set 
through the Composer UI.  em_process.py ./emprocess directory can be uploaded to a cloud bucket associated
under the Composer DAGs folder.

Airflow in Google Composer can be accessed via a user terminal command line by prepending an airflow
command with the following:

	% gcloud composer environments run [name of environment] --location [location] [airflow arguments]


## Testing local install

After installing locally, one can verify that the workflow parses properly by running

	% python emprocess.py

This does not execute the operations but builds the graph and its dependencies.  If this fails,
Airflow will not be able to create a workflow DAG corresponding to this file.

To test the installation, the following can be run locally without requiring cloud run
functions or source data.

* Add a workflow into the "em_processing_configs" array variable.

```json

[
{
	"id": "sample",
	"image": "img%05d.png",
	"minz": 0,
	"maxz": 10,
	"source": "sample_bucket", 
	"project": "flyem-private"
}
]
```
To avoid Google Cloud Storage during the test, SET the environment
AIRFLOW_TEST_MODE to anything (unset when starting production work).

Instead of launching the fiji and emwrite dockers, use a simple
moc server.py, which will start on localhost:9000 and set ALIGN_CLOUD_RUN
and IMG_WRITE to point to it.

	% python moc_server.py

Once the em_processing workflow is enabled (see web interface), a DAG execution
run can be performed using the following command-line.

	% airflow trigger_dag -r refactor1 -c '{"email": "your email"}' em_processing_sample_0.1

One can monitor progress through the web front-end.  Each task instance including its inputs, outputs, and logs
can be viewed.  

## Running a workflow

To process a stack of EM images the following needs to be done.

* load a set of PNG images to a storage bucket in the directory "raw" (GBUCKET/raw/\*.png)
* ensure that AIRFLOW_TEST_MODE is unset
* make sure that ALIGN_CLOUD_RUN and IMG_WRITE are set to point to the docker containers serving
fiji and em writing applications.
* create a configuration with a name for the workflow ("id"), the image
name string format ("image"), the first image slice ("minz"), the last slice ("maxz"),
the gbucket source ("source"), and the google project id ("project").  The configuration
below can be used for the iso.\* images found in the resources/ folder.

```json
{
	"id": "slice2", 
	"image": "iso.%05d.png",
	"minz": 3493, 
	"maxz": 3494, 
	"source": "SOME BUCKET",
	"project": "GCP PROJECT ID"
}
```

After this configuration is added to "em_processing_configs", the workflow will appear
in the web application.  Note: a version number is appended to the workflow name: em_processing_[id]_[version].

	% airflow trigger_dag -r refactor1 -c '{"email": "your email"}' em_processing_slice2_0.1

Once this workflow finishes, one can view the ingested data using neuroglancer.
To do this, the bucket must be publicly readable to be used by neuroglancer
for now (choose an obscure gbucket name if security is needed).  Go to the google
storage page in the Google cloud console and set permissions for allUsers to have 
viewer access.  Once this is done, the bucket must be configured to enable CORs
so that neuroglancer can access it.  First create the following json file (and call it
cors.json).

```json
[
    {
      "origin": ["*"],
      "responseHeader": ["Content-Length", "Content-Type", "Date", "Range", "Server", "Transfer-Encoding", "X-GUploader-UploadID", "X-Google-Trace"],
      "method": ["GET", "HEAD", "OPTIONS", "POST"],
      "maxAgeSeconds": 3600
    }
]
```
Then run:

	% gsutil cors set cors.json gs://[bucket name]

Navigate to [neuroglance](https://neuroglancer-demo.appspot.com/) and point the source to precomputed://gs://[bucket name]/neuroglancer/jpeg.

## Architecture Description

TBD

## Description of components in workflow

The major components of the workflow are alignment and pyramid
image ingestion.  Future work includes adding contrast enhancement
and various deep learning components for segmentation and synapse
prediction and other feature representation.

### alignment with FIJI
### writing scale pyramid into neuroglancer precomputed format









