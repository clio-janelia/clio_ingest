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
can be viewed.  Note: the default execution management is done with a sequential scheduler.  As such,
the example will run very slowly as only one task can run at a time and the scheduler takes several
seconds until a new task is picked up.  This is not a limitatioin of Airflow
in general but rather of this very simple scheduler.  It is also not a problem when using Google Composer.

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
To run:


	% airflow trigger_dag -r test1 -c '{"email": "your email"}' em_processing_slice2_0.1

The semantics are slightly different when triggering Airflow through Composer on the command line)

	% airflow trigger_dag -- em_processing_slice2_0.1 --run_id test1 --conf '{"email": "your email"}' 

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

Navigate to [neuroglancer](https://neuroglancer-demo.appspot.com/) and point the source to precomputed://gs://[bucket name]/neuroglancer/jpeg.

## Architecture Description


### Why Airflow?

Apache Airflow was chosen to orchestrate the data processing for a few reasons.  Processing EM image data
for connectomics or other analysis requires several processing steps often with diverse
compute, communication, and memory requirements.  Airflow is well-designed
to manage a diversity of different types of compute or "operators" and providing for automatic
retries on failures.  It also has a great substrate
to try to organize these diverse steps into a cogent execution model.  There are other tools that can
manage data on compute clusters such as Spark and Google Dataflow; however, many of the operations
required in our pipeline are very simple, batch-oriented compute.  Furthermore, Airflow can call
these other technologies for parts of the pipeline that might better utilizee those technology
stacks.  Finally, Airflow is well supported and documented and provides a good web UI for debugging
and analyzing workflow runs.  Since Airflow is a generic solution, it seemed more likely to be portable
in other cloud or compute environments.  We chose Google Composer since it wraps
Apache Airflow in a way that makes deployment easy.

### Downsides

There are a few downsides to Airflow and the use of Google Composer.  The following elaborates on several
as it helped to guide design decisions.

Managing versioning in Airflow
seems a bit messy.  If a workflow changes, it is probably best to add the version number to the 
DAG name, so it is a distinctly named workflow.  There is not much dynamicism in the DAGs.  The DAGs
need to be parseable and created before execution of the DAG (DAG run).  Since the datasets for emprocessing
use a unique task for each image, a distinct DAG is needed for each dataset if it has a different
number of images.  The DAG cannot adapt
to the shape of the data during execution,
which is why in emprocessing, there is a fixed-number worker task pool size
to write neuroglancer data since the number of tasks are unknown beforehand.
Some of these design decisions are a reflection on the history of using Airflow to do
scheduled, repeatable 'cron' jobs (which can also be seen by the execution time variable that is required
even for manual, non-scheduled invocations).

Airflow has a concept of sub-graph which intuitively should encapsulate sub-DAGs of computation
that can be re-used or represent and distinct and closely related group of tasks.  However,
the implementation of sub-graphs is such that the sub-graph operator (entry point to the sub-graph)
runs on a separate worker for the duration of the sub-graph computation.  If that worker crashes for some
reason the whole sub-graph dies, which might lead to undesirable results.

Google Composer
does not offer a serverless solution, requiring at least 3 nodes to run Airflow.  It seems that with
the ability to use Cloud Run or dynamic Kubernetes cluster, pre-allocation of compute would be
unnecessary and enable an on-demand, light-weight
web service that is always available, which would be ideal from a costing point-of-view.

Airflow is designed to orchestrate different types of compute.  But the implementation for these
distinct operations are often in the Airflow application as well (encouraged by the diversity
of custom operators).  This mixture of implementation logic
and orchestration seems a little messy.  It is nicely argued [here](https://towardsdatascience.com/how-to-use-airflow-without-headaches-4e6e37e6c2bc), that a better paradigm would be to
only orhestrate kubernetes pods which can then encapsulate all the logic distinctly.  It would also
allow users to not have to spend a lot of time understanding the myriad of operators available in Airflow.
This seems
like a reasonable approach except some tasks are just a lot more easily implemented with
custom operators and embedded logic.


### emprocessing Design

The emprocessing workflow was designed to provide a some dynamicism and version tracking to manage
the (hopefully) one-time execution run for each dataset.  To generate a DAG for a new dataset, a configuration
spec is loaded into an Airflow variable (available to all DAGs and during DAG runs).  Once this
configuration is loaded, Airflow will automatically generate a new workflow for this dataset giving
a unique DAG exists for each dataset (airflow frequently polls all files in the dag folder and it is best
practice to keep the computation at the top-level simple and non-compute intensive).
However, only one template for a DAG is defined in 
emprocss.py; specific instances are generated based on an iteration through the configuration
specs.  emprocess.py also specifies a version number.  When large changes are made to the code, the user
should modify this number which will automatically trigger a new set of workflows tagged with the new
version ID to be created.  Airflow keeps the runtime information for any previous DAG runs
but in this way future invocations will be explicitly separated.

The high-level orchestration of tasks is shown in the figure above (details on the major components below).
In general, the code was designed to make each component (alignment and pyramid creation)
 a sub-dag in code design (each component
is included as a dependency to the main dag file) but without the sub-dag semantics given the
limitations mentioned above.  But it should be pretty straightforward to convert this to sub-dags
if improvements are made in future versions of Airflow.

The majority of compute is not run on the Airflow clusteer but rather using the serverless
Cloud Run platform.  This seeems to combine several ideal features: 1) it can run in a generic
docker container separating logic from python code in the DAG, 2) it auto-scales from to 1000
compute nodes, and 3) it is serverless, requiring no provisioning.  Cloud run functions
are used in alignment and in pyramid writing.  A separate worker pool is created for operators
that call these http-based endpoints, since several hundred can potentially be executed
in parallel even on a lightly-provisioned machine.


### Description of workflow components

The major components of the workflow are alignment and pyramid
image ingestion.  Future work includes adding contrast enhancement
and various deep learning components for segmentation and synapse
prediction and other feature extraction.

#### alignment with FIJI

A headless fiji script (documented in string format in emprocessing/fiji_script.py) is run between
pairs of adjacent images.  An alignment transformation is computed by fitting an affine
transformation using RANSAC over matched SIFT features.  If there is little deformation
in the transformation, a rigid, translation only transformation is favored.  Once these
transforms are computed across all adjacent images, the results are combined and a global
bounding box is computed for the volume.  The images are then written to the "align"
directory along with the transformation matrix used.  Future work could make this more
robust by matching every other slice or to find large transformations as a mechanism
to screen for outliers.  This component also writes out temporary files (one per 2d image),
which encodes each image as an array of 1024x1024 tiles.

#### writing scale pyramid into neuroglancer precomputed format

The dataset is written to /neuroglancer/jpeg and neuroglancer/raw (optional) using
neuroglancer's [pre-computed sharded format](https://github.com/google/neuroglancer/blob/master/src/neuroglancer/datasource/precomputed/sharded.md#sharding-specification).  Each "shard" is a separate
file that encodes several small chunks which can be effienctly retrieved for interactive
viewing.  The purpose of the "shard" is to have very few files to make copying the dataset
fast, while still allowinig fast indexing into small chunks when only needing parts
of the dataset.

This component pre-allocates 500 workers that iterate through different
 "shards" of the dataset of size
1024x1024x1024.  The pyramid generation is simplified greatly by adjusting the shard
size for each pyramid so that there is one for each 1024^3, one for each 512^3, etc.
This allow each worker to be completely independent of all the others.
1024^3 can be read efficiently from the temporary files computed in the previous step
by doing a range fetch to retrieve just the relevant 1024x1024 tile.  Note, that
with this strategy (assuming that 64^3 represents the smallest chunk unit size
recommended), it is not possible (without orchestrating multiple writers to the same file)
to have a scale level higher than 4 (where
0 is maximum resolution) unless an initial shard size over 1024 is used or more sophisticated
inter-process communication is employed.




