"""Workflow to ingest and process EM data.

--Settings--

Command line json:
{
    email: foo@bar # where to send results
    createRawPyramid: True # create raw scale pyramid in addition to jpeg (True is default)
}

Input: images in a source/raw/*.png

Environment: If testing locally without data, set AIRFLOW_TEST_MODE=1

Airflow Configuration:

Setup a pool with 500 workers for lightweight http requests
called "http_requests".

Setup a default email for airflow notifications

Configure email smptp as appropriate

Conn id:

* ALIGN_CLOUD_RUN (str): http address
* IMG_WRITE (str): http address

Airflow Variables:

* SHARD_SIZE (optional): default 1024

* Append configs for each run type to variable "em_processing_configs":

    [{
    image: "template%d.png", # template name
    minz: 0, # first slice
    maxz: 50, # last slice
    source: bucket_name # location of stored pngs
    }
    ]


"""

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from airflow import AirflowException

import logging
import json

# custom local dependencies
from emprocess import align, pyramid

# check if in testing mode
import os
TEST_MODE_ENV = os.environ.get("AIRFLOW_TEST_MODE", None)
TEST_MODE = False
if TEST_MODE_ENV is not None:
    TEST_MODE = True



"""Version of dag.

For very small non-functional change, do not modify.   For small changes for performance
and optimization that don't impact DAG or results greatly, modify subversion.

For task 'additions' and bigger optimization
changes, minor version change.  For very large DAG changes, major version number change.
Both minor and major version changes will reseult in a new DAG workflow.

The initial operator should attempt to print out any necessary provenance
so that it is in the log (such as command line options).  Somehow provide
some version information for dependencies (preferably automatically).
"""

VERSION = "0.62.0"
SUBVERSION = "1"
SHARD_SIZE = Variable.get('SHARD_SIZE', 1024) 
START_DATE = datetime(2020, 4, 4)

# dynamically create DAGs for em_procesing based on configs
configs = Variable.get('em_processing_configs', default_var=[], deserialize_json=True)

for config in configs:

    DAG_NAME = f'em_processing_{config.get("id")}_{VERSION}'

    # each dagrun is executed once and at time of submission
    DEFAULT_ARGS = {
            "owner": "airflow",
            "retries": 1,
            "start_date": START_DATE,
            "email_on_failure": True,
            "email_on_retry": True,
            }

    dag = DAG(
            DAG_NAME,
            default_args=DEFAULT_ARGS,
            description="workflow to ingest, align, and process EM data",
            schedule_interval=None,
            )

    # set to global
    globals()[DAG_NAME] = dag
   
    # optional parameter for downsampling data during alignment
    downsample_factor = config.get("downsample_factor", 1)

    def validate_params(**kwargs):
        """Check that img name, google bucket, and image range is specified.
        """

        logging.info(f"Version({VERSION}) Sub-version({SUBVERSION})")
        logging.info(f"Chunk size: {SHARD_SIZE})")

        # check if email is provided
        email_addr = kwargs['dag_run'].conf.get('email')
        if email_addr is None:
            raise AirflowException("no email provided")

        logging.info(f"Email provided: {email_addr}")

        # check raw pyrmaid config
        if kwargs['dag_run'].conf.get('createRawPyramid', True):
            logging.info("Enables raw pyramid creation")
        else:
            logging.info("Disable raw pyramid creation")
    
        # log downsample factor
        logging.info(f"Downsample factor: {downsample_factor}")

        # format string for image name
        name = config.get('image')
        if name is None:
            raise AirflowException("no image exists")

        # check for [minz, maxz] values
        minz = config.get('minz')
        if minz is None:
            raise AirflowException("no minz exists")

        maxz = config.get('maxz')
        if maxz is None:
            raise AirflowException("no maxz exists")

        if minz > maxz:
            raise AirflowException("no maxz should be greater than minz")

        # location of storage (i.e., storage bucket name)
        location = config.get('source')
        if location is None:
            raise AirflowException("no location exists")
        
    # validate parameters
    validate_t = PythonOperator(
            task_id="validate",
            provide_context=True,
            python_callable=validate_params, 
            dag=dag,
            )


    align_start_t, align_end_t = align.align_dataset_psubdag(dag, "align", config.get("image"), config.get("minz"),
            config.get("maxz"), config.get("source"), config.get("project"), downsample_factor, "http_requests", TEST_MODE)

    
    ngingest_start_t, ngingest_end_t = pyramid.export_dataset_psubdag(dag, "ngingest", config.get("image"), config.get("minz"),
        config.get("maxz"), config.get("source"), align_end_t.task_id, "http_requests", TEST_MODE)

    # pull xcom from a subdag to see if data was written
    def iswritten(**context):
        #value = context['task_instance'].xcom_pull(dag_id=f"{DAG_NAME}.align", task_ids="write_align")
        value = context['task_instance'].xcom_pull(task_ids=align_end_t.task_id)
        if value is not None:
            return value
        return False

    # conditional for successful alignment
    isaligned_t = ShortCircuitOperator(
        task_id='iswritten',
        python_callable=iswritten,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
        dag=dag)

    # delete source_{ds_nodash}/(*.png) (run if align_t succeeds and ngingest finishes) -- let it survive for 1 day in case there are re-runs and the same policy is still in effect
    lifecycle_config = {
                        "lifecycle": {
                            "rule": [
                                {
                                    "action": {"type": "Delete"},
                                    "condition": {
                                        "age": 1
                                        }
                                }
                                ]
                        }
                        }
    commands = f"echo '{json.dumps(lifecycle_config)}' > life.json;\n"
    if not TEST_MODE:
        commands += f"gsutil lifecycle set life.json gs://{config.get('source')}_" + "{{ ds_nodash }};\n"
    commands += "rm life.json;"

    cleanup_t = BashOperator(
                    task_id="cleanup_images",
                    bash_command=commands,
                    dag=dag,
                )

    # notify user
    notify_t = EmailOperator(
            task_id="notify",
            to="{{ dag_run.conf['email'] }}",
            subject=f"airflow:{DAG_NAME}",
            html_content=f"job finished.  view at {config['source']}",
            dag=dag
    )

    # cleanup is triggered if alignment completes properly
    validate_t >> align_start_t
    align_end_t >> ngingest_start_t
    [ngingest_end_t, cleanup_t] >> notify_t
    
    [align_end_t, ngingest_end_t] >> isaligned_t >> cleanup_t 


