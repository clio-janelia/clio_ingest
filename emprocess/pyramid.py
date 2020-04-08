"""Provides DAG task definitions for ingesting multi-scale data from aligned images.

This module creates an image pyramid in neuroglancer format from a
list of aligned images.  The images are stored in a temporary bucket
called source_{{ execution_date }}, where each image is encoded as an 
array of 1024x1024 tiles.  The tiles are located encoded in the file
as deterimined by the header.  The header is N*4 bytes (N is
the number of tiles) which gives the offset for each tile in the larger file.

A scale pyramid in jpeg and no compression is created using sharded
neeuroglancer format.  The sharding options correspond to what will
minimize writes to the same object.  In this case, 1024x1024x1024
cubes are extracted allowing scales 0 through 4 to be written disjointly
by changing the number of shard bits.

This module creates 500 worker tasks that iterate through all 1024x1024x1024
subvolumes.  It would be hard to know the number of tasks beforehand
since the alignment could affect this.

Note: this module defines related tasks and not a subdag.  See the documentation
in align.py for more details regarding this decision.
"""

from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook

import json

SHARD_SIZE = Variable.get('SHARD_SIZE', 1024) 
NUM_WORKERS = 500 # should have decent parallelism on cloud run

def export_dataset_psubdag(dag, name, image, minz, maxz, source, bbox_task_id, pool=None, TEST_MODE=False):
    """Creates ingsetion tasks for creating neuroglancer precomputed volumees.

    Args:
        dag (Airflow DAG): parent dag
        name (str): dag_id.name is the prefix for all tasks
        image (str): image name
        minz (int): minimum z slice
        maxz (int): maximum z slice
        source (str): location for data (gbucket name)
        bbox_task_id (str): task id for task containing bbox information for the images
        pool (str): name of high throughput queue for http requests
        TEST_MODE (boolean): if true disable requests to gbucket

    Returns:
        (starting dag task, ending dag task)

    """

    # write meta data for location/ng/jpeg and location/ng/raw
    create_ngmeta_t = SimpleHttpOperator(
        task_id=f"{dag.dag_id}.{name}.write_ngmeta",
        http_conn_id="IMG_READ_WRITE",
        endpoint="",
        data={
                "mode": "ngconfig",
                "location": source,
                "minz": minz,
                "maxz": maxz,
                "bbox": "{{{{ task_instance.xcom_pull(task_id='{bbox_task_id}', key='bbox') }}}}",
                "writeRaw": "{{ dag_run.conf.get('createRawPyramid', True) }}"
        },
        headers={"Accept": "application/json, text/plain, */*"},
        dag=dag
    )

    # create a pool of workers that iterate through any write
    # tasks determined by the bbox
    def write_ng_shards(temp_location, bbox, writeRaw, worker_id):
        """Write shards by invoking several http requests based on bbox and worker id.
        """
        def extract_range(pt1, pt2):
            start = pt1 // SHARD_SIZE
            finish = pt2 // SHARD_SIZE
            
        zstart, zfinish = extract_range(minz, maxz)
        ystart, yfinish = extract_range(0, bbox[1]-1)
        xstart, xfinish = extract_range(0, bbox[0]-1)
        
        glb_iter = 0

        for iterz in range(zstart, zfinish+1):
            for itery in range(ystart, yfinish+1):
                for iterx in range(xstart, xstart+1):
                    glb_iter += 1
                    if (glb_iter % NUM_WORKERS) == worker_id:
                        # call function that writes shard (exception raised for non-200, 300 response)
                        http = HttpHook("POST", http_conn_id="IMG_READ_WRITE")
                        response = http.run(
                                    "POST",
                                    {
                                            "mode": "ngshard",
                                            "location": source, # will write to location + /ng/raw or /ng/jpeeg
                                            "source": temp_location, # location of tilese 
                                            "start": [iterx, itery, iterz],
                                            "shard_size": SHARD_SIZE,
                                            "minz": minz,
                                            "maxz": maxz,
                                            "writeRaw": writeRaw 
                                    },
                                    {"Accept": "application/json, text/plain, */*"},
                                    {}
                                    )

    finish_t = DummyOperator(task_id=f"{dag.dag_id}.{name}.finish_ngwrite", dag=dag)

    for worker_id in range(NUM_WORKERS):
        write_shards_t = PythonOperator(
            task_id=f"{dag.dag_id}.{name}.write_ng_shards_{worker_id}",
            python_callable=write_ng_shards,
            op_kwargs={
                    "temp_location": f"{source}-" + "{{ execution_date }}",
                    "bbox": "{{{{ task_instance.xcom_pull(task_id='{bbox_task_id}', key='bbox') }}}}",
                    "writeRaw": "{{ dag_run.conf.get('createRawPyramid', True) }}",
                    "worker_id": worker_id
            },
            pool=pool,
            dag=dag,
        )
    
        create_ngmeta_t >> write_shards_t >> finish_t

    # provide bookend tasks to caller
    return create_ngmeta_t, finish_t
    #return subdag

