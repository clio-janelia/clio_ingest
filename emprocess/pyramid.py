"""Provides DAG task definitions for ingesting multi-scale data from aligned images.

This module creates an image pyramid in neuroglancer format from a
list of aligned images.  The images are stored in a temporary bucket
called source_{{ ds_nodash }}, where each image is encoded as an 
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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook
from emprocess.cloudrun_operator import CloudRunOperator

import json
import logging

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
    SHARD_SIZE = Variable.get('SHARD_SIZE', 1024) 
    NUM_WORKERS = 500 # should have decent parallelism on cloud run
        
    # when testing only use 10 workers
    if TEST_MODE:
        NUM_WORKERS = 10
    
    # do not need more workers than slices
    NUM_WORKERS = min(NUM_WORKERS, maxz-minz+1)

    # write meta data for location/ng/jpeg and location/ng/raw
    create_ngmeta_t = CloudRunOperator(
        task_id=f"{dag.dag_id}.{name}.write_ngmeta",
        http_conn_id="IMG_WRITE",
        endpoint="/ngmeta",
        data=json.dumps({
                "dest": source,
                "minz": minz,
                "maxz": maxz,
                "bbox": f"{{{{ task_instance.xcom_pull(task_ids='{bbox_task_id}') }}}}",
                "shard-size": SHARD_SIZE,
                "writeRaw": "{{ dag_run.conf.get('createRawPyramid', True) }}"
        }),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"},
        dag=dag
    )

    # create a pool of workers that iterate through any write
    # tasks determined by the bbox
    def write_ng_shards(temp_location, bbox, writeRaw, worker_id):
        """Write shards by invoking several http requests based on bbox and worker id.
        """
        bbox = json.loads(bbox)
        writeRaw = json.loads(writeRaw.lower())

        def extract_range(pt1, pt2):
            start = pt1 // SHARD_SIZE
            finish = pt2 // SHARD_SIZE
            return start, finish

        zstart, zfinish = extract_range(minz, maxz)
        ystart, yfinish = extract_range(0, bbox[1]-1)
        xstart, xfinish = extract_range(0, bbox[0]-1)
        
        glb_iter = 0

        # get auth
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"}
        try:
            import subprocess
            token = subprocess.check_output(["gcloud auth print-identity-token"], shell=True).decode()
            headers["Authorization"] = f"Bearer {token[:-1]}"
        except Exception:
            pass

        for iterz in range(zstart, zfinish+1):
            for itery in range(ystart, yfinish+1):
                for iterx in range(xstart, xstart+1):
                    glb_iter += 1
                    if (glb_iter % NUM_WORKERS) == worker_id:
                        # call function that writes shard (exception raised for non-200, 300 response)
                        http = HttpHook("POST", http_conn_id="IMG_WRITE")
                        response = http.run(
                                    "/ngshard",
                                    json.dumps({
                                            "dest": source, # will write to location + /ng/raw or /ng/jpeeg
                                            "source": temp_location, # location of tiles
                                            "start": [iterx, itery, iterz],
                                            "shard-size": SHARD_SIZE,
                                            "bbox": json.dumps(bbox),
                                            "minz": minz,
                                            "maxz": maxz,
                                            "writeRaw": json.dumps(writeRaw) 
                                    }),
                                    headers,
                                    {}
                                    )

    finish_t = DummyOperator(task_id=f"{dag.dag_id}.{name}.finish_ngwrite", dag=dag)

    for worker_id in range(NUM_WORKERS):
        write_shards_t = PythonOperator(
            task_id=f"{dag.dag_id}.{name}.write_ng_shards_{worker_id}",
            python_callable=write_ng_shards,
            op_kwargs={
                    "temp_location": f"{source}_" + "{{ ds_nodash }}",
                    "bbox": f"{{{{ task_instance.xcom_pull(task_ids='{bbox_task_id}') }}}}",
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

