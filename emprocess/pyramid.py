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
from emprocess.cloudrun_operator import CloudRunOperator, CloudRunBatchOperator

import json
import logging

def export_dataset_psubdag(dag, name, NUM_WORKERS, bbox_task_id, pool=None, TEST_MODE=False, SHARD_SIZE=1024):
    """Creates ingsetion tasks for creating neuroglancer precomputed volumees.

    Args:
        name (str): dag_id.name is the prefix for all tasks
        NUM_WORKERS (int): number of workers that will process all of the mini tasks
        bbox_task_id (str): task id for task containing bbox information for the images
        pool (str): name of high throughput queue for http requests
        TEST_MODE (boolean): if true disable requests to gbucket
        SHARD_SIZE (int): chunk size used for saving data
    Returns:
        (starting dag task, ending dag task)

    """
    
    # write meta data for location/ng/jpeg and location/ng/raw
    create_ngmeta_t = CloudRunOperator(
        task_id=f"{name}.write_ngmeta",
        http_conn_id="IMG_WRITE",
        endpoint="/ngmeta",
        data=json.dumps({
                "dest": "{{ dag_run.conf['source'] }}",
                "minz": "{{ dag_run.conf['minz'] }}",
                "maxz": "{{ dag_run.conf['maxz'] }}",
                "bbox": f"{{{{ task_instance.xcom_pull(task_ids='{bbox_task_id}') }}}}",
                "shard-size": SHARD_SIZE,
                "writeRaw": "{{ dag_run.conf.get('createRawPyramid', True) }}",
                "resolution": "{{ dag_run.conf.get('resolution', 8) }}"
        }),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"},
        dag=dag
    )

    # create a pool of workers that iterate through any write
    # tasks determined by the bbox
    def write_ng_shards(worker_id, num_workers, data, **context):
        """Write shards by invoking several http requests based on bbox and worker id.
        """
        bbox = json.loads(data["bbox"])
        writeRaw = json.loads(data["writeRaw"].lower())

        def extract_range(pt1, pt2):
            start = pt1 // SHARD_SIZE
            finish = pt2 // SHARD_SIZE
            return start, finish

        zstart, zfinish = extract_range(int(data["minz"]), int(data["maxz"]))
        ystart, yfinish = extract_range(0, bbox[1]-1)
        xstart, xfinish = extract_range(0, bbox[0]-1)
        
        glb_iter = 0
        task_list = []
        for iterz in range(zstart, zfinish+1):
            for itery in range(ystart, yfinish+1):
                for iterx in range(xstart, xfinish+1):
                    if (glb_iter % num_workers) == worker_id:
                        params = {
                                    "dest": data["source"], # will write to location + /ng/raw or /ng/jpeeg
                                    "source": data["temp_location"], # location of tiles
                                    "start": [iterx, itery, iterz],
                                    "shard-size": data["shard-size"],
                                    "bbox": data["bbox"],
                                    "minz": int(data["minz"]),
                                    "maxz": int(data["maxz"]),
                                    "writeRaw": data["writeRaw"] 
                            }
                        task_list.append([glb_iter, params])
                    glb_iter += 1

        return task_list

    finish_t = DummyOperator(task_id=f"{name}.finish_ngwrite", dag=dag)

    

    headers = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"}
    for worker_id in range(NUM_WORKERS):
        write_shards_t = CloudRunBatchOperator(
            task_id=f"{name}.write_ng_shards_{worker_id}",
            gen_callable=write_ng_shards,
            worker_id=worker_id,
            num_workers=NUM_WORKERS,
            data={
                    "source": "{{ dag_run.conf['source'] }}",
                    "temp_location": f"{{{{ dag_run.conf['source'] }}}}_" + "{{ ds_nodash }}",
                    "minz": "{{ dag_run.conf['minz'] }}",
                    "maxz": "{{ dag_run.conf['maxz'] }}",
                    "bbox": f"{{{{ task_instance.xcom_pull(task_ids='{bbox_task_id}') }}}}",
                    "writeRaw": "{{ dag_run.conf.get('createRawPyramid', True) }}",
                    "shard-size": SHARD_SIZE
            },
            conn_id="IMG_WRITE",
            endpoint="/ngshard",
            headers=headers,
            log_response=False,
            num_http_tries=8, # retrying works okay nown
            cache="gs://" + "{{ dag_run.conf['source'] }}" + "/neuroglancer/cache" if not TEST_MODE else "",
            xcom_push=False,
            pool=pool,
            try_number = "{{ task_instance.try_number }}",
            dag=dag,
        )

        create_ngmeta_t >> write_shards_t >> finish_t

    # provide bookend tasks to caller
    return create_ngmeta_t, finish_t
    #return subdag

