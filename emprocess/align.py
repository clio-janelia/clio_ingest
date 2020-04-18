"""Provides DAG task definitions for performing alignment.

The module generates tasks that examines image pairs,
calculates an affine transformation that minimizes differences
between matches SIFT features using Fiji.

From a DAG point of view, an initial and final task for this set
of functionality will be returned to the caller.

Note: This module wraps alignment in a set of task that can be
viewed conceptually as a sub-dag.  However, in Airflow, using a
subdag operator is problematic since the actual subdag opeerator
holds onto a worker while subdag tasks are executed.  While
this problem can be circumvented using subdag specific queues,
if the subdag task instance crashes, all sub dag tasks fail,
which could lead to undesirable behavior.  If sub-dag is made
a first-class citizen this module can be easily modified to return
a starting and finished task, which would both point to the
subdag operator.
"""

from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

import json
import logging
from emprocess import fiji_script

def align_dataset_psubdag(dag, name, image, minz, maxz, source, project_id, pool=None, TEST_MODE=False):
    """Creates aligntment tasks and communicates a resulting bounding box
    and success based on returned task instance's output.

    Note:
        ending dag task returns extents under the key "bbox" if it succeeds.

    Args:
        dag (Airflow DAG): parent dag
        name (str): dag_id.name is the prefix for all tasks
        image (str): image name
        minz (int): minimum z slice
        maxz (int): maximum z slice
        source (str): location for data (gbucket name)
        pool (str): name of high throughput queue for http requests
        TEST_MODE (boolean): if true disable requests to gbucket

    Returns:
        (starting dag task, ending dag task)

    """
    SHARD_SIZE = Variable.get('SHARD_SIZE', 1024) 
  
    # starting task (check for the existence of the raw/*.png data
    def check_data(**context):
        """Check if images exist.
        """

        # skip if testing workflow
        if TEST_MODE:
            return

        # grab all files in raw
        ghook = GoogleCloudStorageHook() # uses default gcp connection
        file_names = ghook.list(source, prefix="raw/")
        file_names = set(file_names)

        for slice in range(minz, maxz):
            if ("raw/" + (image % slice)) not in file_names:  
                raise AirflowException("raw data not loaded properly")


    # find global coordinate system and write transforms
    start_t = PythonOperator(
        task_id=f"{dag.dag_id}.{name}.start_align",
        python_callable=check_data,
        dag=dag,
    )   

    """ alternative create subdag task and subdag
    subdag = DAG(
                f"{parent}.align",
                start_date=START_DATE,
                )
    return subdag
     
    align_t = SubDagOperator(
        subdag = align_workflow(DAG_NAME, config.get("image"), config.get("minz"),
            config.get("maxz"), config.get("source"), "create_volume_coords"),
        task_id="align",
        dag=dag,
        )
    """

    def collect_affine(temp_location, **context):
        """Create transform arrays for each image and global bbox.

        Note: the computation is very straighforward matrix multiplication.  No
        need to use a docker image.
        """

        def calculate_transform(x, y, affine):
            """Apply transform to a point.
            """
            x1 = affine[0]*x+affine[2]*y+affine[4]
            y1 = affine[1]*x+affine[3]*y+affine[5]
            return (round(x1), round(y1))
      
        def process_results(res):
            """Determines whether affine or translation is used.
            The transform is also adjusted for a top-left origin
            and reorders the paramters to list col1, col2, and col3.
            """

            # default no-op
            affine = [1, 0, 0, 1, 0, 0]
            translation = [1, 0, 0, 1, 0, 0]

            width = res["width"]
            height = res["height"]

            def adjust_trans(trans):
                """Flips Y and moves origin to top left.
                """
                dx = trans[4] - (1 - trans[0])*width/2 + trans[2]*height/2
                dy = trans[5] - (1 - trans[3])*height/2 + trans[1]*width/2
                return [trans[0], -trans[2], -trans[1], trans[3], dx, dy]

            affine = adjust_trans(res["affine"])
            translation = adjust_trans(res["translation"])


            # use translatee coefficients if image rotated less than 0.5 percent
            if affine[2] <= 0.0008:
                affine = translation
            return affine, [width, height]

        # read each transform and create global coordinate system
        # (note: each transform is applied to n+1 slice, image sizes are assumed to have identical dims)
        last_affine = [1, 0, 0, 1, 0, 0]
        transforms = [[1, 0, 0, 1, 0, 0]]
        
        # store current bbox x range and y range and find max
        bbox = None
        global_bbox = None
        for slice in range(minz, maxz):
            res = json.loads(context['task_instance'].xcom_pull(task_ids=f"{dag.dag_id}.{name}.affine_{slice}"))
            # affine has already been modified to treat top-left of image as origin
            
            # process results
            curr_affine, bbox = process_results(res)
            
            # get bbox
            if slice == minz:
                global_bbox = [0, bbox[0], 0, bbox[1]] 
    
            # multiply matrices
            mod_affine = []
            mod_affine.append(last_affine[0]*curr_affine[0] + last_affine[2]*curr_affine[1])
            mod_affine.append(last_affine[1]*curr_affine[0] + last_affine[3]*curr_affine[1])
            
            mod_affine.append(last_affine[0]*curr_affine[2] + last_affine[2]*curr_affine[3])
            mod_affine.append(last_affine[1]*curr_affine[2] + last_affine[3]*curr_affine[3])
            
            mod_affine.append(last_affine[0]*curr_affine[4] + last_affine[2]*curr_affine[5] + last_affine[4])
            mod_affine.append(last_affine[1]*curr_affine[4] + last_affine[3]*curr_affine[5] + last_affine[5])
         
            last_affine = mod_affine
            # add affine to list
            transforms.append(mod_affine)

            # check corners to find bbox
            shift1 = calculate_transform(0, 0, mod_affine)
            shift2 = calculate_transform(0, bbox[1], mod_affine)
            shift3 = calculate_transform(bbox[0], 0, mod_affine)
            shift4 = calculate_transform(bbox[0], bbox[1], mod_affine)
            xmin = min(shift1[0], shift2[0], shift3[0], shift4[0]) 
            xmax = max(shift1[0], shift2[0], shift3[0], shift4[0]) 
            ymin = min(shift1[1], shift2[1], shift3[1], shift4[1]) 
            ymax = max(shift1[1], shift2[1], shift3[1], shift4[1]) 
            if xmin < global_bbox[0]:
                global_bbox[0] = xmin
            if ymin < global_bbox[2]:
                global_bbox[2] = ymin
            if xmax > global_bbox[1]:
                global_bbox[1] = xmax
            if ymax > global_bbox[3]:
                global_bbox[3] = ymax


        # push results for each image and create csv of transforms
        affines_csv = ""
        for slice in range(minz, maxz+1):
            curr_affine = transforms[slice-minz]
            curr_affine[4] = curr_affine[4]-global_bbox[0] # shift by min x
            curr_affine[5] = curr_affine[5]-global_bbox[2] # shift by min y
            context['task_instance'].xcom_push(key=f"{slice}", value=curr_affine)
            affines_csv += f"{slice} , '{curr_affine}'\n"

        logging.info(affines_csv)
        logging.info([global_bbox[1]-global_bbox[0], global_bbox[3]-global_bbox[2]])
        # push bbox for new image size
        context['task_instance'].xcom_push(key="bbox", value=[global_bbox[1]-global_bbox[0], global_bbox[3]-global_bbox[2]])
        # test mode disable
        if not TEST_MODE:
            # write transforms to align/tranforms.csv
            ghook = GoogleCloudStorageHook() # uses default gcp connection
            client = ghook.get_conn()
            bucket = client.bucket(source)
            blob = bucket.blob(blob_name="align/transforms.csv")
            blob.upload_from_string(affines_csv) 
            
            # create bucket for temporary images
            try:
                ghook.create_bucket(bucket_name=temp_location, project_id=project_id)
            except AirflowException as e:
                # ignore if the erorr is the bucket exists
                if not str(e).startswith("409"):
                    raise

    # find global coordinate system and write transforms
    collect_id = f"{dag.dag_id}.{name}.collect"
    collect_t = PythonOperator(
        task_id=collect_id,
        python_callable=collect_affine,
        provide_context=True,
        op_kwargs={'temp_location': f"{source}_" + "{{ ds_nodash }}"},
        dag=dag,
    )
 
    # finishing tasks
    def finish_align(**context):
        """Wait for all images to be written and push bbox.
        """
        return context['task_instance'].xcom_pull(task_ids=collect_id, key="bbox")

    # find global coordinate system and write transforms
    finish_t = PythonOperator(
        task_id=f"{dag.dag_id}.{name}.finish_align",
        python_callable=finish_align,
        provide_context=True,
        dag=dag,
    )   

    # align each pair of images, find global offsets, write results
    for slice in range(minz, maxz+1):
        if slice < maxz:
            img1 = "gs://" + source + "/raw/" + image % slice 
            img2 = "gs://" + source + "/raw/" + image % (slice+1) 

            #compute affine match between two images.
            #note: files are expected in src/raw/*
            affine_t = SimpleHttpOperator(
                task_id=f"{dag.dag_id}.{name}.affine_{slice}",
                http_conn_id="ALIGN_CLOUD_RUN",
                endpoint="",
                data=json.dumps({
                        "command": "-Xmx4g -XX:+UseCompressedOops -Dpre=\"img1.png\" -Dpost=\"img2.png\" -- --headless \"fiji_align.bsh\"",
                    "input-map": {
                            "img1.png": img1,
                            "img2.png": img2 
                    },
                    "input-str": {
                            "fiji_align.bsh": fiji_script.SCRIPT
                    }
                }),
                xcom_push=True, # push affine results to next task
                headers={"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"},
                pool=pool,
                dag=dag
            )
            start_t >> affine_t >> collect_t
        
        transform_val = f"{{{{ task_instance.xcom_pull(task_ids='{collect_id}', key='{slice}') }}}}"
        bbox_val = f"{{{{ task_instance.xcom_pull(task_ids='{collect_id}', key='bbox') }}}}"
        # write collected transforms back to google bucket (including temporary tile data)
        write_aligned_image_t = SimpleHttpOperator(
            task_id=f"{dag.dag_id}.{name}.write_{slice}",
            http_conn_id="IMG_WRITE",
            endpoint="/alignedslice",
            data=json.dumps({
                    "img": image % slice,
                    "transform": transform_val, 
                    "bbox": bbox_val, 
                    "dest-tmp": source + "_" + "{{ ds_nodash }}",
                    "slice": slice,
                    "shard-size": SHARD_SIZE,
                    "dest": source
            }),
            headers={"Content-Type": "application/json", "Accept": "application/json, text/plain, */*"},
            pool=pool,
            dag=dag
        )
        collect_t >> write_aligned_image_t >> finish_t

    # provide bookend tasks to caller
    return start_t, finish_t
    #return subdag

