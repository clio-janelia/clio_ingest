"""Workflow to ingest and process EM data.

Command line json:
{
    email: foo@bar # where to send results
}

Configuration:

Setup a pool with 500 workers for lightweight http requests
called "http_requests".

Set variable BATCH_SIZE (optional)

Append configs for each run type to variable "em_processing_configs":

    [{
    image: "template%d.png", # template name
    minz: 0, # first slice
    maxz: 50, # last slice
    source: bucket_name # location of stored pngs
    }
    ]

Setup a default email for airflow notifications

Configure email smptp as appropriate

"""

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email_operator import EmailOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable

BATCH_SIZE = Variable.get('BATCH_SIZE', 1024) 
START_DATE = datetime(2020, 4, 4)

# dynamically create DAGs for em_procesing based on configs
configs = Variable.get('em_processing_configs', default_var=[], deserialize_json=True)

for config in configs:

    DAG_NAME = f'em_processing_{config.get("id")}'

    # each dagrun is executed once and at time of submission
    DEFAULT_ARGS = {
            "owner": "airflow",
            "retries":1,
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

    def validate_params(**kwargs):
        """Check that img name, google bucket, and image range is specified.
        """

        # check if email is provided
        email_addr = kwargs['dag_run'].conf.get('email')
        if email_addr is None:
            return False

        # format string for image name4yy
        name = config.get('image')
        if name is None:
            return False

        # check for [minz, maxz] values
        minz = config.get('minz')
        if minz is None:
            return False

        maxz = config.get('maxz')
        if maxz is None:
            return False

        if minz > maxz:
            return False

        # location of storage (i.e., storage bucket name)
        location = config.get('source')
        if location is None:
            return False

    # validate parameters
    validate_t = PythonOperator(
            task_id="validate",
            provide_context=True,
            python_callable=validate_params, 
            dag=dag,
            )

    # alignment workflo which is a sub dag
    def align_workflow(parent, name, minz, maxz, source, collect_id):
        """Sub dag dynamically creates alignment tasks.
        """
        subdag = DAG(
                f"{parent}.align",
                start_date=START_DATE,
                )
        
        def compute_affine_ph(img1, img2, src):
            """Compute affine between two images (placeholde)
            """
            return [1, 0, 0, 1, 0, 0]


        def collect_affine_ph(**context):
            """Create transform array.
            """

            # ?! combine affines
            # read each transform and create global coordinate system
            for slice in range(minz, maxz+1):
                value = context['task_instance'].xcom_pull(task_ids=f"affine_{slice}")
                context['task_instance'].xcom_push(key="affine", value=value)

            # ?! write transforms to align/tranforms.csv
            # push bbox
            context['task_instance'].xcom_push(key="bbox", value=[2042, 3201])

        # find global coordinate system and write transforms
        collect_t = PythonOperator(
            task_id=collect_id,
            python_callable=collect_affine_ph,
            provide_context=True,
            dag=subdag,
        )

        # write alignd images
        def write_align_ph():
            """Apply transform and write files.

            Aligned images are saved in align/*.png

            Chunked BATCH_SIZExBATCH_SIZE file format is stored at tmp/*.png

            """
            # ?! get xcom
            return True

        # apply affines and write results 
        write_align_t = PythonOperator(
            task_id="write_align",
            python_callable=write_align_ph,
            dag=subdag,
        )

        # run pairwise affine calculation
        for slice in range(minz, maxz):
            affine_t = PythonOperator(
                task_id=f"affine_{slice}",
                python_callable=compute_affine_ph,
                pool="http_requests",
                dag=subdag,
                op_kwargs={'img1': name%slice, 'img2': name%(slice+1),'src': source},
            )
        
            affine_t >> collect_t
        
        collect_t >> write_align_t

        return subdag

    # create subdag for alignment
    align_t = SubDagOperator(
        subdag = align_workflow(DAG_NAME, config.get("image"), config.get("minz"),
            config.get("maxz"), config.get("source"), "create_volume_coords"),
        task_id="align",
        dag=dag,
        )

    # write neuroglancer scale pyramid
    def neuroglancer_ingest_workflow(parent, child, name, minz, maxz, source, bbox):
        """Creates scale pyramid and writes to ng format.

        Note: date is written to location/neuroglancer/*
        """

        subdag = DAG(
                f"{parent}.{child}",
                start_date=START_DATE
                )
       
        def setup_config_ph():
            """Write configuration for ng multiscale format.
            """

            # ?! setup ng configuration (probably use VM)
            print(source, bbox)

        def extract_range(pt1, pt2):
            start = pt1 // BATCH_SIZE
            finish = pt2 // BATCH_SIZE
            if (pt2 % BATCH_SIZE) != 0:
                finish += 1
            return start, finish
        zstart, zfinish = extract_range(minz, maxz)
        ystart, yfinish = extract_range(0, bbox[1])
        xstart, xfinish = extract_range(0, bbox[0])
       
        setup_config_t = PythonOperator(
                task_id="setup_ng_config",
                python_callable=setup_config_ph,
                pool="http_requests",
                dag=subdag
                )

        def write_pyramid_ph(z, y, x, name, source, chunk):
            """Write pyramid in ng for provide chunk.
            """
            pass

        for iterz in range(zstart, zfinish+1):
            for itery in range(ystart, yfinish+1):
                for iterx in range(xstart, xstart+1):
                    ng_pyramid_t = PythonOperator(
                        task_id=f"ng_pyrmaid_{iterz}_{itery}_{iterx}",
                        python_callable=write_pyramid_ph,
                        pool="http_requests",
                        dag=subdag,
                        op_kwargs={'z': iterz, 'y': itery, 'x': iterx, 'name': name, 'source': source, 'chunk': BATCH_SIZE}
                    )
                    setup_config_t >> ng_pyramid_t

        return subdag    


    # ?! how to extract image range??
    # create subdag for ingestion
    ngingest_t = SubDagOperator(
        subdag = neuroglancer_ingest_workflow(DAG_NAME, "ngingest", config.get("image"), config.get("minz"),
            config.get("maxz"), config.get("source"), [2000, 2000]), # "create_volume_coords"),
        #subdag = sub_dag(DAG_NAME, dag.img_name, dag.minz, dag.maxz, dag.source,
        #    {{ task_instance.xcom_pull(task_ids="create_volume_coords", key="bbox") }}),
        task_id="ngingest",
        dag=dag,
        )

    # conditional for successful alignment
    isaligned_t = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: "align",
        dag=dag)

    # delete temp tile images (*.png) (run if align_t succeeds and ngingest finishes)
    def cleanup_images_ph(source):
        """Deletes source/tmp/*.png temporary pngs.
        """
        # ?! delete data from source
        pass

    cleanup_t = PythonOperator(
                    task_id="cleanup_images",
                    python_callable=cleanup_images_ph,
                    dag=dag,
                    op_kwargs={'source': config.get("source")},
                    trigger_rule=TriggerRule.ALL_DONE
                )

    # notify user
    notify_t = EmailOperator(
            task_id="notify",
            to="{{ dag_run.conf['email'] }}",
            subject=f"airflow:{DAG_NAME}",
            html_content=f"job finished.  view at {config['source']}",
            dag=dag
    )

    validate_t >> align_t >> ngingest_t >> [cleanup_t,  notify_t]
    align_t >> isaligned_t >> cleanup_t 


