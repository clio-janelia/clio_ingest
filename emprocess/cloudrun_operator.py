"""Custom operator that just re-implements SimpleHTTPOperator but with gcloud bearer token.

Note: this requires gcloud to be available through the command line.
"""


from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow import AirflowException
import subprocess
import json
import time
import threading

class CloudRunOperator(SimpleHttpOperator):
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # add authorization if not presen and gcloud is available
        if "Authorization" not in self.headers:
            # extract auth token from gcloud
            try:
                token = subprocess.check_output(["gcloud auth print-identity-token"], shell=True).decode()
                self.headers["Authorization"] = f"Bearer {token[:-1]}"
            except Exception:
                pass

        return super().execute(context)



class CloudRunBatchOperator(BaseOperator):
    """Executes a series of mini tasks (cloud run) from a batch.

    This operator requires a task callable for generating minitasks.
    This callable is passed the context and any 'data' which is
    teemplated.

    """
    template_fields = ['data']

    @apply_defaults
    def __init__(
        self,
        gen_callable=None, # Callable
        worker_id=0, # int
        num_workers=0, # int
        data=None,  # dict (templated)
        conn_id=None, # string for connection
        endpoint="", # string for endpoint
        headers=None, # dict with http headers
        log_response = False,
        num_http_tries = 1, # int
        xcom_push = False,
        num_threads=8, # default threading for low-compute jobs
        validate_output=None, # callable with response as parameter
        *args,
        **kwargs
    ):
        super(CloudRunBatchOperator, self).__init__(*args, **kwargs)
        if not callable(gen_callable):
            raise AirflowException('`gen_callable` param must be callable')

        self.gen_callable = gen_callable
        self.worker_id = worker_id
        self.num_workers = num_workers
        self.data = data
        self.conn_id = conn_id
        self.endpoint = endpoint
        self.headers = headers or {}
        self.log_response = log_response
        self.xcom_push_flag = xcom_push
        self.num_http_tries = num_http_tries
        self.num_threads = num_threads
        self.validate_output = validate_output

    def execute(self, context):
        # generate mini tasks
        mini_tasks = self.gen_callable(self.worker_id, self.num_workers, self.data, **context)
        
        # -- call cloud run for each task --
        
        # set authorization
        if "Authorization" not in self.headers:
            # extract auth token from gcloud
            try:
                token = subprocess.check_output(["gcloud auth print-identity-token"], shell=True).decode()
                self.headers["Authorization"] = f"Bearer {token[:-1]}"
            except Exception:
                pass

        results = {}
        failure = None
        def run_query(thread_id):
            nonlocal failure
            for idx, [id, task] in enumerate(mini_tasks):
                if failure is not None:
                    break # exit thread if a failure is detected
                if (idx % self.num_threads) == thread_id:
                    params = json.dumps(task)
                    self.log.info(f"http params {id}: {params}") 

                    http = HttpHook("POST", http_conn_id=self.conn_id)
                    
                    # enable unconditional retries at mini task level
                    # to avoid problems with the whole batch crashing
                    num_tries = 0
                    success = False
                    while not success and num_tries < self.num_http_tries:
                        num_tries += 1
                        success = True
                        try:
                            response = http.run(
                                        self.endpoint,
                                        params,
                                        self.headers,
                                        {}
                                        )
                        except AirflowException as e:
                            if num_tries >= self.num_http_tries:
                                self.log.error(f"http final failure {id}: " + str(e))
                                failure = e
                                break
                            self.log.error(f"http failure {id}: " + str(e))
                            time.sleep(120) # wait a minute to try again
                            success = False
                        except Exception as e:
                            failure = e
                            break

                    if self.log_response:
                        self.log.info(response.text) 

                    # check if output is validate
                    if self.validate_output is not None:
                        if not self.validate_output(response):
                            failure = AirflowException(f"output test failed {id}")
                            break

                    if self.xcom_push_flag:
                        results[id] = response.text

        threads = [threading.Thread(target=run_query, args=[thread_id]) for thread_id in range(self.num_threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        # raise error if one of the threads failed
        if failure is not None:
            raise failure

        if self.xcom_push_flag:
            return results

