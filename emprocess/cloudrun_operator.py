"""Custom operator that just re-implements SimpleHTTPOperator but with gcloud bearer token.

Note: this requires gcloud to be available through the command line.
"""


from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
import subprocess

class CloudRunOperator(SimpleHttpOperator):
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        
        # add authorization if not presen and gcloud is available
        if "Authorization" not in self.headers:
            # extract auth token from gcloud
            try:
                token = subprocess.check_output(["gcloud auth print-identity-token"], shell=True).decode()
                self.headers["Authorization"] = f"Bearer {token}"
            except Exception:
                pass
