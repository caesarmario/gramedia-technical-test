####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Extract FakeStore source (API)
####

import os, json, subprocess
import json
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "caesarmario87@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Project/paths inside the container
PROJECT_ROOT = "/opt/airflow"
BASE_URL = "https://fakestoreapi.com"

def _run_extract(exec_ds: str, **_) -> None:
    """
    Execute the resource-specific extractor via subprocess.
    """
    # Fetch MinIO creds from Variables
    raw = Variable.get("MINIO_CONFIG")
    try:
        creds_str = json.dumps(json.loads(raw))  # handle dict-like storage
    except Exception:
        creds_str = raw

    # Call the CLI extractor module for this resource.
    cmd = [
        "python", "-m", f"scripts.extract.extract_users",
        "--ds", exec_ds,
        "--base-url", BASE_URL,
        "--credentials", creds_str,
    ]

    # Ensure local packages are importable and run from the project root.
    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT, env=env)

with DAG(
    dag_id="00_03_dag_fakestore_extract_users",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "extract"],
) as dag:

    # Start anchor
    t00_start = EmptyOperator(task_id="t00_start")

    # Run the actual extraction (API --> MinIO raw JSON).
    t10_run = PythonOperator(
        task_id="t10_run_extract_users",
        python_callable=_run_extract,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    # End anchor
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Simple, linear flow: start → extract → finish.
    t00_start >> t10_run >> t90_finish