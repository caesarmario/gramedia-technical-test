####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Transform FakeStore resource (raw JSON --> staging Parquet)
####

from __future__ import annotations

import os
import json
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    "owner": "caesarmario87@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _get_var(key: str, default: str) -> str:
    """
    Safe Airflow Variable getter without relying on default_var kwarg
    """
    try:
        return Variable.get(key)
    except Exception:
        return default


# Project root inside the container.
PROJECT_ROOT: str = Variable.get("PROJECT_ROOT", "/opt/airflow")

def _run_transform(exec_ds: str, **_: Dict[str, Any]) -> None:
    """
    Invoke the resource-specific transformer via subprocess.
    """
    # Pull MINIO_CONFIG
    raw = _get_var("MINIO_CONFIG")
    try:
        creds_str = json.dumps(json.loads(raw))
    except Exception:
        creds_str = raw

    # Allow simple per-resource config file resolution
    config_path = f"schema_config/fakestore_raw/carts_schema_config.json"

    # Call the CLI transformer
    cmd = [
        "python", "-m", f"scripts.transform.transform_carts_to_parquet",
        "--ds", exec_ds,
        "--credentials", creds_str,
        "--config-file", config_path,
    ]

    # Ensure Python sees local modules
    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT, env=env)


with DAG(
    dag_id="01_02_dag_fakestore_transform_carts",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "transform", "parquet"],
) as dag:

    # Visual anchor for DAG start
    t00_start = EmptyOperator(task_id="t00_start")

    # Run the actual transformation (raw JSON → staging Parquet) for this resource.
    t10_run = PythonOperator(
        task_id="t10_run_transform_carts",
        python_callable=_run_transform,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    # Visual anchor for DAG end
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Simple linear flow
    t00_start >> t10_run >> t90_finish