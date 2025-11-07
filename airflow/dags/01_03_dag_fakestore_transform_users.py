####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Transform FakeStore resource (raw JSON → staging Parquet)
####

from __future__ import annotations

import os
import json
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _get_var(key: str, default: str) -> str:
    """
    Safe Airflow Variable getter without relying on default_var kwarg (works across versions).
    """
    try:
        return Variable.get(key)
    except Exception:
        return default


# Project/paths (fallbacks keep it simple & readable)
PROJECT_ROOT: str = _get_var("PROJECT_ROOT", "/opt/airflow")

def _run_transform(exec_ds: str, **_: Dict[str, Any]) -> None:
    """
    Call the CLI transformer via subprocess with proper PYTHONPATH
    so `from utils...` imports resolve cleanly inside the container.
    """
    raw = _get_var("MINIO_CONFIG", "{}")
    try:
        creds_str = json.dumps(json.loads(raw))  # normalize if stored as dict
    except Exception:
        creds_str = raw  # already a JSON string

    # Optional: allow per-env override of config root (kept simple)
    config_path = f"schema_config/fakestore_raw/users_schema_config.json"

    cmd = [
        "python", "-m", f"scripts.transform.transform_users_to_parquet",
        "--ds", exec_ds,
        "--credentials", creds_str,
        "--config-file", config_path,
    ]

    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT, env=env)


with DAG(
    dag_id="01_03_dag_fakestore_transform_users",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # triggered by the transform orchestrator or upstream
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "transform", "parquet"],
) as dag:

    t00_start = EmptyOperator(task_id="t00_start")
    t10_resolve_params = EmptyOperator(task_id="t10_resolve_params")

    t20_run = PythonOperator(
        task_id="t20_run_transform_users",
        python_callable=_run_transform,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    t90_finish = EmptyOperator(task_id="t90_finish")

    t00_start >> t10_resolve_params >> t20_run >> t90_finish