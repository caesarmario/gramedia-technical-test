####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Extract FakeStore resource
####

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, subprocess

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Project/paths (fallbacks keep it simple & readable)
PROJECT_ROOT: str = Variable.get("PROJECT_ROOT", default_var="/opt/airflow")
BASE_URL: str     = Variable.get("FAKESTORE_BASE_URL", default_var="https://fakestoreapi.com")

def _run_extract(exec_ds: str, **_) -> None:
    """Run the CLI extractor via subprocess with proper PYTHONPATH so `utils` imports work."""
    raw = Variable.get("MINIO_CONFIG")
    try:
        creds_str = json.dumps(json.loads(raw))
    except Exception:
        creds_str = raw

    cmd = [
        "python", "-m", f"scripts.extract.extract_users",
        "--ds", exec_ds,
        "--base-url", BASE_URL,
        "--credentials", creds_str,
    ]

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

    t00_start = EmptyOperator(task_id="t00_start")
    t10_resolve_params = EmptyOperator(task_id="t10_resolve_params")

    t20_run = PythonOperator(
        task_id="t20_run_extract_users",
        python_callable=_run_extract,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    t90_finish = EmptyOperator(task_id="t90_finish")

    t00_start >> t10_resolve_params >> t20_run >> t90_finish