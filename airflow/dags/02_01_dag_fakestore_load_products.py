####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Load "products" Parquet → L1 + dbt test + quarantine + report upload
####

import os, json, subprocess
from io import BytesIO
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from utils.etl_utils import ETLHelper


# --- Airflow defaults ---
default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}
PROJECT_ROOT: str = Variable.get("PROJECT_ROOT", "/opt/airflow")


def _run_load(exec_ds: str, **_) -> None:
    """Load L1 via the psycopg2 loader script."""
    raw_minio = Variable.get("MINIO_CONFIG")
    raw_pg    = Variable.get("POSTGRESQL_CONFIG")
    try:
        minio_str = json.dumps(json.loads(raw_minio))
    except Exception:
        minio_str = raw_minio
    try:
        pg_str = json.dumps(json.loads(raw_pg))
    except Exception:
        pg_str = raw_pg

    cmd = [
        "python", "-m", f"scripts.load.load_products_parquet_to_l1",
        "--ds", exec_ds,
        "--minio-credentials", minio_str,
        "--pg-credentials",    pg_str,
        "--config-file", f"schema_config/fakestore_raw/products_schema_config.json",
        "--target-schema", "l1",
        "--load-mode", "replace_partition",
    ]
    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT, env=env)


def _dbt_env() -> dict:
    """dbt env from Airflow Variable DBT_PG_CONFIG (and optional extras)."""
    cfg = json.loads(Variable.get("DBT_PG_CONFIG"))
    cfg.setdefault("DBT_TARGET", cfg.get("DBT_TARGET", "dev"))
    cfg.setdefault("DBT_PROFILES_DIR", "/dbt")
    return cfg


def _quarantine_failures(exec_ds: str, **_) -> None:
    dbt_cfg      = json.loads(Variable.get("DBT_PG_CONFIG"))
    audit_schema = dbt_cfg.get("DBT_AUDIT_SCHEMA")
    pg_cfg       = json.loads(Variable.get("POSTGRESQL_CONFIG"))
    minio_cfg    = json.loads(Variable.get("MINIO_CONFIG"))
    bucket_dq    = minio_cfg.get("MINIO_BUCKET_DQ")

    pg_conn   = ETLHelper.create_pg_conn({
        "POSTGRES_HOST": pg_cfg["POSTGRES_HOST"],
        "POSTGRES_PORT": pg_cfg["POSTGRES_PORT"],
        "POSTGRES_USER": pg_cfg["POSTGRES_USER"],
        "POSTGRES_PASSWORD": pg_cfg["POSTGRES_PASSWORD"],
        "POSTGRES_DB": pg_cfg["POSTGRES_DB"],
    })
    minio_cli = ETLHelper.create_minio_client(minio_cfg)

    try:
        ETLHelper.quarantine_dbt_failures_to_minio(
            pg_conn, minio_cli,
            audit_schema=audit_schema,
            bucket_name=bucket_dq,
            layer="l1",
            resource="products",
            ds=exec_ds,
        )
    finally:
        pg_conn.close()


def _upload_dbt_artifacts(exec_ds: str, **_) -> None:
    cfg       = json.loads(Variable.get("MINIO_CONFIG"))
    client    = ETLHelper.create_minio_client(cfg)
    bucket    = cfg.get("MINIO_BUCKET_DQ_REPORTS")

    ETLHelper.upload_dbt_artifacts_to_minio(
        minio_client=client,
        bucket_name=bucket,
        layer="l1",
        resource="products",
        ds=exec_ds,
        base_dir="/dbt/target",
    )


with DAG(
    dag_id=f"02_01_dag_fakestore_load_products",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "load", "l1"],
) as dag:

    t00_start = EmptyOperator(task_id="t00_start")

    t10_run = PythonOperator(
        task_id="t10_run_load_products",
        python_callable=_run_load,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    # HARD GATE: dbt tests must pass (severity:error -> nonzero -> stop)
    t20_dbt_test_l1 = BashOperator(
        task_id="t20_dbt_test_l1",
        bash_command="""
            cd /dbt && \
            dbt test --select source:fakestore_l1.products
        """,
        env=_dbt_env(),
    )

    # Generate catalog & docs for report bundle
    t21_dbt_docs_generate = BashOperator(
        task_id="t21_dbt_docs_generate",
        bash_command="""
            cd /dbt && \
            dbt docs generate
        """,
        env=_dbt_env(),
    )

    # Upload artifacts ke MinIO (jalan walau test fail)
    t22_upload_dbt_artifacts = PythonOperator(
        task_id="t22_upload_dbt_artifacts",
        python_callable=_upload_dbt_artifacts,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # QUARANTINE: always try to export failing rows to MinIO, even if tests failed
    t25_quarantine_failures = PythonOperator(
        task_id="t25_quarantine_failures",
        python_callable=_quarantine_failures,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t90_finish = EmptyOperator(task_id="t90_finish")

    # chaining
    t00_start >> t10_run >> t20_dbt_test_l1
    t20_dbt_test_l1 >> t21_dbt_docs_generate >> t22_upload_dbt_artifacts
    t20_dbt_test_l1 >> t25_quarantine_failures
    [t22_upload_dbt_artifacts, t25_quarantine_failures] >> t90_finish