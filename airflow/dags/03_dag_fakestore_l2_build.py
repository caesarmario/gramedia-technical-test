####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Build L2 (dbt models) + tests + quarantine + report upload
####

import json

from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from utils.etl_utils import ETLHelper


# --- Defaults ---
default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}


def _dbt_env() -> dict:
    """dbt env from Airflow Variable DBT_PG_CONFIG (and optional extras)."""
    cfg = json.loads(Variable.get("DBT_PG_CONFIG"))
    cfg.setdefault("DBT_TARGET", cfg.get("DBT_TARGET"))
    cfg.setdefault("DBT_PROFILES_DIR")
    return cfg


def _quarantine_failures(exec_ds: str, **_) -> None:
    """Export dbt test failure rows (store_failures) for L2 to MinIO."""
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
            layer="l2",
            resource="all",
            ds=exec_ds,
        )
    finally:
        pg_conn.close()


def _upload_dbt_artifacts(exec_ds: str, **_) -> None:
    """Upload dbt artifacts for L2 to MinIO."""
    minio_cfg = json.loads(Variable.get("MINIO_CONFIG"))
    client    = ETLHelper.create_minio_client(minio_cfg)
    bucket    = minio_cfg.get("MINIO_BUCKET_DQ_REPORTS")

    ETLHelper.upload_dbt_artifacts_to_minio(
        minio_client=client,
        bucket_name=bucket,
        layer="l2",
        resource="all",
        ds=exec_ds,
        base_dir="/dbt/target",
    )


with DAG(
    dag_id="03_dag_fakestore_l2_build",
    start_date=datetime(2025, 1, 1),
    schedule="5 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "l2", "dbt", "dq"],
) as dag:

    t00_start = EmptyOperator(task_id="t00_start")

    # Build L2 models + run L2 tests (acts as the quality gate)
    # Select all models under models/l2; pass batch_ds to models if they use it.
    t10_dbt_build_l2 = BashOperator(
        task_id="t10_dbt_build_l2",
        bash_command="""
            cd /dbt && \
            dbt build --select path:models/l2 --vars '{batch_ds: "{{ dag_run.conf.get("ds", ds) }}"}'
        """,
        env=_dbt_env(),
    )

    # Generate docs/catalog for reporting bundle (run even if build fails, to capture state)
    t20_dbt_docs_generate = BashOperator(
        task_id="t20_dbt_docs_generate",
        bash_command="""
            cd /dbt && \
            dbt docs generate --vars '{batch_ds: "{{ dag_run.conf.get("ds", ds) }}"}'
        """,
        env=_dbt_env(),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Upload artifacts (run_results.json, manifest.json, catalog.json, index.html) to MinIO
    t21_upload_dbt_artifacts = PythonOperator(
        task_id="t21_upload_dbt_artifacts",
        python_callable=_upload_dbt_artifacts,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Export failing rows (store_failures) to MinIO even when tests fail
    t25_quarantine_failures = PythonOperator(
        task_id="t25_quarantine_failures",
        python_callable=_quarantine_failures,
        op_kwargs={"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t90_finish = EmptyOperator(task_id="t90_finish")

    # chaining
    t00_start >> t10_dbt_build_l2
    t10_dbt_build_l2 >> t20_dbt_docs_generate >> t21_upload_dbt_artifacts
    t10_dbt_build_l2 >> t25_quarantine_failures
    [t21_upload_dbt_artifacts, t25_quarantine_failures] >> t90_finish
