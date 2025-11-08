####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Pack deliverables → write LOCAL samples under /opt/project/assets/sample_output/*
####

import os
import io
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.etl_utils import ETLHelper
from utils.logging_utils import logger


default_args = {"owner": "caesarmario87@gmail.com", "retries": 1, "retry_delay": timedelta(minutes=1)}
SAMPLE_OUTPUT_DIR = "/opt/project/assets/sample_output"
RESOURCES = json.loads(Variable.get("FAKESTORE_RESOURCES", '["products","carts","users"]'))

# -------- Helpers --------
def _ensure_dirs(ds_val: str, **_):
    """Create the local output directory structure if missing (idempotent)."""
    base = SAMPLE_OUTPUT_DIR
    for sub in [
        "dq-reports",
        "l1",
        "l2",
        "raw/json",
        "raw/parquet",
    ]:
        os.makedirs(os.path.join(base, sub), exist_ok=True)

def _pg_conn():
    """Open a Postgres connection using POSTGRESQL_CONFIG Airflow Variable."""
    pg_cfg = json.loads(Variable.get("POSTGRESQL_CONFIG"))
    return ETLHelper.create_pg_conn({
        "POSTGRES_HOST": pg_cfg["POSTGRES_HOST"],
        "POSTGRES_PORT": pg_cfg["POSTGRES_PORT"],
        "POSTGRES_USER": pg_cfg["POSTGRES_USER"],
        "POSTGRES_PASSWORD": pg_cfg["POSTGRES_PASSWORD"],
        "POSTGRES_DB": pg_cfg["POSTGRES_DB"],
    })

def _export_fact_csv(exec_ds: str, **_):
    """Export L2.fact_sales for this ds into l2/cleaned_data.csv."""
    out_path = os.path.join(SAMPLE_OUTPUT_DIR, "l2", "cleaned_data.csv")
    with _pg_conn() as conn:
        df = pd.read_sql_query(
            "select * from l2.fact_sales where load_ds::date = %s::date order by transaction_date, product_id",
            conn, params=[exec_ds]
        )
        df.to_csv(out_path, index=False)

def _export_dim_csv(exec_ds: str, **_):
    """Export l2.dim_product rows relevant to this batch"""
    out_path = os.path.join(SAMPLE_OUTPUT_DIR, "l2", "dim_product.csv")
    with _pg_conn() as conn:
        try:
            df = pd.read_sql_query(
                "select * from l2.dim_product where last_ingested_ds::date = %s::date order by product_id",
                conn, params=[exec_ds]
            )
            if df.empty:
                df = pd.read_sql_query("select * from l2.dim_product order by product_id", conn)
        except Exception:
            df = pd.read_sql_query("select * from l2.dim_product order by product_id", conn)
        df.to_csv(out_path, index=False)

def _export_l1_tables(exec_ds: str, **_):
    """Export per-table L1 CSVs filtered by ds to keep sample sizes small and reproducible."""
    out_dir = os.path.join(SAMPLE_OUTPUT_DIR, "l1")
    with _pg_conn() as conn:
        for t in RESOURCES:
            sql = f'select * from l1."{t}" where ds::date = %s::date'
            df = pd.read_sql_query(sql, conn, params=[exec_ds])
            out_path = os.path.join(out_dir, f"l1_{t}.csv")
            df.to_csv(out_path, index=False)

def _copy_dq_reports(ds_val: str, **_):
    """Copy dbt target artifacts --> sample_output/dq-reports for offline review."""
    src_dir = "/dbt/target"
    out_dir = os.path.join(SAMPLE_OUTPUT_DIR, "dq-reports")
    files = ["run_results.json", "manifest.json", "catalog.json", "index.html"]
    for fname in files:
        src = os.path.join(src_dir, fname)
        if os.path.exists(src):
            with open(src, "rb") as fsrc, open(os.path.join(out_dir, fname), "wb") as fdst:
                fdst.write(fsrc.read())

def _download_first_from_minio(exec_ds: str, kind: str, **_):
    """
    Pull the first matching object per resource for the ds partition from MinIO.
    """
    assert kind in ("json", "parquet")
    minio_cfg = json.loads(Variable.get("MINIO_CONFIG"))
    client = ETLHelper.create_minio_client(minio_cfg)

    bucket_raw = minio_cfg.get("MINIO_BUCKET_RAW")
    bucket_stg = minio_cfg.get("MINIO_BUCKET_STG")

    # Root folders inside each bucket (per provided example layout).
    raw_root = "raw"
    stg_root = "staging"
    fakestore_base = "fakestore"

    yyyy, mm, dd = exec_ds.split("-")
    ds_token = f"_{exec_ds}"

    for res in RESOURCES:
        if kind == "json":
            bucket = bucket_raw
            prefixes = [
                f"{raw_root}/{res}/{yyyy}/{mm}/{dd}/",
                f"{fakestore_base}/{res}/ds={exec_ds}/",
            ]
            dest = os.path.join(SAMPLE_OUTPUT_DIR, "raw/json", f"{res}.json")
            suffix = ".json"
        else:
            bucket = bucket_stg
            prefixes = [
                f"{stg_root}/{res}/{yyyy}/{mm}/{dd}/",
                f"{fakestore_base}/{res}/ds={exec_ds}/",
            ]
            dest = os.path.join(SAMPLE_OUTPUT_DIR, "raw/parquet", f"{res}.parquet")
            suffix = ".parquet"

        picked = None
        tried_prefixes = []

        # Try each candidate prefix until we find a suitable object.
        for prefix in prefixes:
            tried_prefixes.append(prefix)
            logger.info("[deliverables] Listing MinIO bucket=%s prefix=%s kind=%s", bucket, prefix, kind)
            try:
                obj_iter = client.list_objects(bucket, prefix=prefix, recursive=True)
            except Exception as e:
                logger.warning("[deliverables] list_objects failed bucket=%s prefix=%s err=%s", bucket, prefix, e)
                continue

            candidates = []
            for obj in obj_iter:
                name = getattr(obj, "object_name", "")
                if not name.endswith(suffix):
                    continue
                candidates.append(name)

            if not candidates:
                continue

            # Prefer date-specific file; otherwise first matching candidate is OK.
            ds_specific = [n for n in candidates if ds_token in n]
            picked = (ds_specific[0] if ds_specific else candidates[0])
            logger.info("[deliverables] Picked object: bucket=%s key=%s", bucket, picked)
            break

        if not picked:
            logger.warning(
                "[deliverables] No %s object found for resource=%s ds=%s in bucket=%s (tried: %s)",
                kind, res, exec_ds, bucket, tried_prefixes
            )
            continue

        # Stream download then write to local disk.
        try:
            resp = client.get_object(bucket, picked)
            data = resp.read()
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(data)
        logger.info("[deliverables] Wrote %s bytes to %s", len(data), dest)

with DAG(
    dag_id="99_dag_fakestore_pack_deliverables",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "deliverables", "packaging"],
) as dag:

    # Allow passing {"ds": "..."} via dag_run.conf.
    ds_kw = {"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"}

    # Start anchors
    t00_start = EmptyOperator(task_id="t00_start")

    # Ensure the output folder structure exists before writing files.
    t05_prepare_dirs = PythonOperator(
        task_id="t05_prepare_dirs",
        python_callable=_ensure_dirs,
        op_kwargs={"ds_val": "{{ dag_run.conf.get('ds', ds) }}"},
    )

    # L2 exports (main deliverables)
    t10_export_fact_csv = PythonOperator(
        task_id="t10_export_fact_csv",
        python_callable=_export_fact_csv,
        op_kwargs=ds_kw,
    )
    t11_export_dim_csv = PythonOperator(
        task_id="t11_export_dim_csv",
        python_callable=_export_dim_csv,
        op_kwargs=ds_kw,
    )

    # Copy dbt target artifacts → dq-reports (run_results/manifest/catalog/docs)
    t20_copy_dq_reports = PythonOperator(
        task_id="t20_copy_dq_reports",
        python_callable=_copy_dq_reports,
        op_kwargs={"ds_val": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,  # still copy whatever is available
    )

    # L1 CSV exports for this ds
    t30_export_l1_tables = PythonOperator(
        task_id="t30_export_l1_tables",
        python_callable=_export_l1_tables,
        op_kwargs=ds_kw,
    )

    # Pull raw JSON & staging Parquet samples from MinIO
    t40_pull_raw_json = PythonOperator(
        task_id="t40_pull_raw_json",
        python_callable=_download_first_from_minio,
        op_kwargs={**ds_kw, "kind": "json"},
    )
    t41_pull_stg_parquet = PythonOperator(
        task_id="t41_pull_stg_parquet",
        python_callable=_download_first_from_minio,
        op_kwargs={**ds_kw, "kind": "parquet"},
    )

    # End anchor
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Flow: create dirs → run exports in parallel → finish
    t00_start >> t05_prepare_dirs
    t05_prepare_dirs >> [t10_export_fact_csv, t11_export_dim_csv, t20_copy_dq_reports, t30_export_l1_tables, t40_pull_raw_json, t41_pull_stg_parquet] >> t90_finish
