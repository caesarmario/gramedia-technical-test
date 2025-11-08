####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG: Pack deliverables → write LOCAL samples under /opt/project/assets/sample_output/*
## Outputs (per ds):
## - assets/sample_output/l2/cleaned_data.csv , dim_product.csv
## - assets/sample_output/l1/l1_<table>.csv  (products, carts, users)
## - assets/sample_output/dq-reports/{run_results.json, manifest.json, catalog.json, index.html}
## - assets/sample_output/raw/json/<resource>.json   (first object from MinIO ds partition)
## - assets/sample_output/raw/parquet/<resource>.parquet (first object from MinIO ds partition)
####

import os, io, json
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from utils.etl_utils import ETLHelper
from utils.logging_utils import logger


# -------- Defaults & constants --------
default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=1)}

# Base local output dir (inside project)
SAMPLE_OUTPUT_DIR: str = Variable.get(
    "SAMPLE_OUTPUT_DIR",
    "/opt/project/assets/sample_output",
)

# Fakestore resources (3 files expected for raw json/parquet, and L1 tables)
RESOURCES = json.loads(Variable.get("FAKESTORE_RESOURCES", '["products","carts","users"]'))

# -------- Helpers --------
def _ensure_dirs(ds_val: str, **_):
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
    pg_cfg = json.loads(Variable.get("POSTGRESQL_CONFIG"))
    return ETLHelper.create_pg_conn({
        "POSTGRES_HOST": pg_cfg["POSTGRES_HOST"],
        "POSTGRES_PORT": pg_cfg["POSTGRES_PORT"],
        "POSTGRES_USER": pg_cfg["POSTGRES_USER"],
        "POSTGRES_PASSWORD": pg_cfg["POSTGRES_PASSWORD"],
        "POSTGRES_DB": pg_cfg["POSTGRES_DB"],
    })

def _export_fact_csv(exec_ds: str, **_):
    out_path = os.path.join(SAMPLE_OUTPUT_DIR, "l2", "cleaned_data.csv")
    with _pg_conn() as conn:
        df = pd.read_sql_query(
            "select * from l2.fact_sales where load_ds::date = %s::date order by transaction_date, product_id",
            conn, params=[exec_ds]
        )
        df.to_csv(out_path, index=False)

def _export_dim_csv(exec_ds: str, **_):
    out_path = os.path.join(SAMPLE_OUTPUT_DIR, "l2", "dim_product.csv")
    with _pg_conn() as conn:
        # keep only rows relevant to this batch if available
        # falls back to all when column not present
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
    out_dir = os.path.join(SAMPLE_OUTPUT_DIR, "l1")
    with _pg_conn() as conn:
        for t in RESOURCES:
            sql = f'select * from l1."{t}" where ds::date = %s::date'
            df = pd.read_sql_query(sql, conn, params=[exec_ds])
            out_path = os.path.join(out_dir, f"l1_{t}.csv")
            df.to_csv(out_path, index=False)

def _copy_dq_reports(ds_val: str, **_):
    """Copy local dbt target artifacts into sample_output/dq-reports."""
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
    Download first object per resource for the given ds from MinIO into:
      - raw/json/<resource>.json     if kind="json"
      - raw/parquet/<resource>.parquet if kind="parquet"

    Supports two layouts:
      1) Hierarchical date:   <root>/{raw|staging}/<res>/YYYY/MM/DD/<file>
      2) ds-partition style:  <root>/<res>/ds=YYYY-MM-DD/<file>
    """
    assert kind in ("json", "parquet")
    minio_cfg = json.loads(Variable.get("MINIO_CONFIG"))
    client = ETLHelper.create_minio_client(minio_cfg)

    bucket_raw = minio_cfg.get("MINIO_BUCKET_RAW")
    bucket_stg = minio_cfg.get("MINIO_BUCKET_STG")

    # Root folders inside bucket (per your sample)
    raw_root = "raw"        # raw-fakestore/**raw**/
    stg_root = "staging"    # stg-fakestore/**staging**/
    # Optional older style (ds=...) base
    fakestore_base = "fakestore"

    yyyy, mm, dd = exec_ds.split("-")
    ds_token = f"_{exec_ds}"  # e.g., _2025-11-07 to prefer the right file

    for res in RESOURCES:
        if kind == "json":
            bucket = bucket_raw
            # Candidate prefixes in PRIORITY order
            prefixes = [
                f"{raw_root}/{res}/{yyyy}/{mm}/{dd}/",   # raw/<res>/YYYY/MM/DD/
                f"{fakestore_base}/{res}/ds={exec_ds}/", # fakestore/<res>/ds=YYYY-MM-DD/
            ]
            dest = os.path.join(SAMPLE_OUTPUT_DIR, "raw/json", f"{res}.json")
            suffix = ".json"
        else:
            bucket = bucket_stg
            prefixes = [
                f"{stg_root}/{res}/{yyyy}/{mm}/{dd}/",   # staging/<res>/YYYY/MM/DD/
                f"{fakestore_base}/{res}/ds={exec_ds}/", # fakestore/<res>/ds=YYYY-MM-DD/
            ]
            dest = os.path.join(SAMPLE_OUTPUT_DIR, "raw/parquet", f"{res}.parquet")
            suffix = ".parquet"

        picked = None
        tried_prefixes = []

        for prefix in prefixes:
            tried_prefixes.append(prefix)
            logger.info("[deliverables] Listing MinIO bucket=%s prefix=%s kind=%s", bucket, prefix, kind)
            try:
                obj_iter = client.list_objects(bucket, prefix=prefix, recursive=True)
            except Exception as e:
                logger.warning("[deliverables] list_objects failed bucket=%s prefix=%s err=%s", bucket, prefix, e)
                continue

            # Prefer file that contains _YYYY-MM-DD in its name (e.g., carts_2025-11-07.json)
            candidates = []
            for obj in obj_iter:
                name = getattr(obj, "object_name", "")
                if not name.endswith(suffix):
                    continue
                candidates.append(name)

            if not candidates:
                continue

            # Prefer DS-specific file; else pick the first
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

        # Download
        try:
            resp = client.get_object(bucket, picked)
            data = resp.read()
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

        # Write locally
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(data)
        logger.info("[deliverables] Wrote %s bytes to %s", len(data), dest)

with DAG(
    dag_id="99_dag_fakestore_pack_deliverables",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # on-demand (manual trigger)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "deliverables", "packaging"],
) as dag:

    # ds passed via conf (e.g., {"ds": "2025-11-07"}), fallback to rendered {{ ds }}
    ds_kw = {"exec_ds": "{{ dag_run.conf.get('ds', ds) }}"}

    t00_start = EmptyOperator(task_id="t00_start")

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

    # DQ reports copy (from local /dbt/target)
    t20_copy_dq_reports = PythonOperator(
        task_id="t20_copy_dq_reports",
        python_callable=_copy_dq_reports,
        op_kwargs={"ds_val": "{{ dag_run.conf.get('ds', ds) }}"},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # L1 samples (per ds)
    t30_export_l1_tables = PythonOperator(
        task_id="t30_export_l1_tables",
        python_callable=_export_l1_tables,
        op_kwargs=ds_kw,
    )

    # Raw JSON & Staging Parquet (first file per resource for the ds partition)
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

    t90_finish = EmptyOperator(task_id="t90_finish")

    # chaining
    t00_start >> t05_prepare_dirs
    t05_prepare_dirs >> [t10_export_fact_csv, t11_export_dim_csv, t20_copy_dq_reports, t30_export_l1_tables, t40_pull_raw_json, t41_pull_stg_parquet] >> t90_finish
