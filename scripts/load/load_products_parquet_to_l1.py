####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Load "products" Parquet (staging/MinIO) → PostgreSQL L1 (psycopg2)
####

import argparse
import json
from datetime import datetime
import pandas as pd

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper
from utils.validation_utils import run_basic_checks


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load 'products' Parquet from MinIO → Postgres L1 (psycopg2)"
    )
    parser.add_argument("--ds", type=str, required=False,
                        help="Execution date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--minio-credentials", type=str, required=True,
                        help="MinIO credentials as JSON string")
    parser.add_argument("--pg-credentials", type=str, required=True,
                        help="PostgreSQL credentials as JSON string")
    parser.add_argument("--config-file", type=str, required=False,
                        default="schema_config/fakestore_raw/products_schema_config.json",
                        help="Path to transform/selection config JSON (for DQ checks)")
    parser.add_argument("--target-schema", type=str, default="l1",
                        help="Target schema (default: l1)")
    parser.add_argument("--load-mode", type=str, default="replace_partition",
                        choices=["replace_partition", "truncate_insert", "upsert"],
                        help="replace_partition=DELETE WHERE ds; truncate_insert=TRUNCATE; upsert=ON CONFLICT")
    args = parser.parse_args()

    ds = args.ds or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        minio_creds = json.loads(args.minio_credentials)
        pg_creds    = json.loads(args.pg_credentials)
    except Exception as e:
        raise SystemExit(f"Invalid JSON creds: {e}")

    # --- Read parquet from MinIO (staging) ---
    bucket_stg = minio_creds.get("MINIO_BUCKET_STG") or minio_creds.get("MINIO_BUCKET_STAGING")
    if not bucket_stg:
        raise SystemExit("MINIO_BUCKET_STG / MINIO_BUCKET_STAGING missing in --minio-credentials")

    client  = ETLHelper.create_minio_client(minio_creds)
    res     = "products"
    obj_key = ETLHelper.build_parquet_object_name(res, ds, layer="staging")

    logger.info("[%s] Read Parquet: s3://%s/%s", res, bucket_stg, obj_key)
    df: pd.DataFrame = ETLHelper.read_parquet_from_minio(client, bucket_stg, obj_key)
    if df.empty:
        logger.warning("[%s] Empty dataframe. Nothing to load.", res)
        return

    # --- Light DQ checks ---
    with open(args.config_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    run_basic_checks(resource=res, df=df, config=cfg)

    # --- Load to Postgres (psycopg2) ---
    schema = args.target_schema
    table  = res

    pk = ["product_id"]

    conn = ETLHelper.create_pg_conn(pg_creds)
    try:
        ETLHelper.ensure_schema_psycopg2(conn, schema)
        ETLHelper.sync_table_schema_psycopg2(conn, schema, table, df, ensure_unique_on=pk if args.load_mode=="upsert" else None)

        if args.load_mode == "replace_partition":
            ETLHelper.delete_partition_psycopg2(conn, schema, table, {"ds": ds})
            ETLHelper.insert_dataframe_psycopg2(conn, schema, table, df)
        elif args.load_mode == "truncate_insert":
            ETLHelper.truncate_then_insert_psycopg2(conn, schema, table, df)
        else:  # upsert
            ETLHelper.upsert_dataframe_psycopg2(conn, schema, table, df, pk)

        logger.info("[%s] Loaded rows: %s → %s.%s", res, len(df), schema, table)
    finally:
        conn.close()


if __name__ == "__main__":
    main()