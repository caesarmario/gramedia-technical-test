####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Load "carts" Parquet (staging/MinIO) → PostgreSQL L1 (psycopg2)
####

import argparse
import json
from datetime import datetime
import pandas as pd

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper
from utils.validation_utils import run_basic_checks


def main() -> None:
    # --- CLI: parse runtime args (ds/minio creds/pg creds/target schema/load mode) ---
    parser = argparse.ArgumentParser(
        description="Load 'carts' Parquet from MinIO → Postgres L1 (psycopg2)"
    )
    parser.add_argument("--ds", type=str, required=False,
                        help="Execution date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--minio-credentials", type=str, required=True,
                        help="MinIO credentials as JSON string")
    parser.add_argument("--pg-credentials", type=str, required=True,
                        help="PostgreSQL credentials as JSON string")
    parser.add_argument("--config-file", type=str, required=False,
                        default="schema_config/fakestore_raw/carts_schema_config.json",
                        help="Path to transform/selection config JSON (for DQ checks)")
    parser.add_argument("--target-schema", type=str, default="l1",
                        help="Target schema (default: l1)")
    parser.add_argument("--load-mode", type=str, default="replace_partition",
                        choices=["replace_partition", "truncate_insert", "upsert"],
                        help="replace_partition=DELETE WHERE ds; truncate_insert=TRUNCATE; upsert=ON CONFLICT")
    args = parser.parse_args()

    # --- Resolve execution date (defaults to today in UTC) ---
    ds = args.ds or datetime.utcnow().strftime("%Y-%m-%d")

    # --- Parse JSON credentials (fail fast if malformed) ---
    try:
        minio_creds = json.loads(args.minio_credentials)
        pg_creds    = json.loads(args.pg_credentials)
    except Exception as e:
        logger.exception("Invalid JSON creds. minio(len)=%s pg(len)=%s",
                         len(args.minio_credentials or ""), len(args.pg_credentials or ""))
        raise SystemExit(f"Invalid JSON creds: {e}")

    # --- Determine staging bucket (supporting both legacy/new key names) ---
    bucket_stg = minio_creds.get("MINIO_BUCKET_STG") or minio_creds.get("MINIO_BUCKET_STAGING")
    if not bucket_stg:
        logger.error("MINIO_BUCKET_STG / MINIO_BUCKET_STAGING missing in --minio-credentials")
        raise SystemExit("MINIO_BUCKET_STG / MINIO_BUCKET_STAGING missing in --minio-credentials")

    # --- Build MinIO client & object key for the given resource/date ---
    try:
        client  = ETLHelper.create_minio_client(minio_creds)
        res     = "carts"
        obj_key = ETLHelper.build_parquet_object_name(res, ds, layer="staging")
        logger.info("[%s] Read Parquet: s3://%s/%s", res, bucket_stg, obj_key)
    except Exception:
        logger.exception("Failed to init MinIO client or build parquet object name (ds=%s)", ds)
        raise SystemExit(1)

    # --- Read parquet from MinIO (staging) into a DataFrame ---
    try:
        df: pd.DataFrame = ETLHelper.read_parquet_from_minio(client, bucket_stg, obj_key)
    except Exception:
        logger.exception("[%s] Failed to read parquet from MinIO (bucket=%s key=%s)", res, bucket_stg, obj_key)
        raise SystemExit(1)

    if df.empty:
        logger.warning("[%s] Empty dataframe. Nothing to load.", res)
        return

    # --- Load transform/validation config and run lightweight DQ checks ---
    try:
        with open(args.config_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        logger.exception("Failed to open/parse config file: %s", args.config_file)
        raise SystemExit(1)

    try:
        run_basic_checks(resource=res, df=df, config=cfg)
    except Exception:
        logger.exception("[%s] Basic DQ checks failed.", res)
        raise SystemExit(1)

    # --- Target schema/table & primary key selection (templated) ---
    schema = args.target_schema
    table  = res

    pk = ["cart_id", "product_id"]

    # --- Connect to Postgres and load according to the selected mode ---
    conn = None
    try:
        conn = ETLHelper.create_pg_conn(pg_creds)
        logger.info("[%s] Connected to Postgres. Target: %s.%s (mode=%s)", res, schema, table, args.load_mode)

        # Ensure target schema/table match current dataframe (add columns if needed)
        ETLHelper.ensure_schema_psycopg2(conn, schema)
        ETLHelper.sync_table_schema_psycopg2(
            conn, schema, table, df,
            ensure_unique_on=pk if args.load_mode == "upsert" else None
        )

        # Mode: replace_partition → delete ds partition then insert
        if args.load_mode == "replace_partition":
            try:
                ETLHelper.delete_partition_psycopg2(conn, schema, table, {"ds": ds})
                ETLHelper.insert_dataframe_psycopg2(conn, schema, table, df)
            except Exception:
                logger.exception("[%s] replace_partition failed during delete/insert.", res)
                raise

        # Mode: truncate_insert → truncate table then bulk insert
        elif args.load_mode == "truncate_insert":
            try:
                ETLHelper.truncate_then_insert_psycopg2(conn, schema, table, df)
            except Exception:
                logger.exception("[%s] truncate_insert failed during truncate/insert.", res)
                raise

        # Mode: upsert → ON CONFLICT (pk) DO UPDATE
        else:
            try:
                ETLHelper.upsert_dataframe_psycopg2(conn, schema, table, df, pk)
            except Exception:
                logger.exception("[%s] upsert failed (pk=%s).", res, pk)
                raise

        logger.info("[%s] Loaded rows: %s → %s.%s", res, len(df), schema, table)

    except SystemExit:
        # Clean exits: re-raise without double-logging
        raise
    except Exception:
        logger.exception("[%s] Load to Postgres failed. schema.table=%s.%s", res, schema, table)
        raise SystemExit(1)
    finally:
        # Always close connection when opened
        try:
            if conn is not None:
                conn.close()
                logger.debug("Postgres connection closed.")
        except Exception:
            logger.warning("Failed to close Postgres connection cleanly.", exc_info=True)


if __name__ == "__main__":
    # Keep the entrypoint minimal and predictable
    main()