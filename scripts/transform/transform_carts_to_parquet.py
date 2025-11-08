####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Transform "carts" JSON (raw) to Parquet (staging) using config
####

import argparse
import json
from datetime import datetime

import pandas as pd

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper


def main() -> None:
    # --- CLI: parse required/optional arguments ---
    parser = argparse.ArgumentParser(
        description="Transform 'carts' JSON (raw) -> Parquet (staging)"
    )
    parser.add_argument(
        "--ds", type=str, required=False,
        help="Execution date YYYY-MM-DD (default: today UTC)"
    )
    parser.add_argument(
        "--credentials", type=str, required=True,
        help="MinIO credentials as JSON string"
    )
    parser.add_argument(
        "--config-file", type=str, required=False,
        default="schema_config/fakestore_raw/carts_schema_config.json",
        help="Path to transformation config JSON"
    )
    args = parser.parse_args()

    # --- Resolve ds & parse creds JSON (fail-fast with context) ---
    ds = args.ds or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        creds = json.loads(args.credentials)
        logger.info("Resolved ds=%s; parsed MinIO credentials.", ds)
    except Exception as e:
        logger.exception("Invalid --credentials JSON.")
        raise SystemExit(f"Invalid --credentials JSON: {e}")

    # --- Buckets (validate presence early) ---
    bucket_raw = creds.get("MINIO_BUCKET_RAW")
    bucket_stg = creds.get("MINIO_BUCKET_STG")
    if not bucket_raw or not bucket_stg:
        logger.error("Missing bucket config. MINIO_BUCKET_RAW=%s MINIO_BUCKET_STG=%s", bucket_raw, bucket_stg)
        raise SystemExit("MINIO_BUCKET_RAW and MINIO_BUCKET_STG must be provided in --credentials")

    # --- MinIO client (network/config errors surface here) ---
    try:
        client = ETLHelper.create_minio_client(creds)
    except Exception:
        logger.exception("Failed to initialize MinIO client.")
        raise SystemExit(1)

    # --- Object keys (raw input & staging output) ---
    resource = "carts"
    try:
        raw_key  = ETLHelper.build_object_name(resource, ds)             # raw/<res>/YYYY/MM/DD/<res>_<ds>.json
        out_key  = ETLHelper.build_parquet_object_name(resource, ds, layer="staging")
        logger.info("[%s] Input key=%s | Output key=%s", resource, raw_key, out_key)
    except Exception:
        logger.exception("Failed to build object keys for resource=%s ds=%s", resource, ds)
        raise SystemExit(1)

    logger.info("[%s] Read raw JSON: s3://%s/%s", resource, bucket_raw, raw_key)

    # --- Read raw JSON from MinIO (with defensive parsing) ---
    try:
        raw_payload = ETLHelper.get_json_object(client, bucket_raw, raw_key)
    except Exception:
        logger.exception("[%s] Failed to fetch JSON from MinIO (bucket=%s key=%s)", resource, bucket_raw, raw_key)
        raise SystemExit(1)

    try:
        records = raw_payload.get("data", raw_payload)
        # Normalize: records should be list-like or dict-like; log size for visibility
        rec_count = len(records) if hasattr(records, "__len__") else 1
        logger.info("[%s] Raw records loaded: %s", resource, rec_count)
    except Exception:
        logger.exception("[%s] Unexpected raw payload format.", resource)
        raise SystemExit(1)

    # --- Load transformation config (schema/renames/casts/etc.) ---
    try:
        with open(args.config_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("Loaded transform config: %s", args.config_file)
    except Exception:
        logger.exception("Failed to open/parse config file: %s", args.config_file)
        raise SystemExit(1)

    # --- Transform → DataFrame (templated branch for carts) ---
    try:
        df = ETLHelper.records_to_dataframe(records)
        # Carts: explode products into row-per-item, then apply carts-specific config
        df = ETLHelper._explode_carts_products(df)
        df = ETLHelper.apply_config_carts(df, cfg)
        # Partition column for downstream loads
        df["ds"] = pd.to_datetime(ds)
        logger.info("[%s] Transformed rows: %s | Columns: %s", resource, len(df), list(df.columns))
    except Exception:
        logger.exception("[%s] Transformation step failed.", resource)
        raise SystemExit(1)

    # --- Write Parquet to staging (ensure bucket, then upload) ---
    try:
        ETLHelper.ensure_bucket(client, bucket_stg)
        ETLHelper.put_parquet(client, bucket_stg, out_key, df)
    except Exception:
        logger.exception("[%s] Failed to write Parquet to s3://%s/%s", resource, bucket_stg, out_key)
        raise SystemExit(1)

    logger.info("[%s] Wrote Parquet: s3://%s/%s (rows=%s)", resource, bucket_stg, out_key, len(df))


if __name__ == "__main__":
    # Keep entrypoint minimal; let main() handle all orchestration & error paths.
    main()