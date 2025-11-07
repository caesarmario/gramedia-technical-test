####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Python Extractor `users` from FakeStore API
####

import argparse
import json

from datetime import datetime

from utils.logging_utils import logger
from utils.api_utils import APIHelper
from utils.etl_utils import ETLHelper

def main():
    parser = argparse.ArgumentParser(
        description="Extract 'users' (FakeStore API) to MinIO as JSON"
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
        "--base-url", type=str, required=False, default="https://fakestoreapi.com",
        help="Base URL (default: https://fakestoreapi.com)"
    )
    args = parser.parse_args()

    # Resolve ds and credentials
    ds = args.ds or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        creds = json.loads(args.credentials)
    except Exception as e:
        raise SystemExit(f"Invalid --credentials JSON: {e}")

    resource_name = "users"
    endpoint_path = "users"

    # -------- Orchestration (build → fetch → write) --------
    url = ETLHelper.build_url(args.base_url, endpoint_path)
    logger.info("[%s] GET %s", resource_name, url)

    data = APIHelper.get_json(url)
    payload = {
        "meta": {
            "source": url,
            "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "count": len(data) if isinstance(data, list) else 1,
        },
        "data": data,
    }

    bucket_raw = creds.get("MINIO_BUCKET_RAW")
    object_name = ETLHelper.build_object_name(resource_name, ds)
    client = ETLHelper.create_minio_client(creds)
    ETLHelper.ensure_bucket(client, bucket_raw)
    ETLHelper.put_json(client, bucket_raw, object_name, payload)

    logger.info("[%s] Uploaded → s3://%s/%s", resource_name, bucket_raw, object_name)


if __name__ == "__main__":
    main()