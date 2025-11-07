####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for ETL ops.
####

from __future__ import annotations

import json

from typing import Any, Dict
from datetime import datetime
from io import BytesIO
from minio import Minio

class ETLHelper:

    @staticmethod
    def build_url(base_url: str, endpoint_path: str) -> str:
        return f"{base_url.rstrip('/')}/{endpoint_path.lstrip('/')}"


    @staticmethod
    def build_object_name(resource: str, ds: str) -> str:
        """
        s3 key: raw/<resource>/YYYY/MM/DD/<resource>_<ds>.json
        """
        try:
            y, m, d = ds.split("-")
            assert len(y) == 4 and len(m) == 2 and len(d) == 2
        except Exception:
            raise ValueError("ds must be YYYY-MM-DD, e.g., 2025-11-06")
        return f"raw/{resource}/{y}/{m}/{d}/{resource}_{ds}.json"


    @staticmethod
    def create_minio_client(creds: Dict[str, str]) -> Minio:
        secure = str(creds.get("MINIO_SECURE", "false")).lower() == "true"
        return Minio(
            endpoint=creds["MINIO_ENDPOINT"],
            access_key=creds["MINIO_ROOT_USER"],
            secret_key=creds["MINIO_ROOT_PASSWORD"],
            secure=secure,
        )


    @staticmethod
    def ensure_bucket(client: Minio, bucket: str) -> None:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


    @staticmethod
    def put_json(client: Minio, bucket: str, object_name: str, payload: Any) -> None:
        buf = BytesIO(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=buf,
            length=buf.getbuffer().nbytes,
            content_type="application/json",
        )