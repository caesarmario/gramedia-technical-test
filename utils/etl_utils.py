####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for ETL ops.
####

from __future__ import annotations

import json
import pandas as pd

from typing import Any, Dict, List, Optional, Tuple
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


    # ---------- Paths / object names ----------

    @staticmethod
    def build_parquet_object_name(resource: str, ds: str, layer: str = "staging") -> str:
        """
        s3 key: <layer>/<resource>/YYYY/MM/DD/<resource>_<ds>.parquet
        """
        try:
            y, m, d = ds.split("-")
            assert len(y) == 4 and len(m) == 2 and len(d) == 2
        except Exception:
            raise ValueError("ds must be YYYY-MM-DD, e.g., 2025-11-07")
        return f"{layer}/{resource}/{y}/{m}/{d}/{resource}_{ds}.parquet"

    # ---------- MinIO read helpers ----------

    @staticmethod
    def get_bytes_object(client: "Minio", bucket: str, object_name: str) -> bytes:
        resp = client.get_object(bucket, object_name)
        try:
            data = resp.read()
        finally:
            resp.close()
            resp.release_conn()
        return data

    @staticmethod
    def get_json_object(client: "Minio", bucket: str, object_name: str) -> Any:
        raw = ETLHelper.get_bytes_object(client, bucket, object_name)
        return json.loads(raw.decode("utf-8"))


    @staticmethod
    def records_to_dataframe(records: Any) -> pd.DataFrame:
        if records is None:
            return pd.DataFrame()
        if isinstance(records, dict):
            records = [records]
        return pd.json_normalize(records, sep=".")


    @staticmethod
    def _apply_explode(df: pd.DataFrame, list_path: Optional[str]) -> pd.DataFrame:
        if not list_path:
            return df

        if list_path not in df.columns:
            return df

        exploded = df.explode(list_path, ignore_index=True)
        sub = pd.json_normalize(exploded[list_path]).add_prefix(f"{list_path}.")
        exploded = exploded.drop(columns=[list_path])
        exploded = pd.concat([exploded.reset_index(drop=True), sub.reset_index(drop=True)], axis=1)
        return exploded


    @staticmethod
    def _select_and_rename(df: pd.DataFrame, columns_cfg: Any) -> pd.DataFrame:
        if not columns_cfg:
            return df

        if isinstance(columns_cfg, list):
            keep = [c for c in columns_cfg if c in df.columns]
            return df[keep]

        if isinstance(columns_cfg, dict):
            pairs = [(new, src) for new, src in columns_cfg.items() if src in df.columns]
            out = {}
            for new, src in pairs:
                out[new] = df[src]
            return pd.DataFrame(out)

        return df


    @staticmethod
    def _coerce_dtypes(df: pd.DataFrame, dtypes_map: Optional[Dict[str, str]]) -> pd.DataFrame:
        if not dtypes_map:
            return df

        for col, typ in dtypes_map.items():
            if col not in df.columns:
                continue
            try:
                if typ.startswith("datetime"):
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
                elif typ in {"int", "int64"}:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif typ in {"float", "float64"}:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif typ in {"bool", "boolean"}:
                    df[col] = df[col].astype("boolean")
                elif typ in {"string", "str", "object"}:
                    df[col] = df[col].astype("string")
                else:
                    # fallback: let pandas try
                    df[col] = df[col].astype(typ)
            except Exception:
                # keep original if cast fails
                pass
        return df


    @staticmethod
    def _apply_simple_transforms(df: pd.DataFrame, tfm: Optional[Dict[str, Any]]) -> pd.DataFrame:
        if not tfm:
            return df

        if isinstance(tfm.get("drop"), list):
            cols = [c for c in tfm["drop"] if c in df.columns]
            df = df.drop(columns=cols)

        if isinstance(tfm.get("fillna"), dict):
            for c, v in tfm["fillna"].items():
                if c in df.columns:
                    df[c] = df[c].fillna(v)

        if isinstance(tfm.get("lowercase"), list):
            for c in tfm["lowercase"]:
                if c in df.columns:
                    df[c] = df[c].astype("string").str.lower()

        if isinstance(tfm.get("strip"), list):
            for c in tfm["strip"]:
                if c in df.columns:
                    df[c] = df[c].astype("string").str.strip()

        if isinstance(tfm.get("derive_len"), list):
            for c in tfm["derive_len"]:
                if c in df.columns:
                    df[f"{c}_len"] = df[c].astype("string").str.len()

        if isinstance(tfm.get("deduplicate_on"), list) and tfm["deduplicate_on"]:
            subset = [c for c in tfm["deduplicate_on"] if c in df.columns]
            if subset:
                df = df.drop_duplicates(subset=subset, keep="first", ignore_index=True)

        return df


    @staticmethod
    def apply_config(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        explode_col = cfg.get("explode")
        df = ETLHelper._apply_explode(df, explode_col)

        df = ETLHelper._select_and_rename(df, cfg.get("columns"))
        df = ETLHelper._coerce_dtypes(df, cfg.get("dtypes"))
        df = ETLHelper._apply_simple_transforms(df, cfg.get("transforms"))
        return df


    @staticmethod
    def put_parquet(client: "Minio", bucket: str, object_name: str, df: pd.DataFrame) -> None:
        buf = BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=buf,
            length=buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )