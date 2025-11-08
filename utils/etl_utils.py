####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for ETL ops.
####

from __future__ import annotations

import json
import pandas as pd
import ast
import re
import psycopg2

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from datetime import datetime
from io import BytesIO
from minio import Minio
from psycopg2 import sql
from psycopg2.extras import execute_values

from utils.logging_utils import logger

class ETLHelper:
    """
    Collection of static helpers used across extract/transform/load steps.
    """

    @staticmethod
    def build_url(base_url: str, endpoint_path: str) -> str:
        """Join base URL and endpoint path with a single slash.
        Example: ("https://api", "/v1/items") -> "https://api/v1/items".
        """
        out = f"{base_url.rstrip('/')}/{endpoint_path.lstrip('/')}"
        logger.debug("[ETLHelper.build_url] %s", out)
        return out


    @staticmethod
    def build_object_name(resource: str, ds: str) -> str:
        """Build RAW layer S3 key: raw/<resource>/YYYY/MM/DD/<resource>_<ds>.json.
        Validates ds format = YYYY-MM-DD
        """
        try:
            y, m, d = ds.split("-")
            assert len(y) == 4 and len(m) == 2 and len(d) == 2
        except Exception:
            logger.error("[ETLHelper.build_object_name] Invalid ds: %s", ds)
            raise ValueError("ds must be YYYY-MM-DD, e.g., 2025-11-06")
        key = f"raw/{resource}/{y}/{m}/{d}/{resource}_{ds}.json"
        logger.debug("[ETLHelper.build_object_name] key=%s", key)
        return key


    @staticmethod
    def create_minio_client(creds: Dict[str, str]) -> Minio:
        """Instantiate a MinIO client from credentials dict.
        Required keys: MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_SECURE (optional).
        """
        try:
            secure = str(creds.get("MINIO_SECURE", "false")).lower() == "true"
            client = Minio(
                endpoint=creds["MINIO_ENDPOINT"],
                access_key=creds["MINIO_ROOT_USER"],
                secret_key=creds["MINIO_ROOT_PASSWORD"],
                secure=secure,
            )
            logger.debug("[ETLHelper.create_minio_client] endpoint=%s secure=%s", creds.get("MINIO_ENDPOINT"), secure)
            return client
        except Exception as e:
            logger.exception("[ETLHelper.create_minio_client] Failed to init MinIO client: %s", e)
            raise


    @staticmethod
    def ensure_bucket(client: Minio, bucket: str) -> None:
        """Create bucket if not exists."""
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info("[ETLHelper.ensure_bucket] Created bucket=%s", bucket)
            else:
                logger.debug("[ETLHelper.ensure_bucket] Bucket exists=%s", bucket)
        except Exception as e:
            logger.exception("[ETLHelper.ensure_bucket] Failed bucket=%s: %s", bucket, e)
            raise


    @staticmethod
    def put_json(client: Minio, bucket: str, object_name: str, payload: Any) -> None:
        """Upload JSON payload to MinIO at <bucket>/<object_name>."""
        try:
            buf = BytesIO(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=buf,
                length=buf.getbuffer().nbytes,
                content_type="application/json",
            )
            logger.info("[ETLHelper.put_json] Uploaded s3://%s/%s bytes=%s", bucket, object_name, buf.getbuffer().nbytes)
        except Exception as e:
            logger.exception("[ETLHelper.put_json] Failed s3://%s/%s: %s", bucket, object_name, e)
            raise


    @staticmethod
    def build_parquet_object_name(resource: str, ds: str, layer: str) -> str:
        """Build layer key: <layer>/<resource>/YYYY/MM/DD/<resource>_<ds>.parquet.
        Validates ds format = YYYY-MM-DD.
        """
        try:
            y, m, d = ds.split("-")
            assert len(y) == 4 and len(m) == 2 and len(d) == 2
        except Exception:
            logger.error("[ETLHelper.build_parquet_object_name] Invalid ds: %s", ds)
            raise ValueError("ds must be YYYY-MM-DD, e.g., 2025-11-07")
        key = f"{layer}/{resource}/{y}/{m}/{d}/{resource}_{ds}.parquet"
        logger.debug("[ETLHelper.build_parquet_object_name] key=%s", key)
        return key


    @staticmethod
    def get_bytes_object(client: "Minio", bucket: str, object_name: str) -> bytes:
        """Read object bytes from MinIO and return its content."""
        try:
            resp = client.get_object(bucket, object_name)
            try:
                data = resp.read()
                logger.info("[ETLHelper.get_bytes_object] Read s3://%s/%s bytes=%s", bucket, object_name, len(data))
                return data
            finally:
                resp.close(); resp.release_conn()
        except Exception as e:
            logger.exception("[ETLHelper.get_bytes_object] Failed s3://%s/%s: %s", bucket, object_name, e)
            raise


    @staticmethod
    def get_json_object(client: "Minio", bucket: str, object_name: str) -> Any:
        """Convenience wrapper: read bytes then parse JSON."""
        raw = ETLHelper.get_bytes_object(client, bucket, object_name)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as e:
            logger.exception("[ETLHelper.get_json_object] JSON decode failed for s3://%s/%s: %s", bucket, object_name, e)
            raise


    @staticmethod
    def records_to_dataframe(records: Sequence[Mapping[str, Any]], sep: str = ".") -> pd.DataFrame:
        """Normalize a list of records into a DataFrame; nested dicts become dotted columns.
        Lists are preserved for later explode. Returns empty DataFrame when no records.
        """
        if not records:
            logger.info("[ETLHelper.records_to_dataframe] Empty records -> empty DataFrame")
            return pd.DataFrame()
        df = pd.json_normalize(records, sep=sep)
        logger.debug("[ETLHelper.records_to_dataframe] Shape=%s", df.shape)
        return df
    

    @staticmethod
    def _safe_explode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Explode a column if it exists, coercing non-list values to singletons/[] safely."""
        if col not in df.columns:
            return df
        df = df.copy()
        df[col] = df[col].map(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))
        out = df.explode(col, ignore_index=True)
        logger.debug("[ETLHelper._safe_explode] col=%s before=%s after=%s", col, df.shape, out.shape)
        return out


    @staticmethod
    def put_parquet(client: "Minio", bucket: str, object_name: str, df: pd.DataFrame) -> None:
        """Write DataFrame as Parquet to MinIO at <bucket>/<object_name>."""
        try:
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
            logger.info("[ETLHelper.put_parquet] Uploaded s3://%s/%s rows=%s bytes=%s", bucket, object_name, len(df), buf.getbuffer().nbytes)
        except Exception as e:
            logger.exception("[ETLHelper.put_parquet] Failed s3://%s/%s: %s", bucket, object_name, e)
            raise

    
    @staticmethod
    def _flatten_dict_col(df: pd.DataFrame, col: str, prefix: Optional[str] = None) -> pd.DataFrame:
        """Flatten a dict-typed column into multiple columns with given prefix (safe on None)."""
        if col not in df.columns:
            return df
        if not df[col].apply(lambda x: isinstance(x, dict) or pd.isna(x)).all():
            return df
        out = df.copy()
        norm = pd.json_normalize(out[col].apply(lambda x: x or {}))
        pref = prefix or col
        norm.columns = [f"{pref}_{c}".replace(".", "_") for c in norm.columns]
        out = pd.concat([out.drop(columns=[col]), norm], axis=1)
        logger.debug("[ETLHelper._flatten_dict_col] col=%s -> new_cols=%s", col, list(norm.columns))
        return out
    

    @staticmethod
    def auto_flatten_dicts(df: pd.DataFrame, max_depth: int = 2) -> pd.DataFrame:
        """Iteratively flatten dict columns up to `max_depth`; ignores lists."""
        out = df.copy()
        for _ in range(max_depth):
            dict_cols = [c for c in out.columns if out[c].apply(lambda x: isinstance(x, dict)).any()]
            if not dict_cols:
                break
            for col in dict_cols:
                out = ETLHelper._flatten_dict_col(out, col)
        logger.debug("[ETLHelper.auto_flatten_dicts] max_depth=%s final_shape=%s", max_depth, out.shape)
        return out


    @staticmethod
    def _resolve_col(df: pd.DataFrame, name: str) -> Optional[str]:
        """Resolve a column name with dotted/underscored variants; returns actual column or None."""
        if name in df.columns:
            return name
        candidates = {name.replace(".", "_"), name.replace("_", ".")}
        for cand in candidates:
            if cand in df.columns:
                return cand
        return None


    @staticmethod
    def apply_config(df: pd.DataFrame, cfg: Mapping[str, Any], sep: str = ".") -> pd.DataFrame:
        """Project/reshape a DataFrame according to a permissive config format.
        Supports two styles:
          1) New style: cfg.select is {out_col: {path, type, default}}, plus derive/fillna/order.
          2) Old style: explode/flatten/rename/select(list)/casts/fillna/drop.
        """
        if df.empty:
            return df.copy()

        # 1) Mild auto-flatten for safety
        df = ETLHelper.auto_flatten_dicts(df, max_depth=2)

        sel = cfg.get("select", None)

        # --- New style: select as mapping ---
        if isinstance(sel, Mapping) and sel:
            out_cols: Dict[str, pd.Series] = {}

            for out_col, spec in sel.items():
                spec = spec or {}
                path = spec.get("path", out_col)
                typ  = (spec.get("type") or "").lower()
                dflt = spec.get("default", None)

                resolved = ETLHelper._resolve_col(df, path)
                if resolved:
                    s = df[resolved]
                else:
                    s = df.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)

                if typ:
                    s = ETLHelper._coerce(s, typ)
                if dflt is not None:
                    s = s.fillna(dflt)

                out_cols[out_col] = s

            out = pd.DataFrame(out_cols)

            # Derivations (tiny conveniences)
            derives = cfg.get("derive") or {}
            for new_col, rule in derives.items():
                op = (rule or {}).get("op"); args = (rule or {}).get("args") or []
                if op == "len" and args:
                    src = args[0]
                    if src in out.columns:
                        out[new_col] = out[src].astype("string").str.len()
                    elif src in df.columns:
                        out[new_col] = df[src].astype("string").str.len()

            # Fillna
            for c, v in (cfg.get("fillna") or {}).items():
                if c in out.columns:
                    out[c] = out[c].fillna(v)

            # Column order
            order = cfg.get("order") or []
            if order:
                ordered = [c for c in order if c in out.columns]
                tail = [c for c in out.columns if c not in ordered]
                out = out[ordered + tail]

            logger.debug("[ETLHelper.apply_config:new] out_shape=%s", out.shape)
            return out

        # --- Old style ---
        for col in (cfg.get("explode") or []):
            if col not in df.columns:
                continue
            df = ETLHelper._safe_explode(df, col)
            if df[col].map(lambda v: isinstance(v, Mapping)).any():
                expanded = pd.json_normalize(df[col].map(lambda v: v if isinstance(v, Mapping) else {}), sep=sep)
                expanded.columns = [f"{col}{sep}{c}" for c in expanded.columns]
                df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
            else:
                df = df.rename(columns={col: f"{col}_value"})
            df = ETLHelper.auto_flatten_dicts(df, max_depth=1)

        for path, outcol in (cfg.get("flatten") or {}).items():
            if outcol in df.columns:
                continue
            if path in df.columns:
                df[outcol] = df[path]
            else:
                df[outcol] = df.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)

        if cfg.get("rename"):
            actual = {k: v for k, v in cfg["rename"].items() if k in df.columns}
            if actual:
                df = df.rename(columns=actual)

        if isinstance(sel, list) and sel:
            keep: List[str] = []
            inv_rename = {v: k for k, v in (cfg.get("rename") or {}).items()}
            for c in sel:
                if c in df.columns:
                    keep.append(c); continue
                if c in (cfg.get("rename") or {}) and cfg["rename"][c] in df.columns:
                    keep.append(cfg["rename"][c]); continue
                if c in inv_rename and inv_rename[c] in df.columns:
                    keep.append(inv_rename[c]); continue
                for alt in (c.replace(".", "_"), c.replace("_", ".")):
                    if alt in df.columns:
                        keep.append(alt); break
            keep = [k for k in keep if k in df.columns]
            if keep:
                df = df[keep]

        type_map = {"int": "Int64", "float": "float64", "str": "string", "bool": "boolean"}
        for col, typ in (cfg.get("casts") or {}).items():
            if col not in df.columns:
                continue
            t = str(typ).lower()
            try:
                if t in ("timestamp", "datetime", "date"):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif t in type_map:
                    df[col] = df[col].astype(type_map[t])
            except Exception:
                # swallow cast issues, keep permissive
                pass

        for col, val in (cfg.get("fillna") or {}).items():
            if col in df.columns:
                df[col] = df[col].fillna(val)
        for col in (cfg.get("drop") or []):
            if col in df.columns:
                df = df.drop(columns=[col])

        logger.debug("[ETLHelper.apply_config:old] out_shape=%s", df.shape)
        return df


    @staticmethod
    def _get_by_path(obj: Any, path: str, sep: str = ".") -> Any:
        """Retrieve a nested value by dotted path (supports list indices).
        Example paths: "rating.rate", "products.0.productId". Returns None when missing.
        """
        cur = obj
        for key in path.split(sep):
            if cur is None:
                return None
            try:
                idx = int(key)
                if isinstance(cur, (list, tuple)):
                    cur = cur[idx] if 0 <= idx < len(cur) else None
                else:
                    return None
            except ValueError:
                if isinstance(cur, Mapping):
                    cur = cur.get(key)
                else:
                    return None
        return cur


    @staticmethod
    def _coerce(s: pd.Series, typ: str) -> pd.Series:
        """Coerce a pandas Series to a target logical type (lenient)."""
        t = str(typ).lower()
        if t in ("int", "int64", "integer"):
            return pd.to_numeric(s, errors="coerce").astype("Int64")
        if t in ("float", "float64", "double", "number"):
            return pd.to_numeric(s, errors="coerce")
        if t in ("bool", "boolean"):
            return s.map(lambda x: (bool(x) if pd.notna(x) else pd.NA)).astype("boolean")
        if t in ("datetime", "timestamp", "date"):
            return pd.to_datetime(s, errors="coerce", utc=False)
        if t in ("str", "string", "text"):
            return s.astype("string")
        return s
    

    @staticmethod
    def _explode_carts_products(df: pd.DataFrame) -> pd.DataFrame:
        """Carts-only: explode list of products -> one row per item, keep dict & flat variants.
        Handles None/NaN/json-string/literal forms defensively.
        """
        if "products" not in df.columns:
            return df.copy()

        base_cols = [c for c in df.columns if c != "products"]
        out_rows: list[dict] = []

        def _to_list(obj):
            if isinstance(obj, list):
                return obj
            if obj is None or (isinstance(obj, float) and pd.isna(obj)):
                return []
            if isinstance(obj, str):
                for parser in (json.loads, ast.literal_eval):
                    try:
                        val = parser(obj)
                        break
                    except Exception:
                        val = None
                if val is None:
                    return []
                return val if isinstance(val, list) else [val]
            return [obj]

        for _, rec in df.iterrows():
            base = {c: rec[c] for c in base_cols}
            items = _to_list(rec.get("products"))

            if not items:
                row = {**base, "products": {}, "products.productId": None, "products.quantity": None}
                out_rows.append(row)
                continue

            for it in items:
                if isinstance(it, str):
                    parsed = None
                    for parser in (json.loads, ast.literal_eval):
                        try:
                            parsed = parser(it)
                            break
                        except Exception:
                            parsed = None
                    it = parsed if isinstance(parsed, dict) else {}
                elif not isinstance(it, dict):
                    it = {}

                row = {
                    **base,
                    "products": it,
                    "products.productId": it.get("productId"),
                    "products.quantity": it.get("quantity"),
                }
                out_rows.append(row)

        out = pd.DataFrame(out_rows)
        if "products.productId" in out.columns:
            out["products.productId"] = pd.to_numeric(out["products.productId"], errors="coerce")
        if "products.quantity" in out.columns:
            out["products.quantity"] = pd.to_numeric(out["products.quantity"], errors="coerce")
        logger.debug("[ETLHelper._explode_carts_products] out_shape=%s", out.shape)
        return out


    @staticmethod
    def apply_config_carts(df: pd.DataFrame, cfg: Mapping[str, Any], sep: str = ".") -> pd.DataFrame:
        """Carts-only apply_config variant to avoid duplicate product fields and enforce order."""
        if df.empty:
            return df.copy()

        work = df.copy()
        if "products" in work.columns:
            work = work.drop(columns=["products"])  # avoid duplicate flattening later

        work = ETLHelper.auto_flatten_dicts(work, max_depth=2)

        col_specs: Dict[str, Dict[str, Any]] = {
            k: v for k, v in cfg.items() if not str(k).startswith("_") and isinstance(v, Mapping)
        }

        def _coerce_series(s: pd.Series, dtype: str) -> pd.Series:
            t = (dtype or "").strip().lower()
            if t in ("int", "int64", "integer"):
                return pd.to_numeric(s, errors="coerce").astype("Int64")
            if t in ("float", "float64", "double", "number"):
                return pd.to_numeric(s, errors="coerce")
            if t in ("bool", "boolean"):
                return s.map(lambda x: (bool(x) if pd.notna(x) else pd.NA)).astype("boolean")
            if t in ("datetime", "timestamp", "date"):
                return pd.to_datetime(s, errors="coerce", utc=False)
            if t in ("str", "string", "text"):
                return s.astype("string")
            return s

        out_cols: Dict[str, pd.Series] = {}
        for out_col, spec in col_specs.items():
            path = spec.get("mapping", out_col)
            dtype = spec.get("data_type", "")
            resolved = ETLHelper._resolve_col(work, path)
            if resolved:
                series = work[resolved]
            else:
                series = work.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)
            if dtype:
                series = _coerce_series(series, dtype)
            out_cols[out_col] = series

        out = pd.DataFrame(out_cols)

        cfg_order = (cfg.get("_config") or {}).get("order") or []
        if cfg_order:
            ordered = [c for c in cfg_order if c in out.columns]
            tail = [c for c in out.columns if c not in ordered]
            out = out[ordered + tail]
        else:
            preferred = ["cart_id", "user_id", "order_date", "product_id", "quantity", "ds"]
            ordered = [c for c in preferred if c in out.columns]
            tail = [c for c in out.columns if c not in ordered]
            out = out[ordered + tail]

        logger.debug("[ETLHelper.apply_config_carts] out_shape=%s", out.shape)
        return out


    @staticmethod
    def read_parquet_from_minio(client, bucket: str, object_name: str) -> pd.DataFrame:
        """Read a Parquet file from MinIO into a DataFrame."""
        try:
            resp = client.get_object(bucket, object_name)
            try:
                raw = resp.read()
                df = pd.read_parquet(BytesIO(raw))
                logger.info("[ETLHelper.read_parquet_from_minio] s3://%s/%s rows=%s", bucket, object_name, len(df))
                return df
            finally:
                resp.close(); resp.release_conn()
        except Exception as e:
            logger.exception("[ETLHelper.read_parquet_from_minio] Failed s3://%s/%s: %s", bucket, object_name, e)
            raise


    @staticmethod
    def create_pg_conn(creds: Dict):
        """Open a psycopg2 connection with autocommit enabled."""
        try:
            conn = psycopg2.connect(
                host=creds["POSTGRES_HOST"],
                port=int(creds.get("POSTGRES_PORT", 5432)),
                user=creds["POSTGRES_USER"],
                password=creds["POSTGRES_PASSWORD"],
                dbname=creds["POSTGRES_DB"],
            )
            conn.autocommit = True
            logger.debug("[ETLHelper.create_pg_conn] Connected host=%s db=%s", creds.get("POSTGRES_HOST"), creds.get("POSTGRES_DB"))
            return conn
        except Exception as e:
            logger.exception("[ETLHelper.create_pg_conn] Connection failed: %s", e)
            raise


    @staticmethod
    def _pg_dtype_from_series(s: pd.Series) -> str:
        """Map pandas dtype to a reasonable Postgres column type."""
        if pd.api.types.is_integer_dtype(s):     return "BIGINT"
        if pd.api.types.is_float_dtype(s):       return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(s):        return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(s): return "TIMESTAMP"
        return "TEXT"


    @staticmethod
    def ensure_schema_psycopg2(conn, schema: str) -> None:
        """CREATE SCHEMA IF NOT EXISTS <schema>."""
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {} ").format(sql.Identifier(schema)))
        logger.debug("[ETLHelper.ensure_schema_psycopg2] ensured schema=%s", schema)


    @staticmethod
    def _table_exists(conn, schema: str, table: str) -> bool:
        """Return True if <schema>.<table> exists."""
        q = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """
        with conn.cursor() as cur:
            cur.execute(q, (schema, table))
            exists = cur.fetchone() is not None
        logger.debug("[ETLHelper._table_exists] %s.%s exists=%s", schema, table, exists)
        return exists


    @staticmethod
    def _existing_columns(conn, schema: str, table: str) -> List[str]:
        """List existing column names for <schema>.<table>."""
        q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        """
        with conn.cursor() as cur:
            cur.execute(q, (schema, table))
            cols = [r[0] for r in cur.fetchall()]
        logger.debug("[ETLHelper._existing_columns] %s.%s cols=%s", schema, table, cols)
        return cols


    @staticmethod
    def sync_table_schema_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, ensure_unique_on: Optional[List[str]] = None) -> None:
        """Create table if missing; add new columns; ensure unique index for upsert (optional)."""
        ident = lambda *parts: sql.SQL(".").join(sql.Identifier(p) for p in parts)
        try:
            if not ETLHelper._table_exists(conn, schema, table):
                cols_sql = sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(ETLHelper._pg_dtype_from_series(df[c])))
                    for c in df.columns
                )
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("CREATE TABLE {} ({})").format(ident(schema, table), cols_sql))
                logger.info("[ETLHelper.sync_table_schema_psycopg2] Created table %s.%s", schema, table)
            else:
                existing = set(ETLHelper._existing_columns(conn, schema, table))
                with conn.cursor() as cur:
                    for c in df.columns:
                        if c not in existing:
                            cur.execute(
                                sql.SQL("ALTER TABLE {} ADD COLUMN {} {} ").format(
                                    ident(schema, table), sql.Identifier(c), sql.SQL(ETLHelper._pg_dtype_from_series(df[c]))
                                )
                            )
                            logger.info("[ETLHelper.sync_table_schema_psycopg2] Added column %s to %s.%s", c, schema, table)
            if ensure_unique_on:
                idx_name = f"ux_{table}_{'_'.join(ensure_unique_on)}"
                cols_id = sql.SQL(", ").join(sql.Identifier(c) for c in ensure_unique_on)
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({}) ").format(
                            sql.Identifier(idx_name), ident(schema, table), cols_id
                        )
                    )
                logger.debug("[ETLHelper.sync_table_schema_psycopg2] Ensured unique index %s on %s.%s", idx_name, schema, table)
        except Exception as e:
            logger.exception("[ETLHelper.sync_table_schema_psycopg2] Failed for %s.%s: %s", schema, table, e)
            raise


    @staticmethod
    def _to_rows(df: pd.DataFrame) -> tuple[list[str], list[tuple]]:
        """Convert DataFrame rows to Python/psycopg2-friendly tuples (NA->None, Timestamp->datetime)."""
        def _py(v):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            if hasattr(v, "item"):
                try:
                    return v.item()
                except Exception:
                    pass
            return v

        cols = list(df.columns)
        rows = [tuple(_py(v) for v in row) for row in df.itertuples(index=False, name=None)]
        logger.debug("[ETLHelper._to_rows] cols=%s rows=%s", len(cols), len(rows))
        return cols, rows


    @staticmethod
    def insert_dataframe_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, page_size: int = 5000) -> None:
        """Bulk INSERT a DataFrame into <schema>.<table> using execute_values."""
        try:
            cols, rows = ETLHelper._to_rows(df)
            with conn.cursor() as cur:
                query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)]),
                    sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                )
                execute_values(cur, query.as_string(conn), rows, page_size=page_size)
            logger.info("[ETLHelper.insert_dataframe_psycopg2] Inserted rows=%s into %s.%s", len(rows), schema, table)
        except Exception as e:
            logger.exception("[ETLHelper.insert_dataframe_psycopg2] Failed insert into %s.%s: %s", schema, table, e)
            raise


    @staticmethod
    def truncate_then_insert_psycopg2(conn, schema: str, table: str, df: pd.DataFrame) -> None:
        """TRUNCATE <schema>.<table> then bulk INSERT all rows from DataFrame."""
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {} ").format(sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])))
            ETLHelper.insert_dataframe_psycopg2(conn, schema, table, df)
            logger.info("[ETLHelper.truncate_then_insert_psycopg2] Truncated and reloaded %s.%s", schema, table)
        except Exception as e:
            logger.exception("[ETLHelper.truncate_then_insert_psycopg2] Failed for %s.%s: %s", schema, table, e)
            raise


    @staticmethod
    def delete_partition_psycopg2(conn, schema: str, table: str, where: Dict[str, str]) -> None:
        """DELETE rows from <schema>.<table> that match equality filters in `where` (e.g., {ds: 'YYYY-MM-DD'})."""
        if not where:
            return
        try:
            with conn.cursor() as cur:
                clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in where.keys()]
                q = sql.SQL("DELETE FROM {} WHERE ").format(sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])) + sql.SQL(" AND ").join(clauses)
                cur.execute(q, tuple(where.values()))
            logger.info("[ETLHelper.delete_partition_psycopg2] Deleted partition where=%s on %s.%s", where, schema, table)
        except Exception as e:
            logger.exception("[ETLHelper.delete_partition_psycopg2] Failed for %s.%s where=%s: %s", schema, table, where, e)
            raise


    @staticmethod
    def upsert_dataframe_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, pk_cols: List[str], page_size: int = 5000) -> None:
        """INSERT rows with ON CONFLICT (...) DO UPDATE for non-PK columns; fallback to DO NOTHING if no non-PK."""
        try:
            cols, rows = ETLHelper._to_rows(df)
            cols_id = [sql.Identifier(c) for c in cols]
            pk_id   = [sql.Identifier(c) for c in pk_cols]
            non_pk = [c for c in cols if c not in pk_cols]
            set_parts = [sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in non_pk]
            insert_q = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO {} ").format(
                sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)]),
                sql.SQL(", ").join(cols_id),
                sql.SQL(", ").join(pk_id),
                sql.SQL("NOTHING") if not set_parts else sql.SQL("UPDATE SET ") + sql.SQL(", ").join(set_parts),
            )
            with conn.cursor() as cur:
                execute_values(cur, insert_q.as_string(conn), rows, page_size=page_size)
            logger.info("[ETLHelper.upsert_dataframe_psycopg2] Upserted rows=%s into %s.%s", len(rows), schema, table)
        except Exception as e:
            logger.exception("[ETLHelper.upsert_dataframe_psycopg2] Failed upsert into %s.%s: %s", schema, table, e)
            raise


    @staticmethod
    def quarantine_dbt_failures_to_minio(
        pg_conn,
        minio_client,
        *,
        audit_schema: str = "dq_audit",
        bucket_name: str = "dq-failures",
        layer: str,
        resource: str,
        ds: str,
    ) -> int:
        """Export dbt test failure/audit tables from Postgres -> MinIO parquet files.
        Returns the number of parquet objects written.
        """
        sql_list = """
          select table_name
          from information_schema.tables
          where table_schema = %s
            and (table_name like 'dbt_test__audit%%' or table_name like '%%__failures')
        """
        try:
            with pg_conn.cursor() as cur:
                cur.execute(sql_list, (audit_schema,))
                found = [r[0] for r in cur.fetchall()]
            if not found:
                logger.info("[ETLHelper.quarantine_dbt_failures_to_minio] No failure tables under schema=%s", audit_schema)
                return 0

            ETLHelper.ensure_bucket(minio_client, bucket_name)
            written = 0
            for tbl in found:
                with pg_conn.cursor() as cur:
                    cur.execute(f'"""SELECT * FROM "{audit_schema}"."{tbl}"""')
                    rows = cur.fetchall(); cols = [d[0] for d in cur.description]
                if not rows:
                    continue
                df = pd.DataFrame(rows, columns=cols)
                obj = f"{layer}/{resource}/ds={ds}/{tbl}.parquet"

                buf = BytesIO(); df.to_parquet(buf, engine="pyarrow", index=False); buf.seek(0)
                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=obj,
                    data=buf,
                    length=buf.getbuffer().nbytes,
                    content_type="application/octet-stream",
                )
                written += 1
                logger.info("[ETLHelper.quarantine_dbt_failures_to_minio] Wrote %s rows to s3://%s/%s", len(df), bucket_name, obj)
            return written
        except Exception as e:
            logger.exception("[ETLHelper.quarantine_dbt_failures_to_minio] Failed: %s", e)
            raise
    

    @staticmethod
    def upload_dbt_artifacts_to_minio(
        minio_client,
        bucket_name: str,
        layer: str,
        resource: str,
        ds: str,
        base_dir: str = "/dbt/target",
        files: list[str] | None = None,
        prefix: str | None = None,
    ) -> None:
        """Push common dbt artifacts (run_results.json, manifest.json, catalog.json, index.html)
        to MinIO at <bucket>/<layer>/<resource>/ds=<YYYY-MM-DD>/.
        Missing files are skipped silently.
        """
        import os
        files = files or ["run_results.json", "manifest.json", "catalog.json", "index.html"]
        try:
            ETLHelper.ensure_bucket(minio_client, bucket_name)
            obj_prefix = prefix or f"{layer}/{resource}/ds={ds}"
            for fname in files:
                fpath = os.path.join(base_dir, fname)
                if not os.path.exists(fpath):
                    logger.debug("[ETLHelper.upload_dbt_artifacts_to_minio] Skip missing %s", fpath)
                    continue
                with open(fpath, "rb") as f:
                    data = f.read()
                content_type = "application/json" if fname.endswith(".json") else "text/html"
                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=f"{obj_prefix}/{fname}",
                    data=BytesIO(data),
                    length=len(data),
                    content_type=content_type,
                )
                logger.info("[ETLHelper.upload_dbt_artifacts_to_minio] Uploaded %s to s3://%s/%s/%s", fname, bucket_name, obj_prefix, fname)
        except Exception as e:
            logger.exception("[ETLHelper.upload_dbt_artifacts_to_minio] Failed: %s", e)
            raise
