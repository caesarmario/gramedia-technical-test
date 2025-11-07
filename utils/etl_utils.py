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


    @staticmethod
    def build_parquet_object_name(resource: str, ds: str, layer: str) -> str:
        """
        s3 key: <layer>/<resource>/YYYY/MM/DD/<resource>_<ds>.parquet
        """
        try:
            y, m, d = ds.split("-")
            assert len(y) == 4 and len(m) == 2 and len(d) == 2
        except Exception:
            raise ValueError("ds must be YYYY-MM-DD, e.g., 2025-11-07")
        return f"{layer}/{resource}/{y}/{m}/{d}/{resource}_{ds}.parquet"


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
    def records_to_dataframe(records: Sequence[Mapping[str, Any]], sep: str = ".") -> pd.DataFrame:
        """
        Gunakan json_normalize agar kolom nested dict otomatis jadi 'a.b'.
        List akan tetap berupa list—ditangani belakangan via explode.
        """
        if not records:
            return pd.DataFrame()
        return pd.json_normalize(records, sep=sep)
    

    @staticmethod
    def _safe_explode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if col not in df.columns:
            # tidak apa-apa; biarkan saja
            return df
        df[col] = df[col].map(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))
        df = df.explode(col, ignore_index=True)
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

    
    @staticmethod
    def _flatten_dict_col(df: pd.DataFrame, col: str, prefix: Optional[str] = None) -> pd.DataFrame:
        """Flatten satu kolom dict menjadi kolom-kolom baru dengan prefix."""
        if col not in df.columns:
            return df
        if not df[col].apply(lambda x: isinstance(x, dict) or pd.isna(x)).all():
            return df

        out = df.copy()
        # safe normalize: gantikan None dengan {} agar normalize tidak error
        norm = pd.json_normalize(out[col].apply(lambda x: x or {}))
        pref = prefix or col
        norm.columns = [f"{pref}_{c}".replace(".", "_") for c in norm.columns]
        out = pd.concat([out.drop(columns=[col]), norm], axis=1)
        return out
    

    @staticmethod
    def auto_flatten_dicts(df: pd.DataFrame, max_depth: int = 2) -> pd.DataFrame:
        """
        Secara otomatis flatten kolom bertipe dict beberapa kali (aman untuk None).
        Tidak menyentuh list (ditangani oleh explode_and_flatten).
        """
        out = df.copy()
        for _ in range(max_depth):
            dict_cols = [c for c in out.columns if out[c].apply(lambda x: isinstance(x, dict)).any()]
            if not dict_cols:
                break
            for col in dict_cols:
                out = ETLHelper._flatten_dict_col(out, col)
        return out


    @staticmethod
    def _resolve_col(df: pd.DataFrame, name: str) -> Optional[str]:
        """
        Cari nama kolom aktual di df untuk 'name' dengan beberapa variasi:
        - persis sama
        - titik <-> underscore
        """
        if name in df.columns:
            return name
        candidates = {name.replace(".", "_"), name.replace("_", ".")}
        for cand in candidates:
            if cand in df.columns:
                return cand
        return None


    # utils/etl_utils.py

    @staticmethod
    def apply_config(df: pd.DataFrame, cfg: Mapping[str, Any], sep: str = ".") -> pd.DataFrame:
        if df.empty:
            return df.copy()

        # 1) Mild auto-flatten for safety
        df = ETLHelper.auto_flatten_dicts(df, max_depth=2)

        sel = cfg.get("select", None)

        # --- New style: select is a dict of {out_col: {path, type, default}} ---
        if isinstance(sel, Mapping) and sel:
            out_cols: Dict[str, pd.Series] = {}

            for out_col, spec in sel.items():
                spec = spec or {}
                path = spec.get("path", out_col)  # default path -> same name
                typ  = (spec.get("type") or "").lower()
                dflt = spec.get("default", None)

                # Try fast path: if a column exists that matches the path (dot or underscore variant)
                resolved = ETLHelper._resolve_col(df, path)
                if resolved:
                    s = df[resolved]
                else:
                    # Fallback: row-wise nested extraction
                    s = df.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)

                # Coerce dtype if requested
                if typ:
                    s = ETLHelper._coerce(s, typ)

                # Apply default if provided
                if dflt is not None:
                    s = s.fillna(dflt)

                out_cols[out_col] = s

            out = pd.DataFrame(out_cols)

            # 2) Derivations (simple helpers like len on an existing column)
            derives = cfg.get("derive") or {}
            for new_col, rule in derives.items():
                op = (rule or {}).get("op")
                args = (rule or {}).get("args") or []
                if op == "len" and args:
                    src = args[0]
                    # prefer derived/select result; fallback to original df
                    if src in out.columns:
                        out[new_col] = out[src].astype("string").str.len()
                    elif src in df.columns:
                        out[new_col] = df[src].astype("string").str.len()

            # 3) Fillna (post-derive)
            for c, v in (cfg.get("fillna") or {}).items():
                if c in out.columns:
                    out[c] = out[c].fillna(v)

            # 4) Column order if provided
            order = cfg.get("order") or []
            if order:
                ordered = [c for c in order if c in out.columns]
                tail = [c for c in out.columns if c not in ordered]
                out = out[ordered + tail]

            return out

        # --- Old style (compat): rename/select/casts/fillna/drop like previous version ---
        # 2) explode lists if present
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

        # flatten map (path -> outcol)
        for path, outcol in (cfg.get("flatten") or {}).items():
            if outcol in df.columns:
                continue
            if path in df.columns:
                df[outcol] = df[path]
            else:
                df[outcol] = df.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)

        # rename
        if cfg.get("rename"):
            actual = {k: v for k, v in cfg["rename"].items() if k in df.columns}
            if actual:
                df = df.rename(columns=actual)

        # select (list) with tolerant matching
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

        # casts
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
                pass

        # fillna & drop
        for col, val in (cfg.get("fillna") or {}).items():
            if col in df.columns:
                df[col] = df[col].fillna(val)
        for col in (cfg.get("drop") or []):
            if col in df.columns:
                df = df.drop(columns=[col])

        return df


    @staticmethod
    def _get_by_path(obj: Any, path: str, sep: str = ".") -> Any:
        """
        Ambil nilai nested dengan path bertitik, contoh:
        - "rating.rate"
        - "products.0.productId"
        - jika idx list tidak ada / None -> None
        """
        cur = obj
        for key in path.split(sep):
            if cur is None:
                return None
            # coba int index untuk list
            try:
                idx = int(key)
                if isinstance(cur, (list, tuple)):
                    cur = cur[idx] if 0 <= idx < len(cur) else None
                else:
                    return None
            except ValueError:
                # dict key
                if isinstance(cur, Mapping):
                    cur = cur.get(key)
                else:
                    return None
        return cur


    @staticmethod
    def _coerce(s: pd.Series, typ: str) -> pd.Series:
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
        """
        Carts-only:
        - Explode 'products' (list of {productId, quantity}) -> satu baris per item.
        - SIMPAN kolom 'products' sebagai dict (agar path nested 'products.productId' bisa di-resolve),
        dan BUAT juga kolom datar 'products.productId' & 'products.quantity'.
        - Tahan kasus None/NaN/string-json/string-literal.
        """
        if "products" not in df.columns:
            return df.copy()

        base_cols = [c for c in df.columns if c != "products"]
        out_rows: list[dict] = []

        def _to_list(obj):
            # Sudah list
            if isinstance(obj, list):
                return obj
            # None / NaN
            if obj is None or (isinstance(obj, float) and pd.isna(obj)):
                return []
            # String -> coba json lalu literal
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
            # Satu dict / tipe lain -> bungkus list
            return [obj]

        for _, rec in df.iterrows():
            base = {c: rec[c] for c in base_cols}
            items = _to_list(rec.get("products"))

            if not items:
                # baris kosong produk (tetap pertahankan bentuk kolom)
                row = {**base, "products": {}, "products.productId": None, "products.quantity": None}
                out_rows.append(row)
                continue

            for it in items:
                # item bisa saja string; coba parse
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
                    "products": it,  # simpan dict-nya untuk nested-path resolver
                    "products.productId": it.get("productId"),
                    "products.quantity": it.get("quantity"),
                }
                out_rows.append(row)

        out = pd.DataFrame(out_rows)

        # optional: cast ke numeric aman (biar tidak NaN -> error)
        if "products.productId" in out.columns:
            out["products.productId"] = pd.to_numeric(out["products.productId"], errors="coerce")
        if "products.quantity" in out.columns:
            out["products.quantity"] = pd.to_numeric(out["products.quantity"], errors="coerce")

        return out


    @staticmethod
    def apply_config_carts(df: pd.DataFrame, cfg: Mapping[str, Any], sep: str = ".") -> pd.DataFrame:
        """
        Carts-only apply_config:
        - Hindari duplikasi `products.productId` vs `products_productId` dengan
        menghapus kolom dict 'products' sebelum auto-flatten.
        - Baca format config berbasis 'mapping' + 'data_type' (tanpa 'select').
        - Susun kolom output hanya sesuai yang didefinisikan di config.
        """
        if df.empty:
            return df.copy()

        # 0) Drop kolom dict 'products' (sudah dipecah via _explode_carts_products)
        work = df.copy()
        if "products" in work.columns:
            work = work.drop(columns=["products"])

        # 1) Auto-flatten dict lain (mis. meta) tapi bukan 'products'
        work = ETLHelper.auto_flatten_dicts(work, max_depth=2)

        # 2) Kumpulkan definisi kolom dari config (semua key selain yang diawali '_')
        col_specs: Dict[str, Dict[str, Any]] = {
            k: v for k, v in cfg.items()
            if not str(k).startswith("_") and isinstance(v, Mapping)
        }

        # 3) Helper: coercion tipe dari data_type config
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

        # 4) Bangun output per kolom berdasarkan mapping path
        out_cols: Dict[str, pd.Series] = {}
        for out_col, spec in col_specs.items():
            path = spec.get("mapping", out_col)
            dtype = spec.get("data_type", "")

            # coba cari kolom langsung (dukung titik/underscore)
            resolved = ETLHelper._resolve_col(work, path)
            if resolved:
                series = work[resolved]
            else:
                # fallback: ambil dari nested path per-baris
                series = work.apply(lambda r: ETLHelper._get_by_path(r.to_dict(), path, sep=sep), axis=1)

            # koersi tipe jika ada
            if dtype:
                series = _coerce_series(series, dtype)

            out_cols[out_col] = series

        out = pd.DataFrame(out_cols)

        # 5) Urutan kolom: pakai yang didefinisikan di config (jika ada ‘order’), else prefer default di bawah
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

        return out


    @staticmethod
    def read_parquet_from_minio(client, bucket: str, object_name: str) -> pd.DataFrame:
        resp = client.get_object(bucket, object_name)
        try:
            raw = resp.read()
        finally:
            resp.close(); resp.release_conn()
        return pd.read_parquet(BytesIO(raw))


    @staticmethod
    def create_pg_conn(creds: Dict):
        conn = psycopg2.connect(
            host=creds["POSTGRES_HOST"],
            port=int(creds.get("POSTGRES_PORT", 5432)),
            user=creds["POSTGRES_USER"],
            password=creds["POSTGRES_PASSWORD"],
            dbname=creds["POSTGRES_DB"],
        )
        conn.autocommit = True
        return conn


    @staticmethod
    def _pg_dtype_from_series(s: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(s):     return "BIGINT"
        if pd.api.types.is_float_dtype(s):       return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(s):        return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(s): return "TIMESTAMP"
        return "TEXT"


    @staticmethod
    def ensure_schema_psycopg2(conn, schema: str) -> None:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


    @staticmethod
    def _table_exists(conn, schema: str, table: str) -> bool:
        q = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """
        with conn.cursor() as cur:
            cur.execute(q, (schema, table))
            return cur.fetchone() is not None


    @staticmethod
    def _existing_columns(conn, schema: str, table: str) -> List[str]:
        q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        """
        with conn.cursor() as cur:
            cur.execute(q, (schema, table))
            return [r[0] for r in cur.fetchall()]


    @staticmethod
    def sync_table_schema_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, ensure_unique_on: Optional[List[str]]=None) -> None:
        ident = lambda *parts: sql.SQL(".").join(sql.Identifier(p) for p in parts)
        if not ETLHelper._table_exists(conn, schema, table):
            # create table
            cols_sql = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(ETLHelper._pg_dtype_from_series(df[c])))
                for c in df.columns
            )
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE TABLE {} ({})").format(ident(schema, table), cols_sql))
        else:
            # add missing columns
            existing = set(ETLHelper._existing_columns(conn, schema, table))
            with conn.cursor() as cur:
                for c in df.columns:
                    if c not in existing:
                        cur.execute(
                            sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                                ident(schema, table),
                                sql.Identifier(c),
                                sql.SQL(ETLHelper._pg_dtype_from_series(df[c])),
                            )
                        )
        # ensure unique index for upsert
        if ensure_unique_on:
            idx_name = f"ux_{table}_{'_'.join(ensure_unique_on)}"
            cols_id  = sql.SQL(", ").join(sql.Identifier(c) for c in ensure_unique_on)
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})"
                    ).format(sql.Identifier(idx_name), ident(schema, table), cols_id)
                )


    @staticmethod
    def _to_rows(df: pd.DataFrame) -> tuple[list[str], list[tuple]]:
        """
        Convert a DataFrame into psycopg2-safe rows:
        - <NA>/NaN/NaT -> None
        - pandas/NumPy scalars -> built-in Python (int/float/bool)
        - pandas.Timestamp -> datetime
        """
        def _py(v):
            # null-like
            if v is None:
                return None
            # pandas NA/NaN/NaT
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            # pandas.Timestamp -> datetime
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            # objects exposing .item() (NumPy scalars)
            if hasattr(v, "item"):
                try:
                    return v.item()
                except Exception:
                    pass
            # last resort: bool(np.bool_), etc.
            # (bool/int/float/str already fine)
            return v

        cols = list(df.columns)
        rows = [tuple(_py(v) for v in row) for row in df.itertuples(index=False, name=None)]
        return cols, rows


    @staticmethod
    def insert_dataframe_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, page_size: int = 5000) -> None:
        cols, rows = ETLHelper._to_rows(df)
        with conn.cursor() as cur:
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)]),
                sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            )
            execute_values(cur, query.as_string(conn), rows, page_size=page_size)


    @staticmethod
    def truncate_then_insert_psycopg2(conn, schema: str, table: str, df: pd.DataFrame) -> None:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])))
        ETLHelper.insert_dataframe_psycopg2(conn, schema, table, df)


    @staticmethod
    def delete_partition_psycopg2(conn, schema: str, table: str, where: Dict[str, str]) -> None:
        if not where:
            return
        with conn.cursor() as cur:
            clauses = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in where.keys()]
            q = sql.SQL("DELETE FROM {} WHERE ").format(sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])) + sql.SQL(" AND ").join(clauses)
            cur.execute(q, tuple(where.values()))


    @staticmethod
    def upsert_dataframe_psycopg2(conn, schema: str, table: str, df: pd.DataFrame, pk_cols: List[str], page_size: int = 5000) -> None:
        cols, rows = ETLHelper._to_rows(df)
        cols_id = [sql.Identifier(c) for c in cols]
        pk_id   = [sql.Identifier(c) for c in pk_cols]

        # build DO UPDATE SET for non-PK cols
        non_pk = [c for c in cols if c not in pk_cols]
        set_parts = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in non_pk
        ]
        insert_q = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO {}").format(
            sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)]),
            sql.SQL(", ").join(cols_id),
            sql.SQL(", ").join(pk_id),
            sql.SQL("NOTHING") if not set_parts else sql.SQL("UPDATE SET ") + sql.SQL(", ").join(set_parts),
        )
        with conn.cursor() as cur:
            execute_values(cur, insert_q.as_string(conn), rows, page_size=page_size)


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
        """
        Export all dbt failure/audit tables from Postgres -> MinIO parquet files.

        Returns number of parquet objects written.
        """

        # List all failure/audit tables produced by dbt (store_failures: true)
        sql_list = """
          select table_name
          from information_schema.tables
          where table_schema = %s
            and (table_name like 'dbt_test__audit%%' or table_name like '%%__failures')
        """

        with pg_conn.cursor() as cur:
            cur.execute(sql_list, (audit_schema,))
            found = [r[0] for r in cur.fetchall()]

        if not found:
            return 0

        # Ensure MinIO bucket exists
        ETLHelper.ensure_bucket(minio_client, bucket_name)

        written = 0
        for tbl in found:
            with pg_conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{audit_schema}"."{tbl}"')
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

            if not rows:
                continue

            df = pd.DataFrame(rows, columns=cols)
            obj = f"{layer}/{resource}/ds={ds}/{tbl}.parquet"

            buf = BytesIO()
            df.to_parquet(buf, engine="pyarrow", index=False)
            buf.seek(0)

            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=obj,
                data=buf,
                length=buf.getbuffer().nbytes,
                content_type="application/octet-stream",
            )
            written += 1

        return written
    

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
        """
        Push dbt artifacts (run_results.json, manifest.json, catalog.json, index.html)
        to MinIO at: <bucket>/<layer>/<resource>/ds=<YYYY-MM-DD>/
        """
        import os
        from io import BytesIO

        # default files
        files = files or ["run_results.json", "manifest.json", "catalog.json", "index.html"]
        # ensure bucket
        ETLHelper.ensure_bucket(minio_client, bucket_name)

        # path prefix
        obj_prefix = prefix or f"{layer}/{resource}/ds={ds}"

        for fname in files:
            fpath = os.path.join(base_dir, fname)
            if not os.path.exists(fpath):
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