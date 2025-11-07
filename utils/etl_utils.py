####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for ETL ops.
####

from __future__ import annotations

import json
import pandas as pd

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
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
    def _expand_dict_columns(df: pd.DataFrame, sep: str = ".") -> pd.DataFrame:
        dict_cols = [c for c in df.columns if df[c].map(lambda v: isinstance(v, Mapping)).any()]
        for col in dict_cols:
            expanded = pd.json_normalize(
                df[col].map(lambda v: v if isinstance(v, Mapping) else {}),
                sep=sep
            )
            expanded.columns = [f"{col}{sep}{c}" for c in expanded.columns]
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
        return df
    

    @staticmethod
    def _safe_explode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if col not in df.columns:
            # tidak apa-apa; biarkan saja
            return df
        df[col] = df[col].map(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))
        df = df.explode(col, ignore_index=True)
        return df


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
    def explode_and_flatten(df: pd.DataFrame, list_col: str, prefix: str) -> pd.DataFrame:
        """Explode kolom list (berisi dict atau scalar), lalu flatten jika dict."""
        if list_col not in df.columns:
            return df

        out = df.copy()
        out = out.explode(list_col, ignore_index=True)

        # Kalau setelah explode isinya dict → flatten
        if out[list_col].apply(lambda x: isinstance(x, dict) or pd.isna(x)).all():
            norm = pd.json_normalize(out[list_col].apply(lambda x: x or {}))
            norm.columns = [f"{prefix}_{c}".replace(".", "_") for c in norm.columns]
            out = pd.concat([out.drop(columns=[list_col]), norm], axis=1)
        else:
            # scalar → beri nama kolom tunggal
            out.rename(columns={list_col: f"{prefix}_value"}, inplace=True)

        return out


    @staticmethod
    def flatten_fields(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
        """Flatten bertingkat berdasarkan daftar path sederhana ('address', 'address_geolocation', ...)."""
        out = df.copy()
        for f in fields:
            out = ETLHelper._flatten_dict_col(out, f)
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