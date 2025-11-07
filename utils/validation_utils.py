####
## Gramedia Digital - Data Engineer Take Home Test
## Mario Caesar // caesarmario87@gmail.com
## Validation utility file for checking data when load to db
####

from __future__ import annotations
from typing import Dict, Iterable, List
import pandas as pd


def _ensure_columns(df: pd.DataFrame, cols: Iterable[str]) -> List[str]:
    return [c for c in cols if c in df.columns]


def _assert_no_nulls(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    cols = _ensure_columns(df, cols)
    if not cols:
        return
    bad = {c: int(df[c].isna().sum()) for c in cols if df[c].isna().any()}
    if bad:
        raise ValueError(f"[DQ:{title}] Nulls found: {bad}")


def _assert_gt_zero(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    cols = _ensure_columns(df, cols)
    for c in cols:
        if (df[c] <= 0).any():
            raise ValueError(f"[DQ:{title}] Column '{c}' has non-positive values.")


def _assert_unique(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    cols = _ensure_columns(df, cols)
    if cols and df.duplicated(subset=cols).any():
        raise ValueError(f"[DQ:{title}] Duplicates detected on keys {cols}.")


def run_basic_checks(resource: str, df: pd.DataFrame, config: Dict) -> None:
    """
    Very light checks derived from config.select (output column names):
    - products: title & price not null; price > 0; product_id unique.
    - carts: quantity > 0; (cart_id, product_id) unique.
    - users: user_id unique; email not null.
    """
    sel = config.get("select", {})

    if resource == "products":
        name_col  = "title" if "title" in sel else next((c for c in df.columns if c in ("title","product_name")), None)
        price_col = "price"
        pid_col   = "product_id"

        _assert_no_nulls(df, [name_col, price_col], "products:not_null")
        _assert_gt_zero(df, [price_col], "products:gt_zero")
        _assert_unique(df, [pid_col], "products:unique")

    elif resource == "carts":
        qty_col   = "quantity" if "quantity" in sel else "products.quantity"
        cart_col  = "cart_id" if "cart_id" in sel else "id"
        prod_col  = "product_id" if "product_id" in sel else "products.productId"

        _assert_gt_zero(df, [qty_col], "carts:gt_zero")
        _assert_unique(df, [cart_col, prod_col], "carts:unique")

    elif resource == "users":
        _assert_no_nulls(df, ["email"], "users:not_null")
        _assert_unique(df, ["user_id"], "users:unique")

    # Generic sanity if present in config.validations
    val = config.get("validations", {})
    if "not_null" in val:
        _assert_no_nulls(df, val["not_null"], "cfg:not_null")
    if "unique" in val:
        _assert_unique(df, val["unique"], "cfg:unique")
