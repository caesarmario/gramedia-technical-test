####
## Gramedia Digital - Data Engineer Take Home Test
## Mario Caesar // caesarmario87@gmail.com
## Validation utilities (lightweight checks used before loading into DB)
####

from __future__ import annotations
from typing import Dict, Iterable, List
import pandas as pd

from utils.logging_utils import logger


def _ensure_columns(df: pd.DataFrame, cols: Iterable[str]) -> List[str]:
    """
    Filter down to columns that actually exist in the DataFrame.

    Returns:
        A list of column names from `cols` that are present in `df`.
    """
    present = [c for c in cols if c in df.columns]
    # Debug-only verbosity to avoid noisy logs in green paths.
    logger.debug("[DQ] _ensure_columns requested=%s present=%s", list(cols), present)
    return present


def _assert_no_nulls(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    """
    Assert that the specified columns do not contain NULL/NaN values.

    Raises:
        ValueError if any column has at least one null.
    """
    cols = _ensure_columns(df, cols)
    if not cols:
        logger.info("[DQ:%s] Skipping not_null — no target columns found.", title)
        return
    try:
        bad = {c: int(df[c].isna().sum()) for c in cols if df[c].isna().any()}
        if bad:
            logger.error("[DQ:%s] Nulls found per column: %s", title, bad)
            raise ValueError(f"[DQ:{title}] Nulls found: {bad}")
        logger.info("[DQ:%s] not_null passed for columns=%s", title, cols)
    except Exception as e:
        # Re-raise after logging so the caller can handle/stop the load.
        logger.exception("[DQ:%s] not_null check failed with exception.", title)
        raise


def _assert_gt_zero(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    """
    Assert that the specified numeric columns contain strictly positive values.

    Raises:
        ValueError if any column contains non-positive values (<= 0).
    """
    cols = _ensure_columns(df, cols)
    if not cols:
        logger.info("[DQ:%s] Skipping gt_zero — no target columns found.", title)
        return
    try:
        for c in cols:
            if (df[c] <= 0).any():
                logger.error("[DQ:%s] Column '%s' has non-positive values.", title, c)
                raise ValueError(f"[DQ:{title}] Column '{c}' has non-positive values.")
        logger.info("[DQ:%s] gt_zero passed for columns=%s", title, cols)
    except Exception as e:
        logger.exception("[DQ:%s] gt_zero check failed with exception.", title)
        raise


def _assert_unique(df: pd.DataFrame, cols: Iterable[str], title: str) -> None:
    """
    Assert that the specified set of columns is unique as a composite key.

    Raises:
        ValueError if duplicates exist for that subset.
    """
    cols = _ensure_columns(df, cols)
    if not cols:
        logger.info("[DQ:%s] Skipping unique — no target columns found.", title)
        return
    try:
        dup_mask = df.duplicated(subset=cols)
        dup_count = int(dup_mask.sum())
        if dup_count > 0:
            logger.error("[DQ:%s] Duplicates detected on keys %s (count=%d).", title, cols, dup_count)
            raise ValueError(f"[DQ:{title}] Duplicates detected on keys {cols}.")
        logger.info("[DQ:%s] unique passed for key=%s", title, cols)
    except Exception as e:
        logger.exception("[DQ:%s] unique check failed with exception.", title)
        raise


def run_basic_checks(resource: str, df: pd.DataFrame, config: Dict) -> None:
    """
    Run very light data quality checks derived from the transform config.

    Policy summary (resource-specific defaults):
      - products: title & price not null; price > 0; product_id unique.
      - carts   : quantity > 0; (cart_id, product_id) unique.
      - users   : user_id unique; email not null.

    Also honors optional generic rules from config["validations"]:
      - not_null: list[str]
      - unique  : list[str] or list[list[str]]

    Raises:
      ValueError for any violation (intended to stop downstream loads).
    """
    logger.info("[DQ] Running basic checks for resource=%s rows=%d", resource, len(df))
    sel = config.get("select", {})

    try:
        if resource == "products":
            # Be tolerant to naming differences (title vs product_name).
            name_col  = "title" if "title" in sel else next((c for c in df.columns if c in ("title", "product_name")), None)
            price_col = "price"
            pid_col   = "product_id"

            _assert_no_nulls(df, [name_col, price_col], "products:not_null")
            _assert_gt_zero(df, [price_col], "products:gt_zero")
            _assert_unique(df, [pid_col], "products:unique")

        elif resource == "carts":
            # Support flattened or original FakeStore carts schema.
            qty_col   = "quantity" if "quantity" in sel else "products.quantity"
            cart_col  = "cart_id" if "cart_id" in sel else "id"
            prod_col  = "product_id" if "product_id" in sel else "products.productId"

            _assert_gt_zero(df, [qty_col], "carts:gt_zero")
            _assert_unique(df, [cart_col, prod_col], "carts:unique")

        elif resource == "users":
            _assert_no_nulls(df, ["email"], "users:not_null")
            _assert_unique(df, ["user_id"], "users:unique")

        # Generic validations from config (optional and additive).
        val = config.get("validations", {})
        if "not_null" in val:
            _assert_no_nulls(df, val["not_null"], "cfg:not_null")

        # `unique` can be a flat list (single composite key) or list of lists.
        if "unique" in val:
            unique_spec = val["unique"]
            if isinstance(unique_spec, list) and all(isinstance(x, str) for x in unique_spec):
                _assert_unique(df, unique_spec, "cfg:unique")
            elif isinstance(unique_spec, list):
                for idx, keys in enumerate(unique_spec, start=1):
                    _assert_unique(df, keys, f"cfg:unique[{idx}]")

        logger.info("[DQ] Basic checks completed OK for resource=%s", resource)

    except ValueError:
        # Expected DQ failure — already logged by helper; bubble up as-is.
        raise
    except Exception as e:
        # Unexpected error path; log context clearly, then re-raise.
        logger.exception("[DQ] Unexpected exception during checks for resource=%s", resource)
        raise
