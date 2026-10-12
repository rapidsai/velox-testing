# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Result normalization and comparison for TPC-Havoc runs.

Comparison semantics come from common/testing/result_comparison.py
(compare_result_frames): order-insensitive unless the SQL has ORDER BY, LIMIT
tie-boundary handling, and float tolerance (rel 1e-5, abs 1e-8). Before
comparing, frames from different sources (Presto, DuckDB, a variant vs. its
base, a stored run) are normalized to a common representation, because the
same logical value can arrive with different physical types:

* DECIMAL / integer / floating point -> float64 (a variant may return SUM as
  DECIMAL where the base returns DOUBLE, Presto returns DECIMAL as str, ...)
* DATE / TIMESTAMP -> ISO-8601 string
* everything else -> str (nulls preserved)
"""

from __future__ import annotations

import datetime
import decimal
import re
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import sqlglot

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from common.testing.result_comparison import (
    compare_result_frames,
    get_limit,
    get_orderby_sort_spec,
    restore_orderby,
)

_NUMERIC_PREFIXES = ("decimal", "bigint", "integer", "int", "smallint", "tinyint", "double", "real", "float", "hugeint")
_TEMPORAL_PREFIXES = ("date", "timestamp")


def presto_rows_to_frame(rows: list[list], columns: list[str], types: list[str]) -> pd.DataFrame:
    """Build a raw (lossless) frame from Presto rows; DECIMAL strings become decimal.Decimal."""
    data = {}
    for i, (name, type_name) in enumerate(zip(columns, types)):
        values = [row[i] for row in rows]
        if type_name.lower().startswith("decimal"):
            values = [None if v is None else decimal.Decimal(v) for v in values]
        data[i] = values
    frame = pd.DataFrame(data)
    frame.columns = _dedupe(columns)
    return frame


def _dedupe(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for name in columns:
        if name in seen:
            seen[name] += 1
            out.append(f"{name}__{seen[name]}")
        else:
            seen[name] = 0
            out.append(name)
    return out


def write_frame(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        # Mixed-precision Decimal columns etc.: fall back to a string encoding.
        frame.astype(object).where(frame.notna(), None).astype(str).to_parquet(path, index=False)


def _is_temporal_value(value) -> bool:
    return isinstance(value, (datetime.date, datetime.datetime, pd.Timestamp, np.datetime64))


def _first_non_null(series: pd.Series):
    return next((value for value in series if not _isnull(value)), None)


def normalize_frame(frame: pd.DataFrame, types: list[str] | None = None) -> pd.DataFrame:
    """Normalize a result frame to a canonical, source-independent representation."""
    out = {}
    for i in range(frame.shape[1]):
        col = frame.iloc[:, i]
        type_name = (types[i] if types and i < len(types) else "").lower()
        sample = _first_non_null(col)
        if (
            type_name.startswith(_NUMERIC_PREFIXES)
            or pd.api.types.is_numeric_dtype(col.dtype)
            and not pd.api.types.is_bool_dtype(col.dtype)
            or isinstance(sample, (decimal.Decimal, int, float, np.number))
            and not isinstance(sample, bool)
        ):
            try:
                out[i] = pd.to_numeric(col.map(lambda v: None if v is None else float(v)), errors="raise").astype(
                    "float64"
                )
                continue
            except (TypeError, ValueError):
                pass
        if (
            type_name.startswith(_TEMPORAL_PREFIXES)
            or pd.api.types.is_datetime64_any_dtype(col.dtype)
            or (_is_temporal_value(sample))
        ):
            converted = pd.to_datetime(col, errors="coerce")
            is_date_only = type_name.startswith("date") or (
                converted.notna().any() and (converted.dropna().dt.normalize() == converted.dropna()).all()
            )
            fmt = "%Y-%m-%d" if is_date_only else "%Y-%m-%d %H:%M:%S.%f"
            out[i] = pd.Series(
                [None if pd.isna(v) else v.strftime(fmt) for v in converted], dtype=object, index=col.index
            )
            continue
        out[i] = pd.Series([None if _isnull(v) else str(v) for v in col], dtype=object, index=col.index)
    result = pd.DataFrame(out)
    result.columns = list(frame.columns)
    return result


def _isnull(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def merge_types(*type_lists: list[str] | None) -> list[str] | None:
    """Per column, keep the DECIMAL type with the smallest scale found in any list.

    compare_result_frames derives an absolute tolerance of half a unit in the last
    place from a decimal(p,s) type, so e.g. Presto's AVG over DECIMAL(15,2) (rounded
    to 2 places) matches DuckDB's or a variant's DOUBLE average.
    """
    lists = [t for t in type_lists if t]
    if not lists:
        return None
    merged = []
    for i in range(max(len(t) for t in lists)):
        best, best_scale = "", None
        for types in lists:
            name = (types[i] if i < len(types) else "") or ""
            m = re.fullmatch(r"decimal\(\d+,\s*(\d+)\)", name.lower().replace(" ", ""))
            if m and (best_scale is None or int(m.group(1)) < best_scale):
                best, best_scale = name, int(m.group(1))
        merged.append(best)
    return merged


def has_top_level_order_by(sql: str) -> bool:
    """True if the outermost query has ORDER BY (an ORDER BY inside a subquery does not order the result)."""
    try:
        return sqlglot.parse_one(sql, read="presto").args.get("order") is not None
    except Exception:
        return True


def compare(
    actual: pd.DataFrame, expected: pd.DataFrame, query_sql: str, types: list[str] | None = None
) -> tuple[bool, str | None]:
    """Compare two normalized frames. Returns (passed, message).

    ``types`` (e.g. from merge_types) widens the tolerance of DECIMAL columns.
    Row order is only checked when the outermost query has ORDER BY.
    """
    if not has_top_level_order_by(query_sql):
        query_sql = "SELECT 1"  # order-insensitive comparison
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            compare_result_frames(actual, expected, query_sql, actual_column_types=types)
        return True, None
    except Exception as exc:  # AssertionError for mismatches, anything else for comparison bugs
        return False, f"{type(exc).__name__}: {exc}"[:2000]


def compare_variant_to_base(
    variant: pd.DataFrame,
    base: pd.DataFrame,
    base_sql: str,
    types: list[str] | None = None,
    variant_sql: str | None = None,
) -> tuple[bool, str | None]:
    """Compare a TPC-Havoc variant result with its base TPC-H query result.

    Canonical TPC-H queries Q2, Q3, Q10, Q18 and Q21 end in a presentational
    ``ORDER BY ... LIMIT n`` top-N cut that most variants omit (BenchBox's own
    equivalence oracle strips the LIMIT from both sides). Here the base keeps its
    LIMIT, so when the variant returns more rows than the base, the variant is
    put in the base query's ORDER BY order and cut to the same N rows before
    comparison; compare_result_frames then handles ties at the cut-off.

    Some variants have ORDER BY only inside a subquery, so their row order is
    unspecified; such a variant is put in the base query's order before comparing
    (the result set is still compared exactly).
    """
    if variant_sql is not None and not has_top_level_order_by(variant_sql) and variant.shape[1] == base.shape[1]:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sort_idx, ascending, nulls_first = get_orderby_sort_spec(base_sql, list(base.columns))
        if sort_idx:
            variant = restore_orderby(variant, sort_idx, ascending, nulls_first)
    limit = get_limit(base_sql)
    if limit is not None and len(variant) > len(base) and len(base) <= limit:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sort_idx, ascending, nulls_first = get_orderby_sort_spec(base_sql, list(base.columns))
        if not sort_idx:
            return False, (
                f"Variant returned {len(variant)} rows vs {len(base)} for the base query (LIMIT {limit}); "
                "base ORDER BY could not be mapped to result columns for a top-N comparison"
            )
        if variant.shape[1] != base.shape[1]:
            return False, f"Column count mismatch: {variant.shape[1]} (variant) vs {base.shape[1]} (base)"
        variant = restore_orderby(variant, sort_idx, ascending, nulls_first).head(limit)
    return compare(variant, base, base_sql, types)
