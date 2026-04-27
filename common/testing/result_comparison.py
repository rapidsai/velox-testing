# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared result comparison logic for TPC-H/TPC-DS query validation.

Used by both the integration test path (live query engine results vs DuckDB
reference) and the benchmark validation path (result parquet files vs expected
parquet files). See compare_result_frames for full comparison semantics.
"""

import datetime
import decimal
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import sqlglot

REL_TOL = 1e-5
ABS_TOL = 1e-8
MAX_MISMATCHES = 5

# Known queries that return empty results due to float precision issues.
# These are marked as expected failures rather than test failures.
XFAIL_IF_EMPTY: set[str] = {"q15"}

ValidationStatus = Literal["passed", "failed", "expected-failure", "not-validated"]


# ---------------------------------------------------------------------------
# SQL parsing helpers
# ---------------------------------------------------------------------------


def get_orderby_info(query_sql: str, expected_col_names: list[str]) -> tuple[list[int], list[bool]]:
    """
    Extract ORDER BY information from SQL using sqlglot.

    Returns:
        (sort_col_indices, descending) where sort_col_indices is a list of
        0-based column indices (resolved against expected_col_names) and
        descending is a parallel list of booleans.

    Returns ([], []) when there is no ORDER BY, or when any ORDER BY expression
    is too complex to map to a result column (CASE, aggregate, etc.).
    """
    expr = sqlglot.parse_one(query_sql)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return [], []

    sort_col_indices: list[int] = []
    descending: list[bool] = []

    for ordered in order.expressions:
        key = ordered.this
        desc = bool(ordered.args.get("desc", False))

        # Resolve key → column index via numeric literal or column reference.
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(expected_col_names):
                    sort_col_indices.append(col_num - 1)
                    descending.append(desc)
                    continue
            except (ValueError, TypeError):
                pass

        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in expected_col_names:
                sort_col_indices.append(expected_col_names.index(name))
                descending.append(desc)
                continue

        # Complex expression — skip ORDER BY validation entirely.
        return [], []

    return sort_col_indices, descending


def get_limit(query_sql: str) -> int | None:
    """Return the LIMIT value from SQL, or None if there is no LIMIT."""
    expr = sqlglot.parse_one(query_sql)
    limit_node = next((e for e in expr.find_all(sqlglot.exp.Limit)), None)
    if limit_node is None:
        return None
    limit_expr = limit_node.args.get("expression")
    if limit_expr is None:
        return None
    try:
        return int(limit_expr.this)
    except (ValueError, TypeError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Type normalization
# ---------------------------------------------------------------------------


def _is_decimal_like(series: pd.Series) -> bool:
    """True if the series contains Decimal values (object dtype or ArrowDtype decimal)."""
    if series.dtype == object:
        sample = series.dropna()
        return len(sample) > 0 and isinstance(sample.iloc[0], decimal.Decimal)
    try:
        import pyarrow as pa

        if hasattr(series.dtype, "pyarrow_dtype"):
            return pa.types.is_decimal(series.dtype.pyarrow_dtype)
    except ImportError:
        pass
    return False


def _is_temporal_like(series: pd.Series) -> bool:
    """
    True if series holds date/time values — including:
      - numpy datetime64 (any unit)
      - pandas DatetimeTZDtype
      - ArrowDtype date/time/timestamp (DuckDB 1.3+ with pyarrow)
      - object dtype whose first non-null value is datetime.date / pd.Timestamp
    """
    dtype = series.dtype
    if isinstance(dtype, np.dtype) and np.issubdtype(dtype, np.datetime64):
        return True
    if isinstance(dtype, pd.DatetimeTZDtype):
        return True
    try:
        import pyarrow as pa

        if hasattr(dtype, "pyarrow_dtype"):
            pt = dtype.pyarrow_dtype
            return pa.types.is_date(pt) or pa.types.is_time(pt) or pa.types.is_timestamp(pt)
    except ImportError:
        pass
    if dtype.kind == "O":  # object dtype (numpy dtype('O'), not Python builtin object)
        sample = series.dropna()
        if len(sample) > 0:
            val = sample.iloc[0]
            return isinstance(val, (datetime.date, pd.Timestamp))
    return False


def _normalize_df(df: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize df's column dtypes for comparison, using ref only for dtype detection
    (ref is never modified):
      - Decimal (object or ArrowDtype) → float64
      - int8/int16/int32 (numpy or pandas nullable) → int64
      - Temporal: convert to datetime64 if either df or ref column is temporal-like.
        Covers: object-strings ↔ datetime64, StringDtype ↔ datetime.date,
        ArrowDtype ↔ datetime64, datetime.date ↔ datetime64.
    """
    df = df.copy()
    _NARROW = (np.dtype("int8"), np.dtype("int16"), np.dtype("int32"))
    _NARROW_NULLABLE = ("Int8", "Int16", "Int32")

    for col, ref_col in zip(df.columns, ref.columns):
        # Decimal → float64
        if _is_decimal_like(df[col]):
            df[col] = pd.to_numeric(df[col])

        # Narrow int → int64 (numpy dtypes)
        if df[col].dtype in _NARROW:
            df[col] = df[col].astype("int64")

        # Narrow int → Int64 (pandas nullable integer types)
        if str(df[col].dtype) in _NARROW_NULLABLE:
            df[col] = df[col].astype("Int64")

        # Temporal normalization: convert if either side is temporal-like
        if _is_temporal_like(df[col]) or _is_temporal_like(ref[ref_col]):
            if not (isinstance(df[col].dtype, np.dtype) and np.issubdtype(df[col].dtype, np.datetime64)):
                df[col] = pd.to_datetime(df[col])

    return df


# ---------------------------------------------------------------------------
# Frame comparison helpers
# ---------------------------------------------------------------------------


def _is_float_col(series: pd.Series) -> bool:
    return pd.api.types.is_float_dtype(series)


def _non_float_col_indices(df: pd.DataFrame) -> list[int]:
    return [col for col in df.columns if not _is_float_col(df[col])]


def _sort_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by all non-float columns for deterministic tie-breaking, reset index."""
    non_float = _non_float_col_indices(df)
    if not non_float:
        return df.reset_index(drop=True)
    return df.sort_values(by=non_float, na_position="last").reset_index(drop=True)


def _is_null(v) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _assert_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    """
    Row-by-row comparison with rel_tol=1e-5, abs_tol=1e-8 for float columns.
    Both DataFrames must have matching shape and be pre-sorted/reset-indexed.
    Raises AssertionError describing up to MAX_MISMATCHES differences.
    """
    if len(df1) != len(df2):
        raise AssertionError(f"Row count mismatch: {len(df1)} vs {len(df2)}")

    float_cols = {col for col in df1.columns if _is_float_col(df1[col])}
    mismatches: list[str] = []

    records1 = df1.to_dict("records")
    records2 = df2.to_dict("records")

    for row_idx, (r1, r2) in enumerate(zip(records1, records2)):
        for col in df1.columns:
            v1, v2 = r1[col], r2[col]
            null1, null2 = _is_null(v1), _is_null(v2)

            if null1 and null2:
                continue
            if null1 or null2:
                mismatches.append(f"Row {row_idx}, col {col}: {v1!r} vs {v2!r} (null mismatch)")
            elif col in float_cols:
                fv1, fv2 = float(v1), float(v2)
                diff = abs(fv1 - fv2)
                tol = ABS_TOL + REL_TOL * max(abs(fv1), abs(fv2))
                if diff > tol:
                    mismatches.append(f"Row {row_idx}, col {col}: {fv1} vs {fv2} (diff={diff:.2e}, tol={tol:.2e})")
            elif v1 != v2:
                # Safety net: compare as Timestamps when one side is a date
                # string (e.g. Presto '1995-03-05') and the other is a
                # Timestamp object that slipped through _normalize_dtypes.
                try:
                    if pd.Timestamp(v1) == pd.Timestamp(v2):
                        continue
                except Exception:
                    pass
                mismatches.append(f"Row {row_idx}, col {col}: {v1!r} vs {v2!r}")

            if len(mismatches) >= MAX_MISMATCHES:
                break
        if len(mismatches) >= MAX_MISMATCHES:
            break

    if mismatches:
        truncated = f" (showing first {MAX_MISMATCHES})" if len(mismatches) >= MAX_MISMATCHES else ""
        raise AssertionError(f"Found {len(mismatches)} mismatch(es){truncated}:\n  " + "\n  ".join(mismatches))


def _find_tie_start(df: pd.DataFrame, sort_col_indices: list[int], boundary: pd.Series) -> int:
    """
    Scan upward from the last row and return the index where the contiguous
    block of boundary-tie rows begins. Rows at or above this index are non-ties.
    """
    for i in range(len(df) - 1, -1, -1):
        row = df.iloc[i]
        for col_idx in sort_col_indices:
            v, b = row[col_idx], boundary[col_idx]
            if _is_null(b):
                if not _is_null(v):
                    return i + 1
            elif _is_float_col(df[col_idx]):
                fval = float(b)
                if abs(float(v) - fval) > ABS_TOL + REL_TOL * abs(fval):
                    return i + 1
            elif v != b:
                return i + 1
    return 0


# ---------------------------------------------------------------------------
# Main comparison entry point
# ---------------------------------------------------------------------------


def compare_result_frames(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    query_sql: str,
) -> None:
    """
    Full comparison pipeline. Raises AssertionError on any mismatch.

    Steps:
      1. Validate column count.
      2. Reset both frames to positional integer column indices (0, 1, 2, ...).
      3. Normalize dtypes.
      4. Validate row count.
      5. Parse ORDER BY / LIMIT from SQL via sqlglot.
      6. If ORDER BY + LIMIT: sort both frames, find the tie boundary, compare.
    """
    # 1. Column count check
    if len(actual.columns) != len(expected.columns):
        raise AssertionError(
            f"Column count mismatch: {len(actual.columns)} (actual) vs {len(expected.columns)} (expected)\n"
            f"  actual:   {list(actual.columns)}\n"
            f"  expected: {list(expected.columns)}"
        )

    # 2. Capture expected column names (used for ORDER BY name resolution), then
    # reset both frames to positional integer indices for all subsequent operations.
    expected_col_names = list(expected.columns)
    n = len(expected_col_names)
    actual = actual.copy()
    expected = expected.copy()
    actual.columns = range(n)
    expected.columns = range(n)

    # 3. Normalize dtypes
    actual = _normalize_df(actual, ref=expected)
    expected = _normalize_df(expected, ref=actual)

    # 4. Row count
    if len(actual) != len(expected):
        raise AssertionError(f"Row count mismatch: {len(actual)} (actual) vs {len(expected)} (expected)")

    # 5. Parse ORDER BY / LIMIT
    sort_col_indices, descending = get_orderby_info(query_sql, expected_col_names)
    limit = get_limit(query_sql)

    if not sort_col_indices or limit is None:
        # No parseable ORDER BY, or ORDER BY without LIMIT — sort both sides and compare
        _assert_frames_equal(_sort_for_comparison(actual), _sort_for_comparison(expected))
        return

    # 6. ORDER BY + LIMIT: sort both frames, then split at the tie boundary
    ascending = [not d for d in descending]
    actual_sorted = actual.sort_values(by=sort_col_indices, ascending=ascending, na_position="last").reset_index(
        drop=True
    )
    expected_sorted = expected.sort_values(by=sort_col_indices, ascending=ascending, na_position="last").reset_index(
        drop=True
    )
    boundary = actual_sorted.iloc[-1]

    actual_tie_start = _find_tie_start(actual_sorted, sort_col_indices, boundary)
    expected_tie_start = _find_tie_start(expected_sorted, sort_col_indices, boundary)

    tie_count = len(actual_sorted) - actual_tie_start
    if tie_count > 0:
        pct = 100 * tie_count / len(actual_sorted)
        warnings.warn(
            f"{tie_count}/{len(actual_sorted)} rows ({pct:.0f}%) are boundary ties; "
            "their non-sort columns are excluded from comparison"
        )

    # Non-tie rows: full comparison
    _assert_frames_equal(
        _sort_for_comparison(actual_sorted.iloc[:actual_tie_start]),
        _sort_for_comparison(expected_sorted.iloc[:expected_tie_start]),
    )

    # Tie rows: compare only sort columns (non-sort columns may legitimately differ)
    _assert_frames_equal(
        _sort_for_comparison(actual_sorted.iloc[actual_tie_start:][sort_col_indices]),
        _sort_for_comparison(expected_sorted.iloc[expected_tie_start:][sort_col_indices]),
    )


# ---------------------------------------------------------------------------
# Status-returning wrapper (used by benchmark validation path)
# ---------------------------------------------------------------------------


def validate_query_result(
    query_id: str,
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    query_sql: str,
) -> tuple[ValidationStatus, str | None]:
    """
    Compare actual vs expected for one query. Returns (status, message).

    Status values:
      "passed"           — comparison succeeded
      "failed"           — comparison raised an exception
      "expected-failure" — query is in XFAIL_IF_EMPTY and returned no rows
    """
    if query_id.lower() in XFAIL_IF_EMPTY and actual.empty:
        return (
            "expected-failure",
            f"{query_id.upper()} returned no rows: known float calculation mismatch "
            "in MAX(total_revenue) subquery causes empty result with float data",
        )

    try:
        compare_result_frames(actual, expected, query_sql)
        return "passed", None
    except Exception as e:
        return "failed", f"{type(e).__name__}: {e}"[:500]
