# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared result comparison logic for TPC-H/TPC-DS query validation.

Used by both integration tests (comparing live query engine results against a
DuckDB reference) and benchmark validation (comparing result parquet files
against expected parquet files).

Comparison behaviour
--------------------
- Column names are validated; Presto _colN anonymous aggregate columns are
  renamed to match the expected column names positionally.
- Decimal columns are cast to float64; narrow integers (int8/int16/int32) are
  widened to int64; string columns are cast to temporal when the expected
  column is temporal (Presto may write dates as strings).
- Floating-point values are compared with rel_tol=1e-5, abs_tol=1e-8.
- ORDER BY is extracted dynamically from the SQL using sqlglot:
    - Validates that both frames are sorted by the ORDER BY columns.
    - Tie-breaking is resolved by sorting on all non-float columns.
- For queries with ORDER BY + LIMIT, rows at the limit boundary are compared
  only on the sort columns (other columns may legitimately differ for ties).
"""

import datetime
import decimal
import re
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


def get_orderby_info(query_sql: str, column_names: list[str]) -> tuple[list[tuple[str, bool]], bool]:
    """
    Extract ORDER BY information from SQL using sqlglot.

    Returns:
        (sort_by, nulls_last) where sort_by is [(col_name, descending), ...]
        and nulls_last reflects the first sort column's setting (defaulting to
        True for ASC, matching DuckDB's default).

    Returns ([], True) when there is no ORDER BY, or when any ORDER BY
    expression is too complex to map to a result column (CASE, aggregate, etc.).
    """
    expr = sqlglot.parse_one(query_sql)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return [], True

    sort_by = []
    nulls_last = True  # DuckDB default for ASC

    for i, ordered in enumerate(order.expressions):
        key = ordered.this
        descending = bool(ordered.args.get("desc", False))

        # Determine nulls handling for this column.
        # DuckDB defaults: ASC → NULLS LAST, DESC → NULLS FIRST.
        nulls_arg = ordered.args.get("nulls")
        if nulls_arg is not None:
            col_nulls_last = not isinstance(nulls_arg, sqlglot.exp.NullsFirst)
        else:
            col_nulls_last = not descending

        if i == 0:
            nulls_last = col_nulls_last

        # Resolve key → column name via numeric literal or column reference.
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(column_names):
                    sort_by.append((column_names[col_num - 1], descending))
                    continue
            except (ValueError, TypeError):
                pass

        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in column_names:
                sort_by.append((name, descending))
                continue

        # Complex expression — skip ORDER BY validation entirely.
        return [], True

    return sort_by, nulls_last


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
# Column name reconciliation
# ---------------------------------------------------------------------------


def _reconcile_col_names(actual: pd.DataFrame, expected: pd.DataFrame) -> pd.DataFrame:
    """
    Rename Presto _colN anonymous aggregate columns to match expected column
    names positionally.  Only renames when the actual column matches _colN
    and the expected column at the same position has a different name.
    """
    if len(actual.columns) != len(expected.columns):
        return actual
    renames = {
        res: exp for res, exp in zip(actual.columns, expected.columns) if re.fullmatch(r"_col\d+", res) and res != exp
    }
    return actual.rename(columns=renames) if renames else actual


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


def _normalize_dtypes(actual: pd.DataFrame, expected: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize column dtypes for fair comparison:
      - Decimal (object or ArrowDtype) → float64
      - int8/int16/int32 (numpy or pandas nullable) → int64
      - object string → temporal when the expected column is temporal-like
        (handles numpy datetime64, ArrowDtype date/timestamp, and tz-aware dtypes)
    Applied symmetrically where both sides need it.
    """
    actual = actual.copy()
    expected = expected.copy()

    for col in actual.columns:
        if col not in expected.columns:
            continue

        # Decimal → float64
        if _is_decimal_like(actual[col]):
            actual[col] = pd.to_numeric(actual[col], errors="coerce")
        if _is_decimal_like(expected[col]):
            expected[col] = pd.to_numeric(expected[col], errors="coerce")

        # Narrow int → int64 (numpy dtypes)
        _NARROW = (np.dtype("int8"), np.dtype("int16"), np.dtype("int32"))
        if actual[col].dtype in _NARROW:
            actual[col] = actual[col].astype("int64")
        if expected[col].dtype in _NARROW:
            expected[col] = expected[col].astype("int64")

        # Narrow int → Int64 (pandas nullable integer types)
        _NARROW_NULLABLE = ("Int8", "Int16", "Int32")
        if str(actual[col].dtype) in _NARROW_NULLABLE:
            actual[col] = actual[col].astype("Int64")
        if str(expected[col].dtype) in _NARROW_NULLABLE:
            expected[col] = expected[col].astype("Int64")

        # Temporal normalization: if either side is temporal-like, convert both
        # to numpy datetime64.  This covers all combinations:
        #   object-strings ↔ datetime64       (Presto live vs DuckDB)
        #   StringDtype    ↔ datetime.date    (parquet read in pandas 3.0)
        #   ArrowDtype     ↔ datetime64       (DuckDB 1.3+ with pyarrow)
        #   datetime.date  ↔ datetime64       (mixed parquet sources)
        if _is_temporal_like(actual[col]) or _is_temporal_like(expected[col]):
            _already_dt64 = lambda s: isinstance(s.dtype, np.dtype) and np.issubdtype(s.dtype, np.datetime64)  # noqa: E731
            if not _already_dt64(actual[col]):
                try:
                    actual[col] = pd.to_datetime(actual[col])
                except Exception:
                    pass
            if not _already_dt64(expected[col]):
                try:
                    expected[col] = pd.to_datetime(expected[col])
                except Exception:
                    pass

    return actual, expected


# ---------------------------------------------------------------------------
# Frame comparison helpers
# ---------------------------------------------------------------------------


def _is_float_col(series: pd.Series) -> bool:
    return pd.api.types.is_float_dtype(series)


def _non_float_col_names(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if not _is_float_col(df[col])]


def _sort_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by all non-float columns for deterministic tie-breaking, reset index."""
    non_float = _non_float_col_names(df)
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
                mismatches.append(f"Row {row_idx}, col '{col}': {v1!r} vs {v2!r} (null mismatch)")
            elif col in float_cols:
                fv1, fv2 = float(v1), float(v2)
                diff = abs(fv1 - fv2)
                tol = ABS_TOL + REL_TOL * max(abs(fv1), abs(fv2))
                if diff > tol:
                    mismatches.append(f"Row {row_idx}, col '{col}': {fv1} vs {fv2} (diff={diff:.2e}, tol={tol:.2e})")
            elif v1 != v2:
                # Safety net: compare as Timestamps when one side is a date
                # string (e.g. Presto '1995-03-05') and the other is a
                # Timestamp object that slipped through _normalize_dtypes.
                try:
                    if pd.Timestamp(v1) == pd.Timestamp(v2):
                        continue
                except Exception:
                    pass
                mismatches.append(f"Row {row_idx}, col '{col}': {v1!r} vs {v2!r}")

            if len(mismatches) >= MAX_MISMATCHES:
                break
        if len(mismatches) >= MAX_MISMATCHES:
            break

    if mismatches:
        truncated = f" (showing first {MAX_MISMATCHES})" if len(mismatches) >= MAX_MISMATCHES else ""
        raise AssertionError(f"Found {len(mismatches)} mismatch(es){truncated}:\n  " + "\n  ".join(mismatches))


def _validate_sort_order(
    df: pd.DataFrame,
    sort_cols: list[str],
    descending: list[bool],
    nulls_last: bool,
    side: str,
) -> None:
    """Assert that df is already sorted by the specified columns."""
    ascending = [not d for d in descending]
    na_position = "last" if nulls_last else "first"
    expected_order = (
        df[sort_cols].sort_values(by=sort_cols, ascending=ascending, na_position=na_position).reset_index(drop=True)
    )
    actual_order = df[sort_cols].reset_index(drop=True)
    if not actual_order.equals(expected_order):
        directions = ["DESC" if d else "ASC" for d in descending]
        raise AssertionError(f"{side} result is not sorted by {list(zip(sort_cols, directions))}")


def _build_non_tie_mask(
    df: pd.DataFrame,
    sort_cols: list[str],
    descending: list[bool],
    boundary: pd.Series,
) -> pd.Series:
    """
    Return a boolean mask: True for rows that are strictly 'better' than the
    boundary row (i.e. would appear before it in the sort order).
    A tie requires ALL sort columns to equal the boundary value.
    """
    mask = pd.Series(False, index=df.index)
    for col, desc in zip(sort_cols, descending):
        val = boundary[col]
        col_series = df[col]
        if _is_null(val):
            # NaN boundary: non-NaN values come before it (nulls last)
            mask |= col_series.notna()
        elif _is_float_col(col_series):
            mask |= (col_series - float(val)).abs() > 2 * ABS_TOL
        else:
            mask |= col_series > val if desc else col_series < val
    return mask


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
      1. Reconcile Presto _colN column names.
      2. Validate column name match.
      3. Normalize dtypes.
      4. Validate row count.
      5. Parse ORDER BY / LIMIT from SQL via sqlglot.
      6. If ORDER BY present: validate sort order on both frames.
      7. If ORDER BY + LIMIT: handle boundary ties.
      8. Sort by non-float columns; compare value-by-value with tolerance.
    """
    # 1 & 2. Column reconciliation and validation
    actual = _reconcile_col_names(actual, expected)
    if list(actual.columns) != list(expected.columns):
        extra = set(actual.columns) - set(expected.columns)
        missing = set(expected.columns) - set(actual.columns)
        raise AssertionError(
            f"Column name mismatch — extra: {extra}, missing: {missing}\n"
            f"  actual:   {list(actual.columns)}\n"
            f"  expected: {list(expected.columns)}"
        )

    # 3. Normalize dtypes
    actual, expected = _normalize_dtypes(actual, expected)

    # 4. Row count
    if len(actual) != len(expected):
        raise AssertionError(f"Row count mismatch: {len(actual)} (actual) vs {len(expected)} (expected)")

    # 5. Parse ORDER BY / LIMIT
    sort_by, nulls_last = get_orderby_info(query_sql, list(actual.columns))
    limit = get_limit(query_sql)

    if not sort_by:
        # No ORDER BY (or unparsable) — sort both sides and compare
        _assert_frames_equal(_sort_for_comparison(actual), _sort_for_comparison(expected))
        return

    sort_cols = [col for col, _ in sort_by]
    descending = [d for _, d in sort_by]

    # 6. Validate sort order
    _validate_sort_order(actual, sort_cols, descending, nulls_last, "actual")
    _validate_sort_order(expected, sort_cols, descending, nulls_last, "expected")

    if limit is None:
        # ORDER BY, no LIMIT — sort by non-float cols for tie-breaking
        _assert_frames_equal(_sort_for_comparison(actual), _sort_for_comparison(expected))
        return

    # 7. ORDER BY + LIMIT: split into non-ties and boundary ties
    ascending = [not d for d in descending]
    na_position = "last" if nulls_last else "first"
    boundary = actual.sort_values(by=sort_cols, ascending=ascending, na_position=na_position).iloc[-1][sort_cols]

    actual_non_tie_mask = _build_non_tie_mask(actual, sort_cols, descending, boundary)
    expected_non_tie_mask = _build_non_tie_mask(expected, sort_cols, descending, boundary)

    actual_non_ties = actual[actual_non_tie_mask].reset_index(drop=True)
    expected_non_ties = expected[expected_non_tie_mask].reset_index(drop=True)
    actual_ties = actual[~actual_non_tie_mask].reset_index(drop=True)
    expected_ties = expected[~expected_non_tie_mask].reset_index(drop=True)

    # Non-tie rows: full comparison
    _assert_frames_equal(_sort_for_comparison(actual_non_ties), _sort_for_comparison(expected_non_ties))

    # Tie rows: compare only sort columns (non-sort columns may legitimately differ)
    _assert_frames_equal(
        _sort_for_comparison(actual_ties[sort_cols]),
        _sort_for_comparison(expected_ties[sort_cols]),
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
