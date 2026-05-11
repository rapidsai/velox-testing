# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared result comparison logic for TPC-H/TPC-DS query validation.

Used by both the integration test path (live query engine results vs DuckDB
reference) and the benchmark validation path (result parquet files vs expected
parquet files). See compare_result_frames for full comparison semantics.
"""

import datetime
import warnings
from typing import Literal

import numpy as np
import pandas as pd
import sqlglot

REL_TOL = 1e-5
ABS_TOL = 1e-8
MAX_MISMATCHES = 5

# Relative precision for float canonicalization. Chosen so engine-level
# precision noise (typically <= 1e-10 relative for stable summation) is
# flattened to bit-equal across engines, while distinct logical values
# (relative gap >= 1e-7 in our queries) stay distinct.
CANONICALIZE_RELATIVE_PRECISION = 1e-8

# Known queries that return empty results due to float precision issues.
# These are marked as expected failures rather than test failures.
XFAIL_IF_EMPTY: set[str] = {"q15"}

ValidationStatus = Literal["passed", "failed", "expected-failure", "not-validated"]


# ---------------------------------------------------------------------------
# SQL parsing helpers
# ---------------------------------------------------------------------------


def get_orderby_col_indices(query_sql: str, expected_col_names: list[str]) -> tuple[list[int], list[bool]]:
    """
    Extract ORDER BY column positions and directions from SQL using sqlglot.

    Returns (sort_col_indices, descending) — parallel lists of 0-based column
    indices and DESC flags. Returns ([], []) when there is no ORDER BY, or when
    any ORDER BY expression is too complex to map to a result column.
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
        warnings.warn(
            f"ORDER BY expression {key.sql()!r} couldn't be mapped to a result column; "
            "engine sort verification will be skipped for this query."
        )
        return [], []

    return sort_col_indices, descending


def get_limit(query_sql: str) -> int | None:
    """Return the LIMIT value from SQL, or None if there is no LIMIT (or the
    LIMIT expression isn't a simple integer literal)."""
    expr = sqlglot.parse_one(query_sql)
    limit_node = next((e for e in expr.find_all(sqlglot.exp.Limit)), None)
    if limit_node is None:
        return None
    try:
        return int(limit_node.args["expression"].this)
    except (KeyError, ValueError, TypeError, AttributeError):
        warnings.warn(
            f"LIMIT clause {limit_node.sql()!r} couldn't be parsed as an integer literal; "
            "treating as no LIMIT (boundary tie handling will be skipped)."
        )
        return None


# ---------------------------------------------------------------------------
# Type normalization
# ---------------------------------------------------------------------------


def _first_non_null(col: pd.Series):
    """Return the first value in col that isn't None/NaN/NaT/NA, or None if all null."""
    for v in col:
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        return v
    return None


def _normalize_to_expected(actual: pd.DataFrame, expected: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce each column of actual to match the corresponding expected column.

    Runs two alignment passes per column:
      1. Dtype alignment — astype actual to expected's dtype if they differ.
         (e.g., prestodb returns DATE as str while DuckDB returns datetime64;
          INTEGER comes back as int32 from DuckDB but int64 from Presto.)
      2. Value-type alignment — even when dtypes match (or after the astype
         in step 1), the Python values inside may differ in type (e.g.,
         parquet round-trips where one engine produces str dates and the
         other datetime.date; pandas 3.0 also wraps str in pyarrow-backed
         string dtypes that don't equal object). Sample the first non-null
         on each side and coerce actual's values to match expected's.

    Comparison is positional — column labels do not need to match.
    """
    out = actual.copy()
    for i in range(actual.shape[1]):
        a_col = actual.iloc[:, i]
        e_col = expected.iloc[:, i]
        target_dtype = e_col.dtype

        # 1. Dtype alignment.
        if a_col.dtype != target_dtype:
            a_col = a_col.astype(target_dtype)
            out.isetitem(i, a_col)

        # 2. Value-type alignment (independent of dtype — pandas can hold
        #    Python str in pyarrow-backed string dtypes too, so an object
        #    dtype check alone isn't sufficient).
        a_sample = _first_non_null(a_col)
        e_sample = _first_non_null(e_col)
        if a_sample is None or e_sample is None:
            continue
        if isinstance(a_sample, str) and isinstance(e_sample, datetime.date):
            # actual has str dates, expected has datetime.date objects —
            # parse the strings and yield datetime.date to match.
            out.isetitem(i, pd.to_datetime(a_col).dt.date)
    return out


# ---------------------------------------------------------------------------
# Frame comparison helpers
# ---------------------------------------------------------------------------


def _is_float_col(series: pd.Series) -> bool:
    return pd.api.types.is_float_dtype(series)


def _canonicalize_floats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round each float column to a fixed relative precision so engine-level
    precision noise becomes bit-identical across engines, while distinct
    logical values stay distinct. The quantum at value v is roughly
    10^floor(log10(|v|)) * CANONICALIZE_RELATIVE_PRECISION, so the precision
    is relative to magnitude.
    """
    out = df.copy()
    for i in range(df.shape[1]):
        if not _is_float_col(df.iloc[:, i]):
            continue
        col = df.iloc[:, i].to_numpy(dtype=np.float64)
        with np.errstate(divide="ignore", invalid="ignore"):
            magnitudes = np.where(col != 0, 10.0 ** np.floor(np.log10(np.abs(col))), 1.0)
            quantum = magnitudes * CANONICALIZE_RELATIVE_PRECISION
            rounded = np.where(col != 0, np.round(col / quantum) * quantum, 0.0)
        # Preserve NaN (np.where above propagates NaN through both branches because
        # NaN != 0 evaluates True; NaN/anything = NaN; np.round(NaN) = NaN).
        out.isetitem(i, rounded)
    return out


def _verify_sort(df: pd.DataFrame, sort_col_indices: list[int], descending: list[bool]) -> None:
    """
    Verify that df is correctly sorted per ORDER BY columns and directions.

    Walks consecutive rows; for each pair, the ORDER BY columns must be in the
    correct direction or tied. Float columns use exact equality for "tied"
    (since this runs before canonicalization, engines having seen the values
    as exact-equal is what triggered them to apply the next ORDER BY column);
    if a float pair is non-equal, we still allow within-tolerance flips by
    not enforcing direction (the engine sorted by its own bit values, which
    may have been ULP-different from the reference).

    Raises AssertionError on a real sort violation (wrong direction outside
    tolerance).
    """
    n = len(df)
    if n <= 1 or not sort_col_indices:
        return
    for i in range(1, n):
        for col_idx, desc in zip(sort_col_indices, descending):
            prev = df.iloc[i - 1, col_idx]
            curr = df.iloc[i, col_idx]
            if curr == prev:
                continue  # exact tie — engine should have applied next column
            if _is_float_col(df.iloc[:, col_idx]):
                # Tolerance-tied float: engine may have ranked by its own bit
                # value rather than tying-then-applying-next-column. Don't
                # enforce ordering of subsequent columns at this position.
                tol = ABS_TOL + REL_TOL * max(abs(curr), abs(prev))
                if abs(curr - prev) <= tol:
                    break
            # Distinct values — direction must be respected.
            if (desc and curr > prev) or (not desc and curr < prev):
                raise AssertionError(
                    f"Sort violation at row {i}, column {col_idx}: "
                    f"prev={prev!r}, curr={curr!r}, but ORDER BY is {'DESC' if desc else 'ASC'}"
                )
            break


def _sort_by_positions(df: pd.DataFrame, positions: list[int], ascending: list[bool]) -> pd.DataFrame:
    """Sort df by integer column positions, returning a new frame with reset index."""
    labels = df.columns[positions].tolist()
    return df.sort_values(by=labels, ascending=ascending, na_position="last").reset_index(drop=True)


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
    Comparison is positional — column labels are not used.
    Raises AssertionError describing up to MAX_MISMATCHES differences.
    """
    if len(df1) != len(df2):
        raise AssertionError(f"Row count mismatch: {len(df1)} vs {len(df2)}")

    float_col_positions = {i for i in range(df1.shape[1]) if _is_float_col(df1.iloc[:, i])}
    mismatches: list[str] = []

    rows1 = df1.to_dict("split")["data"]
    rows2 = df2.to_dict("split")["data"]

    for row_idx, (r1, r2) in enumerate(zip(rows1, rows2)):
        for col_idx, (v1, v2) in enumerate(zip(r1, r2)):
            # TPC-DS produces three flavours of null in result frames:
            #   - float NaN (divide-by-zero, empty aggregations)
            #   - None (ROLLUP NULL labels, missing strings)
            #   - pd.NA (nullable Int columns)
            # Each behaves differently under == / != / float(): NaN != NaN is True
            # so the float-tolerance branch silently passes any NaN; float(None)
            # raises TypeError; pd.NA raises in boolean context. Detect nulls up
            # front so the type-specific branches see only real values.
            null1, null2 = _is_null(v1), _is_null(v2)

            if null1 and null2:
                continue
            if null1 or null2:
                mismatches.append(f"Row {row_idx}, col {col_idx}: {v1!r} vs {v2!r} (null mismatch)")
            elif col_idx in float_col_positions:
                fv1, fv2 = float(v1), float(v2)
                diff = abs(fv1 - fv2)
                tol = ABS_TOL + REL_TOL * max(abs(fv1), abs(fv2))
                if diff > tol:
                    mismatches.append(f"Row {row_idx}, col {col_idx}: {fv1} vs {fv2} (diff={diff:.2e}, tol={tol:.2e})")
            elif v1 != v2:
                mismatches.append(f"Row {row_idx}, col {col_idx}: {v1!r} vs {v2!r}")

            if len(mismatches) >= MAX_MISMATCHES:
                break
        if len(mismatches) >= MAX_MISMATCHES:
            break

    if mismatches:
        truncated = f" (showing first {MAX_MISMATCHES})" if len(mismatches) >= MAX_MISMATCHES else ""
        raise AssertionError(f"Found {len(mismatches)} mismatch(es){truncated}:\n  " + "\n  ".join(mismatches))


def _find_last_tie_start(orderby_df: pd.DataFrame) -> int:
    """
    Return the row index where the contiguous tie block at the end of
    orderby_df begins. Rows from this index onward all share the same values
    across orderby_df's columns as the last row. Returns 0 for empty frames
    or frames where every row ties with the last.
    """
    n = len(orderby_df)
    if n == 0:
        return 0
    arr = orderby_df.to_numpy()
    last_row = arr[-1]
    for i in range(n - 2, -1, -1):
        if (arr[i] != last_row).any():
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

    All cross-frame access is positional (iloc / to_dict("split")), so column
    labels never need to align between actual and expected.

    Steps:
      1. Validate column count and capture expected column names.
      2. Normalize dtypes.
      3. Validate row count.
      4. Parse ORDER BY / LIMIT from SQL.
      5. Verify each frame's ORDER BY column ordering (per-frame sort check).
      6. Canonicalize floats so engine-level precision noise becomes bit-equal.
      7. Sort both frames into a canonical order (ORDER BY columns first, then
         remaining columns) so position-by-position comparison aligns rows.
      8. Compare position-by-position, with LIMIT-boundary tie group skipped
         on its non-ORDER-BY columns (engines may have selected different
         tied rows at the cutoff).
    """
    # 1. Column count check
    if len(actual.columns) != len(expected.columns):
        raise AssertionError(
            f"Column count mismatch: {len(actual.columns)} (actual) vs {len(expected.columns)} (expected)\n"
            f"  actual:   {list(actual.columns)}\n"
            f"  expected: {list(expected.columns)}"
        )
    expected_col_names = list(expected.columns)

    # 2. Coerce actual's dtypes to match expected's
    actual = _normalize_to_expected(actual, expected)

    # 3. Row count
    if len(actual) != len(expected):
        raise AssertionError(f"Row count mismatch: {len(actual)} (actual) vs {len(expected)} (expected)")

    # 4. Parse ORDER BY / LIMIT
    sort_col_indices, descending = get_orderby_col_indices(query_sql, expected_col_names)
    limit = get_limit(query_sql)

    # 5. Verify each frame is correctly sorted per the SQL ORDER BY (before
    # canonicalization, so we see what the engines actually produced).
    _verify_sort(actual, sort_col_indices, descending)
    _verify_sort(expected, sort_col_indices, descending)

    # 6. Canonicalize floats — engine precision noise becomes bit-identical.
    actual = _canonicalize_floats(actual)
    expected = _canonicalize_floats(expected)

    # 7. Canonical sort: ORDER BY columns first (with directions), then all
    # remaining columns ascending for a fully deterministic order.
    n_cols = actual.shape[1]
    sort_set = set(sort_col_indices)
    all_positions = list(sort_col_indices) + [i for i in range(n_cols) if i not in sort_set]
    ascending = [not d for d in descending] + [True] * (n_cols - len(sort_col_indices))
    actual = _sort_by_positions(actual, all_positions, ascending)
    expected = _sort_by_positions(expected, all_positions, ascending)

    # 8. Compare. With LIMIT, the last tie group on ORDER BY columns may
    # contain different rows in actual vs expected (engines may have selected
    # different subsets of tied rows at the LIMIT cutoff). Skip the
    # non-ORDER-BY columns there; ORDER BY columns are still verified by the
    # per-frame _verify_sort and the position-by-position compare.
    if sort_col_indices and limit is not None:
        last_start = _find_last_tie_start(actual.iloc[:, sort_col_indices])
        _assert_frames_equal(actual.iloc[:last_start], expected.iloc[:last_start])
        _assert_frames_equal(
            actual.iloc[last_start:, sort_col_indices],
            expected.iloc[last_start:, sort_col_indices],
        )
        return

    _assert_frames_equal(actual, expected)


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
