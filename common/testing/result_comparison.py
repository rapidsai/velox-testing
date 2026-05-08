# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Shared result comparison logic for TPC-H/TPC-DS query validation.

Used by both the integration test path (live query engine results vs DuckDB
reference) and the benchmark validation path (result parquet files vs expected
parquet files). See compare_result_frames for full comparison semantics.
"""

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


def get_orderby_col_indices(query_sql: str, expected_col_names: list[str]) -> list[int]:
    """
    Extract ORDER BY column positions from SQL using sqlglot.

    Returns a list of 0-based column indices resolved against expected_col_names.
    Returns [] when there is no ORDER BY, or when any ORDER BY expression is too
    complex to map to a result column (CASE, aggregate, etc.).
    """
    expr = sqlglot.parse_one(query_sql)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return []

    sort_col_indices: list[int] = []

    for ordered in order.expressions:
        key = ordered.this

        # Resolve key → column index via numeric literal or column reference.
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(expected_col_names):
                    sort_col_indices.append(col_num - 1)
                    continue
            except (ValueError, TypeError):
                pass

        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in expected_col_names:
                sort_col_indices.append(expected_col_names.index(name))
                continue

        # Complex expression — skip ORDER BY validation entirely.
        warnings.warn(
            f"ORDER BY expression {key.sql()!r} couldn't be mapped to a result column; "
            "engine sort verification will be skipped for this query."
        )
        return []

    return sort_col_indices


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


def _normalize_to_expected(actual: pd.DataFrame, expected: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce each column of actual to the dtype of the corresponding expected column.

    Engines and drivers can land identically-sourced columns in different dtypes —
    prestodb returns DATE as str while DuckDB returns datetime64; INTEGER comes
    back as int32 from DuckDB but int64 from Presto. astype(target) handles all
    cases we hit in practice: str → datetime64, narrow int → wider int, etc.
    Comparison is positional — column labels do not need to match.
    """
    out = actual.copy()
    for i in range(actual.shape[1]):
        target_dtype = expected.iloc[:, i].dtype
        if actual.iloc[:, i].dtype != target_dtype:
            out.isetitem(i, actual.iloc[:, i].astype(target_dtype))
    return out


# ---------------------------------------------------------------------------
# Frame comparison helpers
# ---------------------------------------------------------------------------


def _is_float_col(series: pd.Series) -> bool:
    return pd.api.types.is_float_dtype(series)


def _sort_by_non_float(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort df by all non-float columns for deterministic position-by-position
    comparison. Float columns are excluded as sort keys because actual and
    expected are sorted independently — if the two engines computed slightly
    different float values for the same logical row, sorting each frame by
    those floats could land them at different row positions, breaking the
    alignment that position-by-position comparison relies on. Non-float
    columns (strings, ints, dates) match exactly across engines, so they
    always produce the same row order on both sides.

    Used both for the no-ORDER BY case (entire frame) and for individual tie
    groups (where ORDER BY columns are constant within the group, so sorting
    by them is a no-op).
    """
    non_float_labels = df.columns[[i for i in range(df.shape[1]) if not _is_float_col(df.iloc[:, i])]].tolist()
    if not non_float_labels:
        return df.reset_index(drop=True)
    return df.sort_values(by=non_float_labels, na_position="last").reset_index(drop=True)


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


def _identify_tie_groups(orderby_df: pd.DataFrame) -> list[tuple[int, int]]:
    """
    Return [(start, end), ...] row index ranges for contiguous rows tied on all
    columns of orderby_df. Each range is half-open: end is exclusive.
    """
    n = len(orderby_df)
    if n == 0:
        return []
    arr = orderby_df.to_numpy()
    is_new_group = np.empty(n, dtype=bool)
    is_new_group[0] = True
    is_new_group[1:] = (arr[1:] != arr[:-1]).any(axis=1)
    starts = np.flatnonzero(is_new_group).tolist()
    ends = [*starts[1:], n]
    return list(zip(starts, ends))


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

    All cross-frame access is positional (iloc/to_dict("split")), so column
    labels never need to align between actual and expected.

    Steps:
      1. Validate column count.
      2. Capture expected column names for ORDER BY name resolution.
      3. Normalize dtypes.
      4. Validate row count.
      5. Parse ORDER BY / LIMIT from SQL via sqlglot.
      6. Verify ORDER BY columns match, then compare each tie group's rows
         (skipping the boundary tie group when LIMIT is present).
    """
    # 1. Column count check
    if len(actual.columns) != len(expected.columns):
        raise AssertionError(
            f"Column count mismatch: {len(actual.columns)} (actual) vs {len(expected.columns)} (expected)\n"
            f"  actual:   {list(actual.columns)}\n"
            f"  expected: {list(expected.columns)}"
        )

    # 2. Capture expected column names for ORDER BY name resolution.
    expected_col_names = list(expected.columns)

    # 3. Coerce actual's dtypes to match expected's
    actual = _normalize_to_expected(actual, expected)

    # 4. Row count
    if len(actual) != len(expected):
        raise AssertionError(f"Row count mismatch: {len(actual)} (actual) vs {len(expected)} (expected)")

    # 5. Parse ORDER BY / LIMIT
    sort_col_indices = get_orderby_col_indices(query_sql, expected_col_names)
    limit = get_limit(query_sql)

    if not sort_col_indices:
        # No parseable ORDER BY — sort everything and compare.
        _assert_frames_equal(_sort_by_non_float(actual), _sort_by_non_float(expected))
        return

    # 6a. Verify the engines sorted the ORDER BY columns the same way.
    _assert_frames_equal(
        actual.iloc[:, sort_col_indices],
        expected.iloc[:, sort_col_indices],
    )

    # 6b. Identify tie groups based on ORDER BY values. Step 6a established that
    # the ORDER BY columns match position-by-position, so the tie groups are
    # identical between actual and expected.
    tie_groups = _identify_tie_groups(actual.iloc[:, sort_col_indices])

    # 6c. Compare each tie group's full rows, sorting within group for
    # deterministic position-by-position comparison. With LIMIT, skip the last
    # tie group: engines may have selected different subsets of tied rows at
    # the cutoff (it's non-deterministic which subsets gets selected).
    # It's ORDER BY columns are already covered by 6a.
    skip_last = limit is not None
    for i, (start, end) in enumerate(tie_groups):
        if skip_last and i == len(tie_groups) - 1:
            continue
        _assert_frames_equal(
            _sort_by_non_float(actual.iloc[start:end]),
            _sort_by_non_float(expected.iloc[start:end]),
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
