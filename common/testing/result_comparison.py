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

    Returns a list of 0-based column indices. Returns [] when there is no
    ORDER BY, or when any ORDER BY expression is too complex to map to a
    result column (CASE, aggregate, etc.).
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

        # Complex expression — skip ORDER BY tie-group handling for this query.
        warnings.warn(
            f"ORDER BY expression {key.sql()!r} couldn't be mapped to a result column; "
            "ORDER BY tie-group handling will be skipped for this query."
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


def _sort_preserving_orderby(df: pd.DataFrame, sort_col_indices: list[int]) -> pd.DataFrame:
    """
    Sort df preserving the engine-given ORDER BY row order. Within ORDER BY
    tie groups, sort by non-float non-ORDER-BY columns first (these are
    exact-equal across engines), then by float non-ORDER-BY columns (which
    may differ by ULPs but only matter when all non-float keys tie).

    When sort_col_indices is empty (no parseable ORDER BY in the SQL), sort
    the entire frame by [non-float, float] columns.
    """
    df = df.reset_index(drop=True)

    sort_set = set(sort_col_indices)
    non_orderby_positions = [i for i in range(df.shape[1]) if i not in sort_set]
    non_float_pos = [i for i in non_orderby_positions if not _is_float_col(df.iloc[:, i])]
    float_pos = [i for i in non_orderby_positions if _is_float_col(df.iloc[:, i])]
    tie_break_labels = df.columns[non_float_pos + float_pos].tolist()

    # Nothing to sort or canonicalize with — return as-is.
    if not tie_break_labels:
        return df

    if not sort_col_indices:
        # No ORDER BY: sort the entire frame by tie-breakers.
        return df.sort_values(by=tie_break_labels, na_position="last").reset_index(drop=True)

    # ORDER BY present: identify tie groups via consecutive-row equality, then
    # sort by [gid, tie-breakers] so engine between-group order is preserved
    # and within-group order is canonicalized by the tie-breakers.
    orderby_arr = df.iloc[:, list(sort_col_indices)].to_numpy()
    is_new_group = np.empty(len(df), dtype=bool)
    is_new_group[0] = True
    is_new_group[1:] = (orderby_arr[1:] != orderby_arr[:-1]).any(axis=1)
    gid = np.cumsum(is_new_group)

    gid_col = "__velox_orderby_tie_gid__"
    return (
        df.assign(**{gid_col: gid})
        .sort_values(by=[gid_col, *tie_break_labels], na_position="last")
        .drop(columns=gid_col)
        .reset_index(drop=True)
    )


def _column_mismatches(a_col: pd.Series, b_col: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """
    Return (bad_indices, null_mismatch_mask) for a single-column comparison.

    bad_indices: positions where the two columns disagree (after null and
        tolerance rules). null_mismatch_mask: boolean array, True where exactly
        one side is null.
    """
    a_null = pd.isna(a_col).to_numpy()
    b_null = pd.isna(b_col).to_numpy()
    null_mismatch = a_null ^ b_null  # exactly one side is null

    if _is_float_col(a_col):
        a = a_col.to_numpy(dtype=np.float64, na_value=np.nan)
        b = b_col.to_numpy(dtype=np.float64, na_value=np.nan)
        with np.errstate(invalid="ignore"):
            value_mismatch = np.abs(a - b) > ABS_TOL + REL_TOL * np.maximum(np.abs(a), np.abs(b))
        # NaN arithmetic returns NaN, and NaN > tol is False, so any cell
        # involving a null is automatically excluded from value_mismatch.
    else:
        a = a_col.to_numpy()
        b = b_col.to_numpy()
        # In object arrays NaN != NaN is True; explicitly mask out null cells.
        value_mismatch = (a != b) & ~(a_null | b_null)

    return np.flatnonzero(null_mismatch | value_mismatch), null_mismatch


def _format_mismatch(row_idx: int, col_idx: int, a_col: pd.Series, b_col: pd.Series, is_null_mismatch: bool) -> str:
    v1, v2 = a_col.iloc[row_idx], b_col.iloc[row_idx]
    if is_null_mismatch:
        return f"Row {row_idx}, col {col_idx}: {v1!r} vs {v2!r} (null mismatch)"
    if _is_float_col(a_col):
        fv1, fv2 = float(v1), float(v2)
        diff = abs(fv1 - fv2)
        tol = ABS_TOL + REL_TOL * max(abs(fv1), abs(fv2))
        return f"Row {row_idx}, col {col_idx}: {fv1} vs {fv2} (diff={diff:.2e}, tol={tol:.2e})"
    return f"Row {row_idx}, col {col_idx}: {v1!r} vs {v2!r}"


def _assert_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    """
    Column-by-column vectorized comparison. Float columns use rel_tol=1e-5,
    abs_tol=1e-8; non-float columns use exact equality. Both DataFrames must
    have matching shape and be pre-sorted / reset-indexed. Comparison is
    positional — column labels are not used.

    Null handling: both sides null → equal; exactly one side null → mismatch.
    Covers all three flavours of null TPC-DS can produce (float NaN, None,
    pd.NA) via pandas' uniform `pd.isna`.

    Raises AssertionError describing up to MAX_MISMATCHES differences.
    """
    if len(df1) != len(df2):
        raise AssertionError(f"Row count mismatch: {len(df1)} vs {len(df2)}")

    mismatches: list[str] = []
    for col_idx in range(df1.shape[1]):
        a_col = df1.iloc[:, col_idx]
        b_col = df2.iloc[:, col_idx]
        bad_idx, null_mismatch = _column_mismatches(a_col, b_col)
        for idx in bad_idx[: MAX_MISMATCHES - len(mismatches)]:
            i = int(idx)
            mismatches.append(_format_mismatch(i, col_idx, a_col, b_col, bool(null_mismatch[i])))
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

    All cross-frame access is positional, so column labels never need to
    align between actual and expected.

    Steps:
      1. Validate column count and capture expected column names.
      2. Normalize dtypes / value types.
      3. Validate row count.
      4. Parse ORDER BY / LIMIT from SQL.
      5. Preserve engine ORDER BY ordering; within tie groups (or the whole
         frame when no ORDER BY) canonicalize by [non-float, float] tie-breakers.
      6. Compare position-by-position, with LIMIT-boundary tie group's
         non-ORDER-BY columns skipped (engines may have selected different
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

    # 2. Coerce actual's dtypes/value types to match expected's
    actual = _normalize_to_expected(actual, expected)

    # 3. Row count
    if len(actual) != len(expected):
        raise AssertionError(f"Row count mismatch: {len(actual)} (actual) vs {len(expected)} (expected)")

    # 4. Parse ORDER BY / LIMIT
    sort_col_indices = get_orderby_col_indices(query_sql, expected_col_names)
    limit = get_limit(query_sql)

    # 5. Preserve engine ORDER BY ordering; canonicalize within tie groups.
    actual = _sort_preserving_orderby(actual, sort_col_indices)
    expected = _sort_preserving_orderby(expected, sort_col_indices)

    # 6. Compare. With LIMIT, the last tie group on ORDER BY columns may
    # contain different rows in actual vs expected (engines may have selected
    # different subsets of tied rows at the LIMIT cutoff). Skip the
    # non-ORDER-BY columns there; ORDER BY columns are still verified by
    # position-by-position compare.
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
