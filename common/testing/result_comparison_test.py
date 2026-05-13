# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for result_comparison.py."""

import datetime

import numpy as np
import pandas as pd

from common.testing.result_comparison import (
    _find_last_tie_start,
    _normalize_to_expected,
    _sort_preserving_orderby,
)

# ---------------------------------------------------------------------------
# _normalize_to_expected
# ---------------------------------------------------------------------------


def test_normalize_to_expected_converts_str_dates_to_datetime64():
    """
    When expected has datetime64 and actual has str dates,
    _normalize_to_expected converts actual to datetime64.
    """
    actual = pd.DataFrame({0: ["1995-03-05", "1992-01-01", "1993-06-15"]})
    expected = pd.DataFrame({0: pd.to_datetime(["1995-03-05", "1992-01-01", "1993-06-15"])})

    assert actual[0].dtype == object
    result = _normalize_to_expected(actual, expected)
    assert np.issubdtype(result[0].dtype, np.datetime64)
    assert (result[0] == expected[0]).all()


def test_normalize_to_expected_handles_str_to_date_in_object_dtype():
    """
    Both columns have object dtype but actual holds str dates and expected
    holds datetime.date objects (the parquet round-trip case where the two
    engines' parquet writers produce different DATE encodings).
    _normalize_to_expected converts actual's str values to datetime.date.
    """
    actual = pd.DataFrame({0: ["1995-03-05", "1992-01-01"]})
    expected = pd.DataFrame({0: [datetime.date(1995, 3, 5), datetime.date(1992, 1, 1)]})

    assert actual[0].dtype == object
    assert expected[0].dtype == object

    result = _normalize_to_expected(actual, expected)
    assert result[0].dtype == object
    assert isinstance(result[0].iloc[0], datetime.date)
    assert (result[0] == expected[0]).all()


# ---------------------------------------------------------------------------
# _sort_preserving_orderby
# ---------------------------------------------------------------------------


def test_sort_preserving_orderby_no_orderby_sorts_by_non_float_then_float():
    # No ORDER BY: sort entire frame by non-float first, float second.
    # Both rows tie on col 1 (string "a"), so col 0 (int) determines order.
    df = pd.DataFrame({0: [3.0, 1.0, 2.0], 1: ["c", "a", "b"]})
    result = _sort_preserving_orderby(df, sort_col_indices=[])
    # After sort by [col 1 (string, non-float), col 0 (float)]: "a","b","c"
    assert result[1].tolist() == ["a", "b", "c"]
    assert result[0].tolist() == [1.0, 2.0, 3.0]


def test_sort_preserving_orderby_preserves_engine_between_group_order():
    # ORDER BY col 0 (revenue DESC) — engine produced [3, 2, 1].
    # Each row is its own tie group; preserve engine ordering.
    df = pd.DataFrame({0: [3, 2, 1], 1: ["x", "y", "z"]})
    result = _sort_preserving_orderby(df, sort_col_indices=[0])
    assert result[0].tolist() == [3, 2, 1]
    assert result[1].tolist() == ["x", "y", "z"]


def test_sort_preserving_orderby_canonicalizes_within_tie():
    # ORDER BY col 0; rows 0 and 1 tied at 100. Engine put them in (B, A) order.
    # Within tie, sort by col 1 (string) → (A, B).
    df = pd.DataFrame({0: [100, 100, 200], 1: ["B", "A", "C"]})
    result = _sort_preserving_orderby(df, sort_col_indices=[0])
    assert result[0].tolist() == [100, 100, 200]
    assert result[1].tolist() == ["A", "B", "C"]


def test_sort_preserving_orderby_non_float_dominates_float():
    # ORDER BY col 0; rows tied at 100. Within tie, sort first by col 1 (str),
    # then by col 2 (float). Non-float key alone uniquely identifies the rows,
    # so the float ordering is never consulted.
    df = pd.DataFrame({0: [100, 100], 1: ["B", "A"], 2: [1.0, 2.0]})
    result = _sort_preserving_orderby(df, sort_col_indices=[0])
    # Sorted by col 1 ASC: "A" before "B"
    assert result[1].tolist() == ["A", "B"]
    assert result[2].tolist() == [2.0, 1.0]  # the float "rode along" with its row


def test_sort_preserving_orderby_tolerance_tied_floats_grouped():
    # ORDER BY col 0 (float). Two adjacent rows whose values are ULP-different
    # but tolerance-equal should be grouped and reordered by col 1 (non-float
    # tie-breaker). Without tolerance-aware tie detection, the engine-presented
    # order would be preserved (which is the Q11-at-SF=3k failure mode).
    df = pd.DataFrame({0: [100.0 + 1e-9, 100.0], 1: ["B", "A"]})
    result = _sort_preserving_orderby(df, sort_col_indices=[0])
    # Both values are tolerance-tied (1e-9 << REL_TOL * 100 = 1e-3); the rows
    # share a gid and are canonicalized by col 1: "A" before "B".
    assert result[1].tolist() == ["A", "B"]


# ---------------------------------------------------------------------------
# _find_last_tie_start
# ---------------------------------------------------------------------------


def test_find_last_tie_start_empty_frame():
    assert _find_last_tie_start(pd.DataFrame({0: []})) == 0


def test_find_last_tie_start_all_distinct():
    # Last row is unique → last tie block is just the last row.
    df = pd.DataFrame({0: [1, 2, 3, 4]})
    assert _find_last_tie_start(df) == 3


def test_find_last_tie_start_all_tied():
    df = pd.DataFrame({0: [5, 5, 5]})
    assert _find_last_tie_start(df) == 0


def test_find_last_tie_start_mixed():
    # Last three rows tied at 3.
    df = pd.DataFrame({0: [1, 1, 2, 3, 3, 3]})
    assert _find_last_tie_start(df) == 3


def test_find_last_tie_start_multi_column():
    # Tie requires all ORDER BY columns to match.
    df = pd.DataFrame({0: [1, 1, 2, 2], 1: [10, 20, 30, 30]})
    assert _find_last_tie_start(df) == 2
