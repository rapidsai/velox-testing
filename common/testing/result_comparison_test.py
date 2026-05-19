# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for result_comparison.py."""

import datetime

import numpy as np
import pandas as pd
import pytest

from common.testing.result_comparison import (
    _canonical_sort,
    _find_last_tie_start,
    _normalize_to_expected,
    _validate_orderby,
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
# _validate_orderby
# ---------------------------------------------------------------------------


def test_validate_orderby_no_orderby_columns_is_noop():
    # Empty sort_col_indices: validation is a no-op regardless of frame content.
    df = pd.DataFrame({0: [3, 1, 2], 1: ["c", "a", "b"]})
    _validate_orderby(df, sort_col_indices=[], ascending=[])


def test_validate_orderby_single_column_ascending_correct():
    df = pd.DataFrame({0: [1, 2, 2, 3], 1: ["a", "b", "c", "d"]})
    _validate_orderby(df, sort_col_indices=[0], ascending=[True])


def test_validate_orderby_single_column_descending_correct():
    df = pd.DataFrame({0: [3, 2, 2, 1], 1: ["a", "b", "c", "d"]})
    _validate_orderby(df, sort_col_indices=[0], ascending=[False])


def test_validate_orderby_wrong_direction_raises():
    # Frame is ASC, but spec says DESC → fails.
    df = pd.DataFrame({0: [1, 2, 3]})
    with pytest.raises(AssertionError, match="ORDER BY"):
        _validate_orderby(df, sort_col_indices=[0], ascending=[False])


def test_validate_orderby_multi_column_secondary_within_tie_correct_passes():
    # ORDER BY col 0 ASC, col 1 ASC. Primary tied at 1; secondary correctly
    # ordered within each tie group.
    df = pd.DataFrame({0: [1, 1, 2, 2], 1: ["a", "b", "x", "y"]})
    _validate_orderby(df, sort_col_indices=[0, 1], ascending=[True, True])


def test_validate_orderby_multi_column_secondary_within_tie_violated_raises():
    # Primary tied at 1 but secondary is in wrong order within the tie group.
    df = pd.DataFrame({0: [1, 1, 2, 2], 1: ["b", "a", "x", "y"]})
    with pytest.raises(AssertionError, match="ORDER BY"):
        _validate_orderby(df, sort_col_indices=[0, 1], ascending=[True, True])


def test_validate_orderby_handles_duplicate_column_labels():
    # df with two columns both labeled "x" — validate_orderby should still work
    # by accessing columns positionally.
    df = pd.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["x", "x"])
    _validate_orderby(df, sort_col_indices=[0], ascending=[True])


# ---------------------------------------------------------------------------
# _canonical_sort
# ---------------------------------------------------------------------------


def test_canonical_sort_non_float_before_float():
    # Non-float col uniquely identifies rows; float ordering is irrelevant.
    df = pd.DataFrame({0: [3.0, 1.0, 2.0], 1: ["c", "a", "b"]})
    result = _canonical_sort(df)
    assert result[1].tolist() == ["a", "b", "c"]
    assert result[0].tolist() == [1.0, 2.0, 3.0]


def test_canonical_sort_all_float_columns():
    # No non-float columns; sort by float columns left-to-right.
    df = pd.DataFrame({0: [2.0, 1.0, 1.0], 1: [5.0, 3.0, 1.0]})
    result = _canonical_sort(df)
    assert result[0].tolist() == [1.0, 1.0, 2.0]
    assert result[1].tolist() == [1.0, 3.0, 5.0]


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
