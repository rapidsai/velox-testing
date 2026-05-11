# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for result_comparison.py."""

import datetime

import numpy as np
import pandas as pd
import pytest

from common.testing.result_comparison import (
    _canonicalize_floats,
    _find_last_tie_start,
    _normalize_to_expected,
    _verify_sort,
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
# _canonicalize_floats
# ---------------------------------------------------------------------------


def test_canonicalize_collapses_engine_precision():
    # Two values differing at ~1e-12 relative — well below quantum (1e-8).
    df = pd.DataFrame({0: [100.000000000001, 100.000000000002]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] == result.iloc[1, 0]


def test_canonicalize_preserves_distinct_values():
    # Values 1e-3 apart at magnitude 100 — relative 1e-5, well above quantum.
    df = pd.DataFrame({0: [100.0, 100.001]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] != result.iloc[1, 0]


def test_canonicalize_preserves_nan():
    df = pd.DataFrame({0: [1.0, np.nan, 2.0]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] == 1.0
    assert pd.isna(result.iloc[1, 0])
    assert result.iloc[2, 0] == 2.0


def test_canonicalize_handles_zero():
    df = pd.DataFrame({0: [0.0, 1e-20, -0.0]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] == 0.0
    # 1e-20 is far smaller than any reasonable quantum; canonicalizes to 0 effectively
    assert result.iloc[2, 0] == 0.0


def test_canonicalize_handles_negatives():
    df = pd.DataFrame({0: [-100.000000000001, -100.000000000002]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] == result.iloc[1, 0]


def test_canonicalize_scales_with_magnitude():
    # Big magnitude: precision is relative, so 1e-3 wiggle on 1e7 gets flattened
    # (quantum at 1e7 is 1e7*1e-8 = 0.1).
    df = pd.DataFrame({0: [1e7, 1e7 + 1e-3]})
    result = _canonicalize_floats(df)
    assert result.iloc[0, 0] == result.iloc[1, 0]


def test_canonicalize_skips_non_float_columns():
    df = pd.DataFrame({0: ["a", "b"], 1: [1, 2], 2: [1.0, 2.0]})
    result = _canonicalize_floats(df)
    # Non-float columns untouched
    assert result.iloc[0, 0] == "a"
    assert result.iloc[0, 1] == 1


# ---------------------------------------------------------------------------
# _verify_sort
# ---------------------------------------------------------------------------


def test_verify_sort_ascending_correct():
    df = pd.DataFrame({0: [1, 2, 3]})
    _verify_sort(df, [0], [False])  # no exception


def test_verify_sort_descending_correct():
    df = pd.DataFrame({0: [3, 2, 1]})
    _verify_sort(df, [0], [True])


def test_verify_sort_wrong_direction_raises():
    df = pd.DataFrame({0: [1, 2, 3]})  # ASC data
    with pytest.raises(AssertionError, match="Sort violation"):
        _verify_sort(df, [0], [True])  # DESC expected


def test_verify_sort_no_orderby_passes():
    df = pd.DataFrame({0: [3, 1, 2]})
    _verify_sort(df, [], [])  # no ORDER BY → trivially passes


def test_verify_sort_multi_column_primary_distinct():
    # Primary is distinct → secondary's order is unrestricted per SQL semantics.
    df = pd.DataFrame({0: [1, 2, 3], 1: [99, 50, 75]})
    _verify_sort(df, [0, 1], [False, False])


def test_verify_sort_multi_column_primary_tied_secondary_correct():
    df = pd.DataFrame({0: [1, 1, 2], 1: [10, 20, 5]})
    _verify_sort(df, [0, 1], [False, False])


def test_verify_sort_multi_column_primary_tied_secondary_wrong_raises():
    df = pd.DataFrame({0: [1, 1], 1: [20, 10]})  # primary tied, secondary DESC
    with pytest.raises(AssertionError, match="Sort violation"):
        _verify_sort(df, [0, 1], [False, False])  # ASC, ASC expected


def test_verify_sort_float_within_tolerance_flip_allowed():
    # Two consecutive float values within tolerance — engine may have flipped
    # them due to precision; not a real sort violation.
    # 100.0001 > 100.0, but they're tolerance-equal at REL_TOL=1e-5*100=1e-3.
    df = pd.DataFrame({0: [100.0001, 100.0]})  # ASC expected, locally flipped
    _verify_sort(df, [0], [False])


def test_verify_sort_float_outside_tolerance_raises():
    df = pd.DataFrame({0: [200.0, 100.0]})  # ASC expected; 200 > 100 by ~100
    with pytest.raises(AssertionError, match="Sort violation"):
        _verify_sort(df, [0], [False])


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
