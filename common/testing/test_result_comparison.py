# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for result_comparison.py — focusing on the Presto str-date normalization issue.

Presto's prestodb driver returns DATE columns as plain Python str (e.g. '1995-03-05').
The driver's process() does rows=response.get("data", []) with no type conversion,
so JSON string values pass through unchanged. _normalize_to_expected coerces actual's
str-date column to datetime64 by reading the target dtype from the expected frame.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import prestodb.dbapi

from common.testing.result_comparison import (
    _identify_tie_groups,
    _normalize_to_expected,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_NEXT_URI = "http://localhost:8080/v1/statement/test_query_id/1"

_BASE_STATS = {
    "state": "RUNNING",
    "scheduled": True,
    "nodes": 1,
    "totalSplits": 1,
    "queuedSplits": 0,
    "runningSplits": 1,
    "completedSplits": 0,
    "userTimeMillis": 0,
    "cpuTimeMillis": 0,
    "wallTimeMillis": 0,
    "processedRows": 0,
    "processedBytes": 0,
}


def _fake_http_response(json_body: dict, is_redirect: bool = False) -> MagicMock:
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.is_redirect = is_redirect
    resp.encoding = "utf-8"
    resp.headers = {}
    resp.json.return_value = json_body
    return resp


def _make_presto_cursor_with_date_response(rows: list[list], col_name: str = "ship_date"):
    """
    Return a prestodb cursor that replays a two-page Presto JSON exchange:
      POST → nextUri only (no data, no columns — matches real Presto behaviour)
      GET  → columns + data, no nextUri

    prestodb sets cursor._query._columns only inside fetch() (the GET page), so
    cursor.description is only populated after the first GET response. This helper
    mimics that real exchange so tests can use cursor.description normally.
    """
    post_response = _fake_http_response(
        {
            "id": "test_query_id",
            "infoUri": "http://localhost:8080/v1/query/test_query_id",
            "nextUri": _NEXT_URI,
            "stats": _BASE_STATS,
        }
    )
    get_response = _fake_http_response(
        {
            "id": "test_query_id",
            "infoUri": "http://localhost:8080/v1/query/test_query_id",
            "stats": {**_BASE_STATS, "state": "FINISHED", "completedSplits": 1, "processedRows": len(rows)},
            "columns": [{"name": col_name, "type": "date"}],
            "data": rows,
        }
    )

    conn = prestodb.dbapi.connect(host="localhost", port=8080, user="test")
    cursor = conn.cursor()
    cursor._request._post = MagicMock(return_value=post_response)
    cursor._request._get = MagicMock(return_value=get_response)
    return cursor


# ---------------------------------------------------------------------------
# prestodb driver returns str for DATE columns
# ---------------------------------------------------------------------------


def test_prestodb_date_column_type_is_str():
    """prestodb returns DATE values as plain str — no type conversion in the driver."""
    cursor = _make_presto_cursor_with_date_response(rows=[["1995-03-05"], ["1992-01-01"], ["1993-06-15"]])
    cursor.execute("SELECT ship_date FROM lineitem LIMIT 3")
    rows = cursor.fetchall()

    date_values = [row[0] for row in rows]
    for v in date_values:
        assert isinstance(v, str), f"Expected str, got {type(v).__name__}: {v!r}"


def test_prestodb_date_column_in_dataframe_has_object_dtype():
    """DataFrame built from prestodb rows has object dtype for DATE columns."""
    cursor = _make_presto_cursor_with_date_response(rows=[["1995-03-05"], ["1992-01-01"], ["1993-06-15"]])
    cursor.execute("SELECT ship_date FROM lineitem LIMIT 3")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=columns)
    assert df["ship_date"].dtype == object
    assert isinstance(df["ship_date"].iloc[0], str)


# ---------------------------------------------------------------------------
# _normalize_to_expected coerces actual's dtypes to match expected
# ---------------------------------------------------------------------------


def test_normalize_to_expected_converts_presto_str_dates_to_datetime64():
    """
    When expected has datetime64 and actual has str dates (the Presto case),
    _normalize_to_expected converts actual to datetime64.
    """
    cursor = _make_presto_cursor_with_date_response(rows=[["1995-03-05"], ["1992-01-01"], ["1993-06-15"]])
    cursor.execute("SELECT ship_date FROM lineitem LIMIT 3")
    rows = cursor.fetchall()
    actual = pd.DataFrame({0: [r[0] for r in rows]})
    expected = pd.DataFrame({0: pd.to_datetime(["1995-03-05", "1992-01-01", "1993-06-15"])})

    assert actual[0].dtype == object
    result = _normalize_to_expected(actual, expected)
    assert np.issubdtype(result[0].dtype, np.datetime64)
    assert (result[0] == expected[0]).all()


# ---------------------------------------------------------------------------
# _identify_tie_groups
# ---------------------------------------------------------------------------


def test_identify_tie_groups_empty_frame():
    assert _identify_tie_groups(pd.DataFrame({0: []})) == []


def test_identify_tie_groups_no_ties():
    df = pd.DataFrame({0: [1, 2, 3, 4]})
    assert _identify_tie_groups(df) == [(0, 1), (1, 2), (2, 3), (3, 4)]


def test_identify_tie_groups_all_tied():
    df = pd.DataFrame({0: [5, 5, 5]})
    assert _identify_tie_groups(df) == [(0, 3)]


def test_identify_tie_groups_mixed():
    # Single column: 1, 1, 2, 3, 3, 3
    df = pd.DataFrame({0: [1, 1, 2, 3, 3, 3]})
    assert _identify_tie_groups(df) == [(0, 2), (2, 3), (3, 6)]


def test_identify_tie_groups_multi_column():
    # Tie requires all ORDER BY columns to match.
    df = pd.DataFrame({0: [1, 1, 1, 2, 2], 1: [10, 10, 20, 30, 30]})
    assert _identify_tie_groups(df) == [(0, 2), (2, 3), (3, 5)]
