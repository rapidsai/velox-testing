# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the query ID parametrization of the custom benchmark type in conftest.py."""

import json

import pytest

from common.testing.conftest import _get_custom_query_ids, pytest_generate_tests

# Deliberately not in sorted order and not of the form Q<number>.
QUERIES = {"G00002": "SELECT 2", "T01": "SELECT 1", "G00001": "SELECT '{x}'", "S7": "SELECT 7", "q-5": "SELECT 5"}


class _Config:
    def __init__(self, options):
        self._options = options

    def getoption(self, name, default=None):
        return self._options.get(name, default)


class _Metafunc:
    def __init__(self, fixturename, options):
        self.fixturenames = ["benchmark_query", fixturename]
        self.config = _Config(options)
        self.parametrized = {}

    def parametrize(self, name, values):
        self.parametrized[name] = values


def _write_queries_file(tmp_path, content):
    path = tmp_path / "queries.json"
    path.write_text(json.dumps(content))
    return str(path)


@pytest.fixture
def queries_file(tmp_path):
    return _write_queries_file(tmp_path, QUERIES)


@pytest.mark.parametrize("selected", [None, "", "  "])
def test_all_keys_in_file_order_by_default(queries_file, selected):
    assert _get_custom_query_ids(queries_file, selected) == list(QUERIES)


def test_selected_keys_in_selection_order(queries_file):
    assert _get_custom_query_ids(queries_file, "T01, q-5,G00002") == ["T01", "q-5", "G00002"]


def test_unknown_selected_keys_are_rejected(queries_file):
    """Keys are matched exactly: no Q<number> shorthand and no case folding."""
    with pytest.raises(ValueError, match=r"Query IDs not found in .*queries\.json: 7, t01$"):
        _get_custom_query_ids(queries_file, "T01,7,t01")


@pytest.mark.parametrize("missing", [None, ""])
def test_queries_file_is_required(missing):
    with pytest.raises(ValueError, match="--queries-file is required"):
        _get_custom_query_ids(missing, None)


@pytest.mark.parametrize("content", [{}, ["SELECT 1", "SELECT 2"], "SELECT 1", None])
def test_queries_file_must_hold_a_non_empty_object(tmp_path, content):
    with pytest.raises(ValueError, match=r"queries\.json must contain a non-empty JSON object"):
        _get_custom_query_ids(_write_queries_file(tmp_path, content), None)


def test_ids_without_a_result_file_of_their_own_are_rejected(tmp_path):
    """Results are written to query_results/<lower-case query ID>.parquet, whatever the selection is."""
    queries = {"dup": "SELECT 1", "ok": "SELECT 2", "DUP": "SELECT 3", "x/y": "SELECT 4"}
    with pytest.raises(ValueError, match=r"must be unique ignoring case and must not contain '/': dup, DUP, x/y$"):
        _get_custom_query_ids(_write_queries_file(tmp_path, queries), "ok")


def test_generate_tests_parametrizes_custom_query_id(queries_file):
    metafunc = _Metafunc("custom_query_id", {"--queries-file": queries_file, "--queries": "S7,T01"})
    pytest_generate_tests(metafunc)
    assert metafunc.parametrized == {"custom_query_id": ["S7", "T01"]}


def test_generate_tests_requires_queries_file_when_option_is_not_registered():
    """Engines that do not register --queries-file (spark_gluten) get the same error."""
    with pytest.raises(ValueError, match="--queries-file is required"):
        pytest_generate_tests(_Metafunc("custom_query_id", {}))


@pytest.mark.parametrize("benchmark_type, num_queries", [("tpch", 22), ("tpcds", 99)])
def test_generate_tests_keeps_tpc_query_ids(queries_file, benchmark_type, num_queries):
    """A queries file never changes the IDs of the TPC benchmark types."""
    metafunc = _Metafunc(f"{benchmark_type}_query_id", {"--queries-file": queries_file})
    pytest_generate_tests(metafunc)
    assert metafunc.parametrized == {f"{benchmark_type}_query_id": [f"Q{i}" for i in range(1, num_queries + 1)]}
