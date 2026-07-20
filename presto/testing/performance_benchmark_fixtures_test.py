# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path

import pytest

from presto.testing.performance_benchmarks.common_fixtures import (
    _drop_results_schema,
    alias_ctas_output_expressions,
    build_ctas_query,
    ctas_schema_name,
    ctas_table_name,
    strip_trailing_semicolon,
)


class FakeCursor:
    def __init__(self, responses):
        self.responses = responses
        self.statements = []
        self.current = []

    def execute(self, statement):
        self.statements.append(statement)
        self.current = self.responses.get(statement, [])
        return self

    def fetchall(self):
        return self.current


def test_ctas_names_and_sql():
    assert ctas_schema_name(None) == "benchmark_results"
    assert ctas_schema_name("GPU_SF100") == "benchmark_results_gpu_sf100"
    with pytest.raises(ValueError, match="alphanumeric"):
        ctas_schema_name("../other")
    assert ctas_table_name("Q7", 0) == "q7"
    assert ctas_table_name("Q7", 2) == "q7_iteration_3"
    assert strip_trailing_semicolon("SELECT 1;\n") == "SELECT 1"
    assert build_ctas_query("tpch", "Q7", "SELECT 1;", "hive_output", "results", "q7") == (
        "--tpch_Q7--\n"
        "CREATE TABLE hive_output.results.q7 WITH (format = 'PARQUET') AS\n"
        "SELECT 1 AS result_column_1"
    )


def test_ctas_aliases_unnamed_and_duplicate_output_expressions():
    query = (
        "SELECT c_name, sum(l_quantity), c_name, count(*) AS row_count "
        "FROM customer, lineitem GROUP BY c_name"
    )

    rewritten = alias_ctas_output_expressions(query)

    assert rewritten == (
        "SELECT c_name, SUM(l_quantity) AS result_column_2, "
        "c_name AS result_column_3, COUNT(*) AS row_count "
        "FROM customer, lineitem GROUP BY c_name"
    )


def test_ctas_alias_rewrite_leaves_stars_for_presto_to_expand():
    assert alias_ctas_output_expressions("SELECT nested.* FROM (SELECT 1 AS value) nested") == (
        "SELECT nested.* FROM (SELECT 1 AS value) nested"
    )


@pytest.mark.parametrize("benchmark_type", ["tpch", "tpcds"])
def test_ctas_alias_rewrite_parses_all_builtin_queries(benchmark_type):
    repo_root = Path(__file__).parents[2]
    query_path = repo_root / "common" / "testing" / "queries" / benchmark_type / "queries.json"
    queries = json.loads(query_path.read_text())

    rewritten = {
        query_id: alias_ctas_output_expressions(query.replace("{SF_FRACTION}", "0.0001"))
        for query_id, query in queries.items()
    }

    assert rewritten.keys() == queries.keys()
    if benchmark_type == "tpch":
        assert "SUM(l_quantity) AS result_column_6" in rewritten["Q18"]


def test_drop_results_schema_removes_managed_tables_first():
    cursor = FakeCursor(
        {
            "SHOW SCHEMAS FROM hive_output": [("benchmark_results",)],
            "SHOW TABLES FROM hive_output.benchmark_results": [("q1",), ("q2_iteration_2",)],
        }
    )

    _drop_results_schema(cursor, "hive_output", "benchmark_results")

    assert cursor.statements == [
        "SHOW SCHEMAS FROM hive_output",
        "SHOW TABLES FROM hive_output.benchmark_results",
        'DROP TABLE IF EXISTS hive_output.benchmark_results."q1"',
        'DROP TABLE IF EXISTS hive_output.benchmark_results."q2_iteration_2"',
        "DROP SCHEMA hive_output.benchmark_results",
    ]
