# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path

import pandas as pd
import pytest

from presto.testing.performance_benchmarks.ctas import (
    CTAS_SCHEMA,
    build_ctas_query,
    ctas_output_column_aliases,
    ctas_table_name,
    drop_results_schema,
    finalize_ctas_results,
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
    assert CTAS_SCHEMA == "benchmark_results"
    assert ctas_table_name("Q7", 0) == "q7"
    assert ctas_table_name("Q7", 2) == "q7_iteration_3"
    assert strip_trailing_semicolon("SELECT 1;\n") == "SELECT 1"
    assert build_ctas_query("tpch", "Q7", "SELECT 1;", "hive_output", "results", "q7") == (
        "--tpch_Q7--\nCREATE TABLE hive_output.results.q7 (c1) WITH (format = 'PARQUET') AS\nSELECT 1"
    )


def test_ctas_uses_positional_aliases_for_explicit_output_expressions():
    query = "SELECT c_name, sum(l_quantity), c_name, count(*) AS row_count FROM customer, lineitem GROUP BY c_name"

    assert ctas_output_column_aliases(query) == ["c1", "c2", "c3", "c4"]
    assert build_ctas_query("tpch", "Q18", query, "hive_output", "results", "q18").endswith(
        "(c1, c2, c3, c4) WITH (format = 'PARQUET') AS\n" + query
    )


def test_ctas_alias_list_leaves_stars_for_presto_to_expand():
    query = "SELECT nested.* FROM (SELECT 1 AS value) nested"

    assert ctas_output_column_aliases(query) is None
    assert build_ctas_query("tpcds", "Q88", query, "hive_output", "results", "q88") == (
        "--tpcds_Q88--\nCREATE TABLE hive_output.results.q88 WITH (format = 'PARQUET') AS\n" + query
    )


@pytest.mark.parametrize("benchmark_type", ["tpch", "tpcds"])
def test_ctas_alias_list_parses_all_builtin_queries(benchmark_type):
    repo_root = Path(__file__).parents[2]
    query_path = repo_root / "common" / "testing" / "queries" / benchmark_type / "queries.json"
    queries = json.loads(query_path.read_text())

    aliases = {
        query_id: ctas_output_column_aliases(query.replace("{SF_FRACTION}", "0.0001"))
        for query_id, query in queries.items()
    }

    assert aliases.keys() == queries.keys()
    if benchmark_type == "tpch":
        assert aliases["Q18"] == ["c1", "c2", "c3", "c4", "c5", "c6"]


def test_drop_results_schema_removes_managed_tables_first():
    cursor = FakeCursor(
        {
            "SHOW SCHEMAS FROM hive_output": [("benchmark_results",)],
            "SHOW TABLES FROM hive_output.benchmark_results": [("q1",), ("q2_iteration_2",)],
        }
    )

    drop_results_schema(cursor, "hive_output", "benchmark_results")

    assert cursor.statements == [
        "SHOW SCHEMAS FROM hive_output",
        "SHOW TABLES FROM hive_output.benchmark_results",
        'DROP TABLE IF EXISTS hive_output.benchmark_results."q1"',
        'DROP TABLE IF EXISTS hive_output.benchmark_results."q2_iteration_2"',
        "DROP SCHEMA hive_output.benchmark_results",
    ]


def test_finalize_ctas_results_prepares_and_normalizes_output(tmp_path):
    source_dir = tmp_path / "benchmark_results"
    query_dir = source_dir / "q1"
    query_dir.mkdir(parents=True)
    (query_dir / ".prestoSchema").write_text("{}")
    pd.DataFrame({"c1": [2, 1]}).to_parquet(query_dir / "part-00000.parquet", index=False)

    output_dir = tmp_path / "query_results"
    outputs = finalize_ctas_results(source_dir, output_dir, {"Q1": "SELECT value FROM source ORDER BY value"})

    assert outputs == [output_dir / "q1.parquet"]
    assert pd.read_parquet(outputs[0])["c1"].tolist() == [1, 2]
    assert not query_dir.exists()
