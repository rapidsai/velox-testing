# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pandas as pd
import pytest

from common.testing.validate_results import validate
from presto.testing.performance_benchmarks.common_fixtures import (
    _drop_results_schema,
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
        "SELECT 1"
    )


def test_validator_reads_distributed_parquet_dataset(tmp_path: Path):
    results_dir = tmp_path / "actual"
    query_dir = results_dir / "q1"
    query_dir.mkdir(parents=True)
    # Distributed writer files do not preserve the query's global row order.
    pd.DataFrame({"value": [3, 4]}).to_parquet(query_dir / "part-00000.parquet", index=False)
    pd.DataFrame({"value": [1, 2]}).to_parquet(query_dir / "part-00001.parquet", index=False)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"value": [1, 2, 3, 4]}).to_parquet(expected_dir / "q01.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q1": "SELECT value FROM source ORDER BY value"})

    assert result["overall_status"] == "passed"
    assert result["queries"]["q1"]["status"] == "passed"


def test_validator_keeps_single_file_result_order_sensitive(tmp_path: Path):
    results_dir = tmp_path / "actual"
    results_dir.mkdir()
    pd.DataFrame({"value": [2, 1]}).to_parquet(results_dir / "q1.parquet", index=False)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    pd.DataFrame({"value": [1, 2]}).to_parquet(expected_dir / "q01.parquet", index=False)

    result = validate(results_dir, expected_dir, {"Q1": "SELECT value FROM source ORDER BY value"})

    assert result["overall_status"] == "failed"
    assert "Engine violated ORDER BY" in result["queries"]["q1"]["message"]


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
