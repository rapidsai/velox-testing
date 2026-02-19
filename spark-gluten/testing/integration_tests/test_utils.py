# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import sys


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


sys.path.append(get_abs_file_path("../../.."))

import duckdb  # noqa: E402

from common.testing.comparison import compare_results, normalize_spark_rows  # noqa: E402
from common.testing.duckdb_utils import create_duckdb_table  # noqa: E402
from common.testing.preview import show_result_preview, write_rows_to_parquet  # noqa: E402


def execute_query_and_compare_results(request_config, spark_session, queries, query_id):
    query = queries[query_id]

    # Execute query in Spark
    spark_df = spark_session.sql(query)
    spark_rows = [tuple(row) for row in spark_df.collect()]
    spark_columns = spark_df.columns

    preview_rows_count = request_config.getoption("--preview-rows-count")
    if request_config.getoption("--show-spark-result-preview"):
        show_result_preview(spark_columns, spark_rows, preview_rows_count, "Spark", query_id)

    output_dir = request_config.getoption("--output-dir")
    result_file_name = f"{query_id.lower()}.parquet"
    if request_config.getoption("--store-spark-results"):
        write_rows_to_parquet(output_dir, "spark_results", result_file_name, spark_rows, spark_columns)

    reference_results_dir = request_config.getoption("--reference-results-dir")
    if reference_results_dir:
        duckdb_relation = duckdb.from_parquet(f"{reference_results_dir}/{result_file_name}")
    else:
        duckdb_relation = duckdb.sql(query)

    if request_config.getoption("--store-reference-results"):
        duckdb_relation.write_parquet(f"{output_dir}/reference_results/{result_file_name}")

    duckdb_rows = duckdb_relation.fetchall()
    if request_config.getoption("--show-reference-result-preview"):
        show_result_preview(duckdb_relation.columns, duckdb_rows, preview_rows_count, "Reference", query_id)

    if not request_config.getoption("--skip-reference-comparison"):
        compare_results(
            spark_rows,
            duckdb_rows,
            duckdb_relation.types,
            query,
            duckdb_relation.columns,
            normalize_test_rows_fn=normalize_spark_rows,
        )


# Re-export for backwards compatibility
__all__ = ["execute_query_and_compare_results", "create_duckdb_table", "get_abs_file_path"]
