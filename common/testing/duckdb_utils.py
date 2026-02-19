# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""DuckDB utilities for reference result generation."""

import duckdb


def create_duckdb_table(table_name, data_path):
    """Create a DuckDB table from parquet files."""
    duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")
    duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{data_path}/*.parquet';")


def get_reference_results(query, reference_results_dir=None, result_file_name=None):
    """
    Get reference results either from a pre-computed parquet file or by executing the query in DuckDB.

    Args:
        query: The SQL query to execute
        reference_results_dir: Optional path to pre-computed reference results
        result_file_name: The filename for the result (e.g., 'q1.parquet')

    Returns:
        DuckDB relation with the results
    """
    if reference_results_dir and result_file_name:
        return duckdb.from_parquet(f"{reference_results_dir}/{result_file_name}")
    else:
        return duckdb.sql(query)
