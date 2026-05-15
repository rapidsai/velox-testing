# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import shutil
import sys
from pathlib import Path

import pandas as pd

from ..result_comparison import compare_result_frames
from ..test_utils import get_abs_file_path

sys.path.append(get_abs_file_path(__file__, "../../../benchmark_data_tools"))

import duckdb
from duckdb_utils import create_table


def execute_query_and_compare_results(
    request_config, queries, query_id, query_engine, query_engine_rows, query_engine_columns
):
    query = queries[query_id]

    preview_rows_count = request_config.getoption("--preview-rows-count")
    if request_config.getoption(f"--show-{query_engine}-result-preview"):
        show_result_preview(query_engine_columns, query_engine_rows, preview_rows_count, query_engine, query_id)

    output_dir = request_config.getoption("--output-dir")
    result_file_name = f"{query_id.lower()}.parquet"
    if request_config.getoption(f"--store-{query_engine}-results"):
        write_query_engine_rows(output_dir, result_file_name, query_engine_rows, query_engine_columns, query_engine)

    reference_results_dir = request_config.getoption("--reference-results-dir")
    if reference_results_dir:
        duckdb_relation = duckdb.from_parquet(f"{reference_results_dir}/{result_file_name}")
    else:
        duckdb_relation = duckdb.sql(query)

    if request_config.getoption("--store-reference-results"):
        duckdb_relation.write_parquet(f"{output_dir}/reference_results/{result_file_name}")

    if request_config.getoption("--show-reference-result-preview"):
        duckdb_rows = duckdb_relation.fetchall()
        show_result_preview(duckdb_relation.columns, duckdb_rows, preview_rows_count, "Reference", query_id)

    if not request_config.getoption("--skip-reference-comparison"):
        engine_df = pd.DataFrame(query_engine_rows, columns=query_engine_columns)
        duckdb_df = duckdb_relation.df()
        compare_result_frames(engine_df, duckdb_df, query)


def show_result_preview(columns, rows, preview_rows_count, result_source, query_id):
    start_line = f"\n{'-' * 50} {result_source} {query_id} Result Preview {'-' * 50}"
    print(start_line)
    preview_rows_count = min(preview_rows_count, len(rows))
    print(f"Showing {preview_rows_count} of {len(rows)} rows...\n")
    df = pd.DataFrame(rows[:preview_rows_count], columns=columns)
    print(df)
    print("-" * len(start_line))


def write_query_engine_rows(output_dir, result_file_name, rows, columns, query_engine):
    df = pd.DataFrame(rows, columns=columns)
    df.to_parquet(f"{output_dir}/{query_engine}_results/{result_file_name}")


def create_duckdb_table(table_name, data_path):
    create_table(table_name, get_abs_file_path(__file__, data_path))


def initialize_output_dir(config, query_engine):
    output_dir = Path(config.getoption("--output-dir"))
    user_reference_results_dir = config.getoption("--reference-results-dir")
    output_dir.mkdir(parents=True, exist_ok=True)

    if config.getoption(f"--store-{query_engine}-results"):
        query_engine_results_dir = Path(f"{output_dir}/{query_engine}_results")
        if query_engine_results_dir.exists():
            shutil.rmtree(query_engine_results_dir)
        query_engine_results_dir.mkdir(exist_ok=False)

    if config.getoption("--store-reference-results"):
        # Only manage the reference results directory if it's not being overridden by the user
        reference_results_dir = Path(f"{output_dir}/reference_results")
        if reference_results_dir.exists():
            if not user_reference_results_dir or Path(user_reference_results_dir) != reference_results_dir:
                shutil.rmtree(reference_results_dir)
            else:
                raise Exception(
                    "Reference results directory and store-reference-results should not be set at the same time"
                )
        reference_results_dir.mkdir(exist_ok=False)
