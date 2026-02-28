# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import shutil
import sys
from pathlib import Path

import pandas as pd

from ..test_utils import get_abs_file_path

sys.path.append(get_abs_file_path(__file__, "../../../benchmark_data_tools"))

import duckdb
import sqlglot
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

    duckdb_rows = duckdb_relation.fetchall()
    if request_config.getoption("--show-reference-result-preview"):
        show_result_preview(duckdb_relation.columns, duckdb_rows, preview_rows_count, "Reference", query_id)

    if not request_config.getoption("--skip-reference-comparison"):
        compare_results(query_engine_rows, duckdb_rows, duckdb_relation.types, query, duckdb_relation.columns)


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


def get_is_sorted_query(query):
    return any(isinstance(expr, sqlglot.exp.Order) for expr in sqlglot.parse_one(query).iter_expressions())


def none_safe_sort_key(row):
    """Sort key that treats None as less than any other value."""
    return tuple((0, x) if x is not None else (1, None) for x in row)


def compare_results(query_engine_rows, duckdb_rows, types, query, column_names):
    row_count = len(query_engine_rows)
    assert row_count == len(duckdb_rows)

    duckdb_rows = normalize_rows(duckdb_rows, types)
    query_engine_rows = normalize_rows(query_engine_rows, types)

    # We need a full sort for all non-ORDER BY columns because some ORDER BY comparison
    # will be equal and the resulting order of non-ORDER BY columns will be ambiguous.
    sorted_duckdb_rows = sorted(duckdb_rows, key=none_safe_sort_key)
    sorted_query_engine_rows = sorted(query_engine_rows, key=none_safe_sort_key)
    assert_rows_equal(sorted_query_engine_rows, sorted_duckdb_rows, types)

    # If we have an ORDER BY clause we want to test that the resulting order of those
    # columns is correct, in addition to overall values being correct.
    # However, we can only validate the ORDER BY if we can extract column indices.
    # For complex ORDER BY expressions (aggregates, CASE statements, etc.), we skip
    # the ORDER BY validation but still validate overall result correctness.
    order_indices = get_orderby_indices(query, column_names)
    if order_indices:
        # Project both results to ORDER BY columns and compare in original order
        duckdb_proj = [[row[i] for i in order_indices] for row in duckdb_rows]
        query_engine_proj = [[row[i] for i in order_indices] for row in query_engine_rows]
        projected_types = [types[i] for i in order_indices]
        assert_rows_equal(query_engine_proj, duckdb_proj, projected_types)


def get_orderby_indices(query, column_names):
    expr = sqlglot.parse_one(query)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return []

    indices = []
    for ordered in order.expressions:
        key = ordered.this

        # Handle numeric literals (e.g., ORDER BY 1, 2)
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(column_names):
                    indices.append(col_num - 1)  # Convert to 0-based index
                    continue
            except (ValueError, TypeError):
                pass

        # Handle simple column references
        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in column_names:
                indices.append(column_names.index(name))
                continue

        # For complex expressions (CASE, SUM, etc.), skip ORDER BY validation
        # We still validate overall result correctness with full sorting
        # Just don't validate the specific ORDER BY column ordering
        pass

    return indices


def create_duckdb_table(table_name, data_path):
    create_table(table_name, get_abs_file_path(__file__, data_path))


def normalize_rows(rows, types):
    return [normalize_row(row, types) for row in rows]


FLOATING_POINT_TYPES = ("double", "float", "decimal")


def normalize_row(row, types):
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id
        if type_id == "date":
            normalized_row.append(str(value))
        elif type_id in FLOATING_POINT_TYPES:
            normalized_row.append(float(value))
        else:
            normalized_row.append(value)
    return normalized_row


def assert_rows_equal(rows_1, rows_2, types):
    if len(rows_1) != len(rows_2):
        raise AssertionError(f"Row count mismatch: {len(rows_1)} vs {len(rows_2)}")

    float_cols = {i for i, t in enumerate(types) if t.id in FLOATING_POINT_TYPES}
    mismatches = []
    abs_tolerance = 0.02
    max_mismatches = 5

    for row_idx, (row_1, row_2) in enumerate(zip(rows_1, rows_2)):
        if len(row_1) != len(row_2):
            mismatches.append(f"Row: {row_idx} length mismatch: {len(row_1)} vs {len(row_2)}")
            if len(mismatches) >= max_mismatches:
                break
            continue

        for col_idx, (value_1, value_2) in enumerate(zip(row_1, row_2)):
            if value_1 is None and value_2 is None:
                continue
            if value_1 is None or value_2 is None:
                mismatches.append(f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2} (null mismatch)")
            elif col_idx in float_cols:
                if abs(value_1 - value_2) > abs_tolerance:
                    mismatches.append(
                        f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2} "
                        f"(diff={abs(value_1 - value_2):.6f}, tolerance={abs_tolerance})"
                    )
            elif value_1 != value_2:
                mismatches.append(f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2}")

            if len(mismatches) >= max_mismatches:
                break

        if len(mismatches) >= max_mismatches:
            break

    if mismatches:
        truncated_msg = f" (showing first {max_mismatches})" if len(mismatches) >= max_mismatches else ""
        mismatch_details = "\n  ".join(mismatches)
        raise AssertionError(f"Found {len(mismatches)} mismatches{truncated_msg}:\n  {mismatch_details}")


def initialize_output_dir(config, query_engine):
    output_dir = Path(config.getoption("--output-dir"))
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    if config.getoption(f"--store-{query_engine}-results"):
        Path(f"{output_dir}/{query_engine}_results").mkdir(exist_ok=False)
    if config.getoption("--store-reference-results"):
        Path(f"{output_dir}/reference_results").mkdir(exist_ok=False)
