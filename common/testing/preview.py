# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for result preview and storage."""

import pandas as pd


def show_result_preview(columns, rows, preview_rows_count, result_source, query_id):
    """Print a preview of query results."""
    start_line = f"\n{'-' * 50} {result_source} {query_id} Result Preview {'-' * 50}"
    print(start_line)
    preview_rows_count = min(preview_rows_count, len(rows))
    print(f"Showing {preview_rows_count} of {len(rows)} rows...\n")
    df = pd.DataFrame(rows[:preview_rows_count], columns=columns)
    print(df)
    print("-" * len(start_line))


def write_rows_to_parquet(output_dir, subdir, result_file_name, rows, columns):
    """Write result rows to a parquet file."""
    df = pd.DataFrame(rows, columns=columns)
    df.to_parquet(f"{output_dir}/{subdir}/{result_file_name}")
