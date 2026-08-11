# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from duckdb_utils import init_benchmark_tables
from generate_data_files import generate_data_files


def test_max_rows_per_file_splits_tables(setup_and_teardown):
    """Validate that max_rows_per_file controls file partitioning.

    Verifies that:
    - At least one table is split into multiple files
    - Part files use contiguous 1-based names, including single-part tables
    - Every part contains between one and max_rows_per_file rows
    - The parts contain the same total number of rows as the source table
    """
    data_dir_path, args = setup_and_teardown
    # SF1 tables fit below the 100M default, so use a lower limit to exercise splitting.
    args.max_rows_per_file = 500_000
    expected_row_counts = get_expected_row_counts(args.benchmark_type, args.scale_factor)
    generate_data_files(args)

    assert_partitions_respect_max_rows_per_file(
        data_dir_path,
        args.max_rows_per_file,
        expected_row_counts,
    )


def get_expected_row_counts(benchmark_type, scale_factor):
    with duckdb.connect() as conn:
        init_benchmark_tables(benchmark_type, scale_factor, conn)
        return {
            table_name: conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            for (table_name,) in conn.sql("SHOW TABLES").fetchall()
        }


def assert_partitions_respect_max_rows_per_file(data_dir_path, max_rows_per_file, expected_row_counts):
    split_table_count = 0
    for table_dir in sorted(path for path in Path(data_dir_path).iterdir() if path.is_dir()):
        table_name = table_dir.name
        file_paths = list(table_dir.glob("*.parquet"))
        num_parts = len(file_paths)

        # All tables use the same 1-indexed layout, including single-part tables.
        assert {path.name for path in file_paths} == {
            f"{table_name}-{part}.parquet" for part in range(1, num_parts + 1)
        }

        rows_per_part = [
            pq.ParquetFile(table_dir / f"{table_name}-{part}.parquet").metadata.num_rows
            for part in range(1, num_parts + 1)
        ]
        assert all(0 < rows <= max_rows_per_file for rows in rows_per_part), rows_per_part
        assert sum(rows_per_part) == expected_row_counts[table_name]

        if num_parts > 1:
            split_table_count += 1

    assert split_table_count > 0, "no table was split, so the partitioning path was not tested"
