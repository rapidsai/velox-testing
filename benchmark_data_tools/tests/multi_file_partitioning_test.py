# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from generate_data_files import generate_data_files


def test_max_rows_per_file_splits_tables_tpcds(setup_and_teardown):
    """Validate that max_rows_per_file controls TPC-DS file partitioning.

    Verifies that:
    - At least one table is split into multiple files
    - Part files use contiguous 1-based names, including single-part tables
    - Every part except the last contains exactly max_rows_per_file rows
    - The last part contains between one and max_rows_per_file rows
    - The parts contain the same total number of rows as the source table
    """
    data_dir_path, args = setup_and_teardown
    args.benchmark_type = "tpcds"
    args.use_duckdb = True
    # SF1 tables fit below the 100M default, so use a lower limit to exercise splitting.
    args.max_rows_per_file = 500_000
    generate_data_files(args)

    assert_partitions_respect_max_rows_per_file(data_dir_path, args.max_rows_per_file)


def assert_partitions_respect_max_rows_per_file(data_dir_path, max_rows_per_file):
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
        # Only the final part may contain fewer rows than the limit.
        assert all(rows == max_rows_per_file for rows in rows_per_part[:-1]), rows_per_part
        assert 0 < rows_per_part[-1] <= max_rows_per_file, rows_per_part
        # Verify that the parts preserve the table's row count.
        expected_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert sum(rows_per_part) == expected_rows

        if num_parts > 1:
            split_table_count += 1

    assert split_table_count > 0, "no table was split, so the partitioning path was not tested"
