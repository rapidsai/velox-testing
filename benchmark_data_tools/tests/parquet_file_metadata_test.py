# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pyarrow.parquet as pq
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths


def test_generated_files_use_expected_page_format(setup_and_teardown):
    """Verify each benchmark uses its expected Parquet page format."""
    data_dir_path, args = setup_and_teardown
    generate_data_files(args)
    # TODO: Switch TPC-DS to Parquet V2 after a cuDF release includes rapidsai/cudf#23314.
    # cuDF 26.08 cannot read V2 files produced by DuckDB 1.6.0.dev291.
    expected_version = 2 if args.benchmark_type == "tpch" else 1

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        metadata = pq.ParquetFile(f"{data_dir_path}/{file_path}").metadata
        assert int(float(metadata.format_version)) == expected_version, (
            f"Expected Parquet v{expected_version} format for '{file_path}', got '{metadata.format_version}'"
        )
