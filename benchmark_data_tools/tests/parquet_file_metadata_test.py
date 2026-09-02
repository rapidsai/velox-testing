# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import duckdb
import pyarrow.parquet as pq
from duckdb_utils import copy_to_parquet
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths


def test_generated_files_use_expected_page_format(setup_and_teardown):
    """Verify every generated Parquet file uses the v1 page format."""
    data_dir_path, args = setup_and_teardown
    generate_data_files(args)

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        metadata = pq.ParquetFile(f"{data_dir_path}/{file_path}").metadata
        assert int(float(metadata.format_version)) == 1, (
            f"Expected Parquet v1 format for '{file_path}', got '{metadata.format_version}'"
        )


def test_duckdb_copy_uses_v1_page_format(tmp_path):
    output = tmp_path / "output.parquet"
    with duckdb.connect() as conn:
        copy_to_parquet("SELECT * FROM range(10)", output, conn=conn)

    assert int(float(pq.ParquetFile(output).metadata.format_version)) == 1
