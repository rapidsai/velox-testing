# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json

import pyarrow.parquet as pq
import pytest
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate that the approx_row_group_bytes parameter controls row group sizing.

    Verifies that:
    - Row groups in parquet files are approximately 1MB in size
    - Row group sizes are within 25% tolerance of the target size
    - The last row group in each file may be smaller than the target (contains remaining rows)
    - At least one file has multiple row groups to ensure meaningful test coverage
    """
    data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = 1024 * 1024
    generate_data_files(args)

    assert_approx_row_group_bytes_size(data_dir_path, args.approx_row_group_bytes)
    assert_metadata_approx_row_group_bytes(data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size):
    max_num_row_groups_per_file = 0
    file_paths = get_all_parquet_relative_file_paths(data_dir_path)
    for file_path in file_paths:
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        num_row_groups = parquet_file.num_row_groups
        for row_group_index in range(num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            approx_row_group_byte_size = row_group.total_byte_size == pytest.approx(
                expected_row_group_byte_size, rel=0.25
            )
            # The last row group may be much smaller than the expected size.
            smaller_last_row_group_byte_size = (
                row_group_index == num_row_groups - 1 and row_group.total_byte_size < expected_row_group_byte_size
            )
            assert approx_row_group_byte_size or smaller_last_row_group_byte_size
        max_num_row_groups_per_file = max(max_num_row_groups_per_file, num_row_groups)
    # Ensure test coverage for at least one file with multiple row groups.
    assert max_num_row_groups_per_file > 1


def assert_metadata_approx_row_group_bytes(data_dir_path, approx_row_group_bytes):
    with open(f"{data_dir_path}/metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        assert metadata["approx_row_group_bytes"] == approx_row_group_bytes
