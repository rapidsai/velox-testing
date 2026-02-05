# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pyarrow.parquet as pq
import pytest

from .common_fixtures import (
    setup_and_teardown,
    get_all_parquet_relative_file_paths,
    generate_data_files
)


def test_row_group_size_match(setup_and_teardown):
    """Validate row group structure preservation between original and transformed parquet files.

    Verifies that:
    - File paths match between original and final data directories
    - Number of row groups match for each corresponding file
    - Each row group maintains the same number of rows
    """
    orig_data_dir_path, final_data_dir_path, args = setup_and_teardown
    generate_data_files(args)
    compare_row_group_sizes(orig_data_dir_path, final_data_dir_path)
    assert_metadata_approx_row_group_bytes(final_data_dir_path, args.approx_row_group_bytes)


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate that the approx_row_group_bytes parameter controls row group sizing.

    Verifies that:
    - Row groups in both original and final parquet files are approximately 1MB in size
    - Row group sizes are within 25% tolerance of the target size
    - The last row group in each file may be smaller than the target (contains remaining rows)
    - At least one file has multiple row groups to ensure meaningful test coverage
    """
    orig_data_dir_path, final_data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = 1024 * 1024
    generate_data_files(args)

    assert_approx_row_group_bytes_size(orig_data_dir_path, args.approx_row_group_bytes)
    assert_approx_row_group_bytes_size(final_data_dir_path, args.approx_row_group_bytes)
    assert_metadata_approx_row_group_bytes(final_data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size):
    max_num_row_groups_per_file = 0
    file_paths = get_all_parquet_relative_file_paths(data_dir_path)
    for file_path in file_paths:
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        num_row_groups = parquet_file.num_row_groups
        for row_group_index in range(num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            approx_row_group_byte_size = \
                (row_group.total_byte_size == pytest.approx(expected_row_group_byte_size, rel=0.25))
            # The last row group may be much smaller than the expected size.
            smaller_last_row_group_byte_size = \
                (row_group_index == num_row_groups - 1 and
                 row_group.total_byte_size < expected_row_group_byte_size)
            assert approx_row_group_byte_size or smaller_last_row_group_byte_size
        max_num_row_groups_per_file = max(max_num_row_groups_per_file, num_row_groups)
    # Ensure test coverage for at least one file with multiple row groups.
    assert max_num_row_groups_per_file > 1


def compare_row_group_sizes(orig_data_dir_path, final_data_dir_path):
    orig_file_paths = get_all_parquet_relative_file_paths(orig_data_dir_path)
    final_file_paths = get_all_parquet_relative_file_paths(final_data_dir_path)

    assert orig_file_paths == final_file_paths

    for file_path in orig_file_paths:
        orig_parquet_file = pq.ParquetFile(f"{orig_data_dir_path}/{file_path}")
        final_parquet_file = pq.ParquetFile(f"{final_data_dir_path}/{file_path}")

        assert orig_parquet_file.num_row_groups == final_parquet_file.num_row_groups

        for row_group_index in range(orig_parquet_file.num_row_groups):
            orig_row_group = orig_parquet_file.metadata.row_group(row_group_index)
            final_row_group = final_parquet_file.metadata.row_group(row_group_index)
            assert orig_row_group.num_rows == final_row_group.num_rows

def assert_metadata_approx_row_group_bytes(final_data_dir_path, approx_row_group_bytes):
    with open(f"{final_data_dir_path}/metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        assert  metadata["approx_row_group_bytes"] == approx_row_group_bytes
