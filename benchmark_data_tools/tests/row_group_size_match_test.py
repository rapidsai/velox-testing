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

import pyarrow.parquet as pq

from .common_fixtures import setup_and_teardown, get_all_parquet_relative_file_paths

def test_row_group_size_match(setup_and_teardown):
    orig_data_dir_path, final_data_dir_path = setup_and_teardown
    compare_row_group_sizes(orig_data_dir_path, final_data_dir_path)

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
