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

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .common_fixtures import (
    setup_and_teardown,
    get_all_parquet_relative_file_paths,
    generate_data_files
)


def test_data_content_match(setup_and_teardown):
    """Validate data content equality between original and transformed parquet files.

    Verifies that:
    - File paths match between original and final data directories
    - Row counts match for each corresponding file
    - Decimal columns are correctly converted to float64 (values compared after casting)
    - Non-decimal columns remain identical
    """
    orig_data_dir_path, final_data_dir_path, args = setup_and_teardown
    generate_data_files(args)
    compare_data_content(orig_data_dir_path, final_data_dir_path)


def compare_data_content(orig_data_dir_path, final_data_dir_path):
    orig_file_paths = get_all_parquet_relative_file_paths(orig_data_dir_path)
    final_file_paths = get_all_parquet_relative_file_paths(final_data_dir_path)

    assert orig_file_paths == final_file_paths

    for file_path in orig_file_paths:
        orig_table = pq.read_table(f"{orig_data_dir_path}/{file_path}")
        final_table = pq.read_table(f"{final_data_dir_path}/{file_path}")

        assert orig_table.num_rows == final_table.num_rows

        for orig_column, final_column in zip(orig_table.columns, final_table.columns):
            if pa.types.is_decimal(orig_column.type):
                assert pa.types.is_float64(final_column.type)
                assert pc.cast(orig_column, pa.float64()) == final_column
            else:
                assert orig_column == final_column
