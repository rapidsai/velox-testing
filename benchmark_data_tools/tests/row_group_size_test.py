# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json

import pyarrow.parquet as pq
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths

# A small table cannot fill a single row group, so every row group it produces falls
# below the target. Files with fewer than this many row groups are not size checked.
_MIN_ROW_GROUPS_FOR_SIZE_CHECK = 10
# A row group can only close on a physical chunk boundary, so its size lands near the
# target rather than on it. At SF1 with a 1 MiB target the observed spread is 0.84-1.01
# of target through tpchgen and 0.93-1.31 through DuckDB.
_MIN_TARGET_RATIO = 0.80
_MAX_TARGET_RATIO = 1.35


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate that the approx_row_group_bytes parameter controls row group sizing.

    Verifies that:
    - At least one file is split into _MIN_ROW_GROUPS_FOR_SIZE_CHECK row groups
    - Every row group of a checked file is within _MIN/_MAX_TARGET_RATIO of the target
    - The last row group of each file is exempt because it holds the remaining rows
    - Small tables are excluded from size checks (see _MIN_ROW_GROUPS_FOR_SIZE_CHECK)
    """
    data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = 1024 * 1024
    generate_data_files(args)

    assert_approx_row_group_bytes_size(data_dir_path, args.approx_row_group_bytes)
    assert_metadata_approx_row_group_bytes(data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size):
    min_byte_size = expected_row_group_byte_size * _MIN_TARGET_RATIO
    max_byte_size = expected_row_group_byte_size * _MAX_TARGET_RATIO
    checked_file_count = 0

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        if parquet_file.num_row_groups < _MIN_ROW_GROUPS_FOR_SIZE_CHECK:
            continue
        checked_file_count += 1
        # The last row group holds whatever rows remain, so it is excluded.
        for row_group_index in range(parquet_file.num_row_groups - 1):
            byte_size = parquet_file.metadata.row_group(row_group_index).total_byte_size
            assert min_byte_size <= byte_size <= max_byte_size, f"{file_path} row group {row_group_index}"

    assert checked_file_count > 0, "no file had enough row groups, so no size was checked"


def assert_metadata_approx_row_group_bytes(data_dir_path, approx_row_group_bytes):
    with open(f"{data_dir_path}/metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        assert metadata["approx_row_group_bytes"] == approx_row_group_bytes
