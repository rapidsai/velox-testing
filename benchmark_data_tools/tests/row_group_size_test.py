# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import math
import statistics

import pyarrow.parquet as pq
import pytest
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths

# Check only files with enough groups for a meaningful distribution.
_MIN_ROW_GROUPS_FOR_SIZE_CHECK = 10
_RELATIVE_TOLERANCE = 0.20
_MAX_OUTSIDE_TOLERANCE_FRACTION = 0.10


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate that the approx_row_group_bytes parameter controls row group sizing.

    Verifies that:
    - At least one file has enough row groups to check
    - For large files, the median row group size is within 20% of the 1 MiB target
    - At least 90% of non-final row groups are within the same 20% tolerance
    - The final row group is excluded because it contains the remaining rows
    - Small tables are excluded from size checks (see _MIN_ROW_GROUPS_FOR_SIZE_CHECK)
    """
    data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = 1024 * 1024
    generate_data_files(args)

    assert_approx_row_group_bytes_size(data_dir_path, args.approx_row_group_bytes)
    assert_metadata_approx_row_group_bytes(data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size):
    max_num_row_groups_per_file = 0

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        num_row_groups = parquet_file.num_row_groups
        max_num_row_groups_per_file = max(max_num_row_groups_per_file, num_row_groups)
        if num_row_groups < _MIN_ROW_GROUPS_FOR_SIZE_CHECK:
            continue
        ratios = [
            parquet_file.metadata.row_group(row_group_index).total_byte_size / expected_row_group_byte_size
            for row_group_index in range(num_row_groups - 1)
        ]

        median_ratio = statistics.median(ratios)
        assert median_ratio == pytest.approx(1, rel=_RELATIVE_TOLERANCE), (
            f"{file_path} median is {median_ratio:.3f} of target"
        )

        outside = sum(abs(ratio - 1) > _RELATIVE_TOLERANCE for ratio in ratios)
        allowed_outside = math.ceil(len(ratios) * _MAX_OUTSIDE_TOLERANCE_FRACTION)
        assert outside <= allowed_outside, (
            f"{file_path} has {outside} of {len(ratios)} row groups outside tolerance; "
            f"at most {allowed_outside} allowed"
        )

    assert max_num_row_groups_per_file >= _MIN_ROW_GROUPS_FOR_SIZE_CHECK


def assert_metadata_approx_row_group_bytes(data_dir_path, approx_row_group_bytes):
    with open(f"{data_dir_path}/metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        assert metadata["approx_row_group_bytes"] == approx_row_group_bytes
