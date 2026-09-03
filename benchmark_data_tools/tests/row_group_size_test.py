# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import math
import statistics

import pyarrow.parquet as pq
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths

# Check only files with enough row groups for a meaningful size distribution.
_MIN_ROW_GROUPS_FOR_SIZE_CHECK = 10
# 8 MiB can limit DuckDB's 2,048-row quantization error while still producing
# enough row groups at SF1.
_ROW_GROUP_TARGET_BYTES = 8 * 1024 * 1024
# The median and at least 95% of non-final row groups must be within 20% of target.
_RELATIVE_TOLERANCE = 0.20
_MAX_OUTSIDE_TOLERANCE_FRACTION = 0.05


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate approximate row-group sizing.

    For files with at least 10 row groups, the median and at least 95% of
    non-final row groups must be within 20% of the requested size.
    """
    data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = _ROW_GROUP_TARGET_BYTES
    generate_data_files(args)

    assert_approx_row_group_bytes_size(data_dir_path, args.approx_row_group_bytes)
    assert_metadata_approx_row_group_bytes(data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size):
    checked_file_count = 0

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        if parquet_file.num_row_groups < _MIN_ROW_GROUPS_FOR_SIZE_CHECK:
            continue
        checked_file_count += 1
        ratios = [
            parquet_file.metadata.row_group(row_group_index).total_byte_size / expected_row_group_byte_size
            for row_group_index in range(parquet_file.num_row_groups - 1)
        ]

        median_ratio = statistics.median(ratios)
        assert abs(median_ratio - 1) <= _RELATIVE_TOLERANCE, f"{file_path} median is {median_ratio:.3f} of target"

        outside = sum(abs(ratio - 1) > _RELATIVE_TOLERANCE for ratio in ratios)
        allowed_outside = math.ceil(len(ratios) * _MAX_OUTSIDE_TOLERANCE_FRACTION)
        assert outside <= allowed_outside, (
            f"{file_path} has {outside} of {len(ratios)} row groups outside tolerance; "
            f"at most {allowed_outside} allowed"
        )

    assert checked_file_count > 0, "no file had enough row groups, so no size was checked"


def assert_metadata_approx_row_group_bytes(data_dir_path, approx_row_group_bytes):
    with open(f"{data_dir_path}/metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        assert metadata["approx_row_group_bytes"] == approx_row_group_bytes
