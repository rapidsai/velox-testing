# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import math
import statistics

import pyarrow.parquet as pq
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths

# A small table cannot fill a single row group, so every row group it produces falls
# below the target. Files with fewer than this many row groups are not size checked.
_MIN_ROW_GROUPS_FOR_SIZE_CHECK = 10
# DuckDB's parallel Parquet writer can flush partial row groups before the final
# group. These are writer artifacts rather than sizing decisions, so tolerate a
# small fraction while requiring the file-level median to remain close to target.
_MAX_OUTSIDE_TOLERANCE_FRACTION = 0.05

# (median tolerance, individual row-group tolerance). The median describes the file
# as a whole, so it has the tighter bound. Individual groups can vary because they
# close on physical chunk boundaries and DuckDB quantizes row counts to multiples of
# 2,048.
_DUCKDB_TOLERANCES = (0.10, 0.15)
# tpchgen-cli controls its own row groups and does not use this repository's probe.
# At SF1 with a 1 MiB target, `part` consistently lands around 0.84 of target.
_TPCHGEN_TOLERANCES = (0.20, 0.20)


def test_approx_row_group_bytes_parameter(setup_and_teardown):
    """Validate that the approx_row_group_bytes parameter controls row group sizing.

    Verifies that:
    - At least one file is split into _MIN_ROW_GROUPS_FOR_SIZE_CHECK row groups
    - Each checked file's median row group is within its writer's median tolerance
    - At most _MAX_OUTSIDE_TOLERANCE_FRACTION of its row groups, rounded up to a
      whole group, are outside the writer's individual row-group tolerance
    - The final row group is excluded because it holds the remaining rows
    - Small tables are excluded from size checks (see _MIN_ROW_GROUPS_FOR_SIZE_CHECK)
    """
    data_dir_path, args = setup_and_teardown
    args.approx_row_group_bytes = 1024 * 1024
    generate_data_files(args)

    uses_duckdb = args.benchmark_type != "tpch" or args.use_duckdb
    tolerances = _DUCKDB_TOLERANCES if uses_duckdb else _TPCHGEN_TOLERANCES
    assert_approx_row_group_bytes_size(data_dir_path, args.approx_row_group_bytes, tolerances)
    assert_metadata_approx_row_group_bytes(data_dir_path, args.approx_row_group_bytes)


def assert_approx_row_group_bytes_size(data_dir_path, expected_row_group_byte_size, tolerances):
    median_tolerance, row_group_tolerance = tolerances
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
        assert abs(median_ratio - 1) <= median_tolerance, f"{file_path} median is {median_ratio:.3f} of target"

        outside = sum(abs(ratio - 1) > row_group_tolerance for ratio in ratios)
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
