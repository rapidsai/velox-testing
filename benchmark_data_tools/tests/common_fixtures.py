# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import sys

import pytest

# Enable imports from the benchmark_data_tools directory.
tests_dir = os.path.dirname(os.path.realpath(__file__))
benchmark_data_tools_dir = os.path.dirname(tests_dir)
sys.path.append(benchmark_data_tools_dir)

from dataclasses import dataclass  # noqa: E402
from pathlib import Path  # noqa: E402

from duckdb_utils import drop_benchmark_tables  # noqa: E402


@dataclass
class DataGenArgs:
    benchmark_type: str
    data_dir_path: str
    scale_factor: float
    convert_decimals_to_floats: bool
    use_duckdb: bool
    num_threads: int
    verbose: bool
    max_rows_per_file: int
    keep_original_dataset: bool
    approx_row_group_bytes: int
    codec_definitions: str = None


@pytest.fixture
def setup_and_teardown():
    test_data_dir_path = os.path.abspath("./tpch_test")
    try:
        args = DataGenArgs(
            benchmark_type="tpch",
            data_dir_path=test_data_dir_path,
            scale_factor=1.0,
            # Setting convert_decimals_to_floats to True ensures that the
            # Parquet rewrite path is executed.
            convert_decimals_to_floats=True,
            use_duckdb=False,
            num_threads=4,
            verbose=False,
            max_rows_per_file=100_000_000,
            keep_original_dataset=True,
            approx_row_group_bytes=128 * 1024 * 1024,
        )
        drop_benchmark_tables()
        yield test_data_dir_path, args
    finally:
        delete_directories([test_data_dir_path])


def get_all_parquet_relative_file_paths(dir_path):
    file_paths = {str(path.resolve()).removeprefix(f"{dir_path}/") for path in Path(dir_path).rglob("*.parquet")}
    assert len(file_paths) > 0
    return file_paths


def delete_directories(directory_paths):
    for directory_path in directory_paths:
        if os.path.exists(directory_path):
            assert os.path.isdir(directory_path)
            shutil.rmtree(directory_path)
