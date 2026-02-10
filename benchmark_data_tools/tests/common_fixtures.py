# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import pytest
import os
import shutil

import sys

# Enable imports from the benchmark_data_tools directory.
tests_dir = os.path.dirname(os.path.realpath(__file__))
benchmark_data_tools_dir = os.path.dirname(tests_dir)
sys.path.append(benchmark_data_tools_dir)

from dataclasses import dataclass
from duckdb_utils import drop_benchmark_tables
from generate_data_files import generate_data_files
from pathlib import Path


def ensure_tpchgen_cli_available():
    # Ensure that the tpchgen-cli executable can be found.
    # venv_bin_dir = f"{benchmark_data_tools_dir}/.venv/bin"
    venv_bin_dir = Path(benchmark_data_tools_dir) / ".venv" / "bin"
    if venv_bin_dir.exists():
        os.environ["PATH"] = os.pathsep.join([os.environ["PATH"], str(venv_bin_dir)])

    tpchgen_path = shutil.which("tpchgen-cli")
    if tpchgen_path is None:
        raise FileNotFoundError(f"tpchgen-cli not found. Expected {venv_bin_dir} or an active venv "
                           "with tpchgen-cli on PATH.") from None



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


@pytest.fixture
def setup_and_teardown():
    test_data_dir_path = os.path.abspath("./tpch_test")
    orig_test_data_dir_path = f"{test_data_dir_path}-temp"
    try:
        args = DataGenArgs(benchmark_type="tpch",
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
                           approx_row_group_bytes=128 * 1024 * 1024)
        drop_benchmark_tables()
        yield orig_test_data_dir_path, test_data_dir_path, args
    finally:
        delete_directories([test_data_dir_path, orig_test_data_dir_path])


def get_all_parquet_relative_file_paths(dir_path):
    file_paths = {
        str(path.resolve()).removeprefix(f"{dir_path}/")
        for path in Path(dir_path).rglob("*.parquet")
    }
    assert len(file_paths) > 0
    return file_paths


def delete_directories(directory_paths):
    for directory_path in directory_paths:
        if os.path.exists(directory_path):
            assert os.path.isdir(directory_path)
            shutil.rmtree(directory_path)
