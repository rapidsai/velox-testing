# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import shutil

import pyarrow.parquet as pq
import pyarrow.types as pat
from duckdb_utils import drop_benchmark_tables
from generate_data_files import generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths


def test_convert_decimals_to_floats(setup_and_teardown):
    """Validate that all decimal columns are converted to floats when convert_decimals_to_floats is set.

    Generates data without the flag first to establish a reference showing decimal columns exist,
    then regenerates with the flag and verifies:
    - No decimal columns remain
    - Every previously-decimal column is now a float
    """
    data_dir_path, args = setup_and_teardown

    args.convert_decimals_to_floats = False
    generate_data_files(args)
    reference_decimal_columns = get_decimal_column_names(data_dir_path)
    assert len(reference_decimal_columns) > 0

    shutil.rmtree(data_dir_path)
    drop_benchmark_tables()

    args.convert_decimals_to_floats = True
    generate_data_files(args)
    converted_decimal_columns = get_decimal_column_names(data_dir_path)
    assert len(converted_decimal_columns) == 0
    assert_decimal_columns_are_floats(data_dir_path, reference_decimal_columns)


def get_decimal_column_names(data_dir_path):
    decimal_columns = set()
    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        schema = pq.ParquetFile(f"{data_dir_path}/{file_path}").schema_arrow
        for field in schema:
            if pat.is_decimal(field.type):
                decimal_columns.add(field.name)
    return decimal_columns


def assert_decimal_columns_are_floats(data_dir_path, expected_float_columns):
    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        schema = pq.ParquetFile(f"{data_dir_path}/{file_path}").schema_arrow
        for field in schema:
            if field.name in expected_float_columns:
                assert pat.is_float64(field.type), (
                    f"Column '{field.name}' in '{file_path}' should be a float but is {field.type}"
                )
