# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from generate_data_files import build_default_codec_defs, generate_data_files

from .common_fixtures import get_all_parquet_relative_file_paths

TESTS_DIR = Path(__file__).resolve().parent
TEST_CODEC_DEFINITIONS_PATH = TESTS_DIR / "test_codec_definitions.json"
TEST_RLE_DICTIONARY_PATH = TESTS_DIR / "test_codec_definitions_rle_dictionary.json"
TEST_NON_DEFAULT_COMPRESSION_PATH = TESTS_DIR / "test_codec_definitions_non_default_compression.json"
TEST_INVALID_COMPRESSION_PATH = TESTS_DIR / "test_codec_definitions_invalid_compression.json"


def test_default_codec_defs_applied(setup_and_teardown):
    """Generate data with default codec defs and verify encodings in parquet metadata.

    Verifies that:
    - Integer columns use DELTA_BINARY_PACKED encoding
    - Unique string columns have dictionary disabled (use PLAIN encoding)
    - Non-unique string columns retain dictionary encoding
    """
    data_dir_path, args = setup_and_teardown
    codec_defs = build_default_codec_defs()
    generate_data_files(args)

    disabled_dict_columns = set()
    delta_encoded_columns = set()
    for table_def in codec_defs["tables"]:
        for column_def in table_def.get("columns", []):
            if column_def.get("dictionary") is False:
                disabled_dict_columns.add((table_def["name"], column_def["name"]))
            if column_def.get("encoding") == "DELTA_BINARY_PACKED":
                delta_encoded_columns.add((table_def["name"], column_def["name"]))

    assert len(delta_encoded_columns) > 0, "Expected at least one DELTA_BINARY_PACKED column"
    assert len(disabled_dict_columns) > 0, "Expected at least one column with dictionary disabled"

    for file_path in get_all_parquet_relative_file_paths(data_dir_path):
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        table_name = Path(file_path).parent.name

        for row_group_index in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for col_index in range(row_group.num_columns):
                col_meta = row_group.column(col_index)
                col_name = col_meta.path_in_schema

                if (table_name, col_name) in delta_encoded_columns:
                    assert "DELTA_BINARY_PACKED" in col_meta.encodings, (
                        f"Expected DELTA_BINARY_PACKED in encodings for "
                        f"{table_name}.{col_name}, got {col_meta.encodings}"
                    )

                if (table_name, col_name) in disabled_dict_columns:
                    assert "RLE_DICTIONARY" not in col_meta.encodings, (
                        f"Expected no RLE_DICTIONARY for {table_name}.{col_name}, got {col_meta.encodings}"
                    )


def test_custom_codec_defs_from_file(setup_and_teardown):
    """Generate data with a custom codec definitions file and verify encodings.

    Uses tests/test_codec_definitions.json which specifies:
    - lineitem.l_orderkey: DELTA_BINARY_PACKED, no dictionary
    - lineitem.l_returnflag: PLAIN, dictionary on
    - lineitem.l_comment: PLAIN, UNCOMPRESSED, no dictionary
    - orders.o_orderkey: DELTA_BINARY_PACKED, no dictionary
    """
    data_dir_path, args = setup_and_teardown
    args.codec_definitions = str(TEST_CODEC_DEFINITIONS_PATH)
    generate_data_files(args)

    file_paths = get_all_parquet_relative_file_paths(data_dir_path)
    lineitem_files = [file_path for file_path in file_paths if "lineitem" in file_path]
    orders_files = [file_path for file_path in file_paths if "orders" in file_path]
    assert len(lineitem_files) > 0, "Expected lineitem parquet files"
    assert len(orders_files) > 0, "Expected orders parquet files"

    for file_path in lineitem_files:
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        for row_group_index in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for col_index in range(row_group.num_columns):
                col_meta = row_group.column(col_index)
                col_name = col_meta.path_in_schema
                if col_name == "l_orderkey":
                    assert "DELTA_BINARY_PACKED" in col_meta.encodings
                    assert "RLE_DICTIONARY" not in col_meta.encodings
                    assert col_meta.compression == "SNAPPY"
                elif col_name == "l_returnflag":
                    assert "RLE_DICTIONARY" in col_meta.encodings
                    assert col_meta.compression == "SNAPPY"
                elif col_name == "l_comment":
                    assert "RLE_DICTIONARY" not in col_meta.encodings
                    assert col_meta.compression == "UNCOMPRESSED"

    for file_path in orders_files:
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        for row_group_index in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for col_index in range(row_group.num_columns):
                col_meta = row_group.column(col_index)
                if col_meta.path_in_schema == "o_orderkey":
                    assert "DELTA_BINARY_PACKED" in col_meta.encodings
                    assert "RLE_DICTIONARY" not in col_meta.encodings
                    assert col_meta.compression == "SNAPPY"


def test_codec_definitions_rejected_for_tpcds(setup_and_teardown):
    _, args = setup_and_teardown
    args.benchmark_type = "tpcds"
    args.codec_definitions = str(TEST_CODEC_DEFINITIONS_PATH)
    with pytest.raises(ValueError, match="--codec-definitions is currently only supported for TPC-H benchmarks"):
        generate_data_files(args)


def test_codec_definitions_rejected_with_duckdb(setup_and_teardown):
    _, args = setup_and_teardown
    args.use_duckdb = True
    args.codec_definitions = str(TEST_CODEC_DEFINITIONS_PATH)
    with pytest.raises(ValueError, match="--codec-definitions is not supported with --use-duckdb"):
        generate_data_files(args)


def test_rle_dictionary_encoding_rejected(setup_and_teardown):
    _, args = setup_and_teardown
    args.codec_definitions = str(TEST_RLE_DICTIONARY_PATH)
    with pytest.raises(ValueError, match="RLE_DICTIONARY cannot be used as a column encoding"):
        generate_data_files(args)


def test_non_default_table_compression(setup_and_teardown):
    """Generate data with a non-default table-level compression and verify per-column codecs.

    Asserts that pyarrow reports ZSTD for all lineitem columns except l_comment, which is
    UNCOMPRESSED (the per-column override should take precedence over the table-level codec).
    """
    data_dir_path, args = setup_and_teardown
    args.codec_definitions = str(TEST_NON_DEFAULT_COMPRESSION_PATH)
    generate_data_files(args)

    file_paths = get_all_parquet_relative_file_paths(data_dir_path)
    lineitem_files = [file_path for file_path in file_paths if "lineitem" in file_path]
    assert len(lineitem_files) > 0, "Expected lineitem parquet files"

    for file_path in lineitem_files:
        parquet_file = pq.ParquetFile(f"{data_dir_path}/{file_path}")
        for row_group_index in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for col_index in range(row_group.num_columns):
                col_meta = row_group.column(col_index)
                col_name = col_meta.path_in_schema
                if col_name == "l_comment":
                    assert col_meta.compression == "UNCOMPRESSED", (
                        f"Expected UNCOMPRESSED for l_comment, got {col_meta.compression}"
                    )
                else:
                    assert col_meta.compression == "ZSTD", f"Expected ZSTD for {col_name}, got {col_meta.compression}"


def test_invalid_table_compression_rejected(setup_and_teardown):
    """An invalid table-level compression value is translated into a user-facing ValueError."""
    _, args = setup_and_teardown
    args.codec_definitions = str(TEST_INVALID_COMPRESSION_PATH)
    with pytest.raises(ValueError) as exception_info:
        generate_data_files(args)
    assert str(exception_info.value) == (
        "Invalid 'compression' value 'INVALID' for table 'lineitem' in codec definitions. "
        "See codec_definition_template.json for valid values."
    )
