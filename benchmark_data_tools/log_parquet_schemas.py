# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
from pathlib import Path

import pyarrow.parquet as pq


def get_schema_columns(parquet_path):
    schema = pq.ParquetFile(parquet_path).schema
    columns = tuple((column.path, column.physical_type, str(column.logical_type)) for column in schema)
    return str(schema), columns


def log_parquet_schemas(data_dir_path):
    data_dir = Path(data_dir_path)
    parquet_paths = sorted(data_dir.rglob("*.parquet"))
    if not parquet_paths:
        raise FileNotFoundError(f"No Parquet files found under '{data_dir}'")

    table_schemas = {}
    for parquet_path in parquet_paths:
        relative_path = parquet_path.relative_to(data_dir)
        table_name = relative_path.parts[0]
        schema_text, columns = get_schema_columns(parquet_path)
        if table_name in table_schemas:
            first_path, first_schema_text, _ = table_schemas[table_name]
            if schema_text != first_schema_text:
                raise ValueError(
                    f"Parquet schema mismatch for table '{table_name}': '{first_path}' differs from '{relative_path}'"
                )
            continue
        table_schemas[table_name] = (relative_path, schema_text, columns)

    for table_name, (relative_path, _, columns) in sorted(table_schemas.items()):
        print(f"--- parquet schema: {table_name} ({relative_path}) ---")
        for column_path, physical_type, logical_type in columns:
            print(f"{column_path}: physical={physical_type}, logical={logical_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log physical and logical schemas for a Parquet dataset.")
    parser.add_argument(
        "--data-dir-path", type=str, required=True, help="Root directory containing table Parquet files."
    )
    args = parser.parse_args()
    log_parquet_schemas(args.data_dir_path)
