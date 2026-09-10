# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
from pathlib import Path

import duckdb
import duckdb_utils as duck


def _sample_table(benchmark_type, table_name, data_path):
    """Load a small sample of a table's Parquet into DuckDB so its schema can be read
    back via DESCRIBE. ``data_path`` may be a local directory or a remote (e.g. s3://)
    prefix."""
    if benchmark_type == "tpch":
        # For tpch we use the optional NOT NULL qualifier on all columns.
        duck.create_not_null_table_from_sample(table_name, data_path)
    else:
        duck.create_table_from_sample(table_name, data_path)


def generate_table_schemas(
    benchmark_type,
    schemas_dir_path,
    data_dir_name=None,
    verbose=False,
    external_location_base=None,
    table_names=None,
):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if external_location_base:
        # Derive the schema from the Parquet that actually lives at the external
        # location. A remote prefix cannot be listed, so the table names are supplied
        # by the caller.
        base = external_location_base.rstrip("/")
        for table_name in table_names:
            _sample_table(benchmark_type, table_name, f"{base}/{table_name}")
    else:
        for file in os.listdir(data_dir_name):
            sub_dir = os.path.join(data_dir_name, file)
            if os.path.isdir(sub_dir):
                _sample_table(benchmark_type, os.path.basename(file), sub_dir)

    Path(schemas_dir_path).mkdir(parents=True, exist_ok=True)

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for (table_name,) in tables:
        with open(f"{schemas_dir_path}/{table_name}.sql", "w") as file:
            file.write(get_table_schema(benchmark_type, table_name))
            file.write("\n")
            if verbose:
                print(f"wrote: {schemas_dir_path}/{table_name}.sql")


def get_table_schema(benchmark_type, table_name):
    column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
    columns_ddl_list = [
        f"{' ' * 4}{get_column_definition(column_metadata)}" for column_metadata in column_metadata_rows
    ]
    columns_text = ",\n".join(columns_ddl_list)
    schema = f"CREATE TABLE hive.{{schema}}.{table_name} (\n{columns_text}\n) \
WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{{location}}')"
    return schema


def get_column_definition(column_metadata):
    col_name, col_type, nullable, *_ = column_metadata
    col_def = f"{col_name} {col_type}{' NOT NULL' if nullable == 'NO' else ''}"
    return col_def


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark table schemas. Only the TPC-H and TPC-DS benchmarks are currently supported."
    )
    parser.add_argument(
        "-b",
        "--benchmark-type",
        type=str,
        required=True,
        choices=["tpch", "tpcds"],
        help="The type of benchmark to generate table schemas for.",
    )
    parser.add_argument(
        "-s",
        "--schemas-dir-path",
        type=str,
        required=True,
        help="The path to the directory that will contain the schema files. "
        "This directory will be created if it does not already exist.",
    )
    parser.add_argument(
        "-d",
        "--data-dir-name",
        type=str,
        required=False,
        default=None,
        help="Path to a local directory that contains the benchmark data (one subdirectory per "
        "table). Mutually exclusive with --external-location-base.",
    )
    parser.add_argument(
        "--external-location-base",
        type=str,
        required=False,
        default=None,
        help="URI base whose per-table subdirectories hold the Parquet data (e.g. "
        "s3://bucket/prefix/scale-1000). The schema is derived from that data instead of a local "
        "directory. Requires --table-names.",
    )
    parser.add_argument(
        "--table-names",
        nargs="+",
        default=None,
        help="Table names to generate schemas for. Required with --external-location-base since a "
        "remote prefix cannot be listed.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", required=False, default=False, help="Extra verbose logging"
    )
    args = parser.parse_args()

    if bool(args.data_dir_name) == bool(args.external_location_base):
        parser.error("Provide exactly one of --data-dir-name or --external-location-base")
    if args.external_location_base and not args.table_names:
        parser.error("--table-names is required when --external-location-base is set")

    generate_table_schemas(
        args.benchmark_type,
        args.schemas_dir_path,
        data_dir_name=args.data_dir_name,
        verbose=args.verbose,
        external_location_base=args.external_location_base,
        table_names=args.table_names,
    )
