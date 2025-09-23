# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import duckdb

from duckdb_utils import init_benchmark_tables, is_decimal_column
from pathlib import Path


def generate_table_schemas(benchmark_type, schemas_dir_path, schema_name, convert_decimals_to_floats):
    init_benchmark_tables(benchmark_type, 0)

    Path(schemas_dir_path).mkdir(parents=True, exist_ok=True)

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        with open(f"{schemas_dir_path}/{table_name}.sql", "w") as file:
            file.write(get_table_schema(benchmark_type, table_name, schema_name, convert_decimals_to_floats))
            file.write("\n")


def get_table_schema(benchmark_type, table_name, schema_name, convert_decimals_to_floats):
    column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
    columns_ddl_list = [
        f"{' ' * 4}{get_column_definition(column_metadata, convert_decimals_to_floats)}"
        for column_metadata in column_metadata_rows
    ]
    columns_text = ",\n".join(columns_ddl_list)
    schema_var = "{schema}"
    schema = f"CREATE TABLE hive.{schema_var}.{table_name} \
    (\n{columns_text}\n) \
    WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{{file_path}}')"
    return schema


def get_column_definition(column_metadata, convert_decimals_to_floats):
    col_name, col_type, nullable, *_ = column_metadata
    if convert_decimals_to_floats and is_decimal_column(col_type):
        col_type = "DOUBLE"

    col_def = f"{col_name} {col_type}{' NOT NULL' if nullable == 'NO' else ''}"
    return col_def


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark table schemas. Only the TPC-H and TPC-DS "
                    "benchmarks are currently supported.")
    parser.add_argument("--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate table schemas for.")
    parser.add_argument("--schemas-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the schema files. "
                             "This directory will be created if it does not already exist.")
    parser.add_argument("--schema-name", type=str, required=True,
                        help="Name of the table schema.")
    parser.add_argument("--convert-decimals-to-floats", action="store_true", required=False,
                        default=False, help="Convert all decimal columns to float column type.")
    args = parser.parse_args()

    generate_table_schemas(args.benchmark_type, args.schemas_dir_path, args.schema_name, args.convert_decimals_to_floats)
