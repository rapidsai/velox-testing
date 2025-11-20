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
import duckdb_utils as duck
import os
from pathlib import Path


def generate_table_schemas(benchmark_type, schemas_dir_path, data_dir_name, verbose):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    for file in os.listdir(data_dir_name):
        sub_dir = os.path.join(data_dir_name, file)
        if os.path.isdir(sub_dir):
            if benchmark_type == "tpch":
                # For tpch we use the optional NOT NULL qualifier on all columns.
                duck.create_not_null_table_from_sample(os.path.basename(file), sub_dir)
            else:
                duck.create_table_from_sample(os.path.basename(file), sub_dir)

    Path(schemas_dir_path).mkdir(parents=True, exist_ok=True)

    tables = duckdb.sql("SHOW TABLES").fetchall()
    for table_name, in tables:
        with open(f"{schemas_dir_path}/{table_name}.sql", "w") as file:
            file.write(get_table_schema(benchmark_type, table_name))
            file.write("\n")
            if verbose:
                print(f"wrote: {schemas_dir_path}/{table_name}.sql")


def get_table_schema(benchmark_type, table_name):
    column_metadata_rows = duckdb.query(f"DESCRIBE {table_name}").fetchall()
    columns_ddl_list = [
        f"{' ' * 4}{get_column_definition(column_metadata)}"
        for column_metadata in column_metadata_rows
    ]
    columns_text = ",\n".join(columns_ddl_list)
    schema = f"CREATE TABLE hive.{{schema}}.{table_name} (\n{columns_text}\n) \
WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = 'file:{{file_path}}')"
    return schema


def get_column_definition(column_metadata):
    col_name, col_type, nullable, *_ = column_metadata
    col_def = f"{col_name} {col_type}{' NOT NULL' if nullable == 'NO' else ''}"
    return col_def


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate benchmark table schemas. Only the TPC-H and TPC-DS "
                    "benchmarks are currently supported.")
    parser.add_argument("-b", "--benchmark-type", type=str, required=True, choices=["tpch", "tpcds"],
                        help="The type of benchmark to generate table schemas for.")
    parser.add_argument("-s", "--schemas-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the schema files. "
                             "This directory will be created if it does not already exist.")
    parser.add_argument("-d", "--data-dir-name", type=str, required=True,
                        help="The name of the directory that contains the benchmark data.")
    parser.add_argument("-v", "--verbose", action="store_true", required=False,
                        default=False, help="Extra verbose logging")
    args = parser.parse_args()

    generate_table_schemas(args.benchmark_type, args.schemas_dir_path, args.data_dir_name, args.verbose)
