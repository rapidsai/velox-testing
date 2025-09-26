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

import duckdb
import re

def init_benchmark_tables(benchmark_type, scale_factor):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        function_name = "dbgen"
    else:
        assert benchmark_type == "tpcds"
        function_name = "dsdgen"

    duckdb.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type}; CALL {function_name}(sf = {scale_factor});")

def create_table(table_name, data_path):
    duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{data_path}/*.parquet';")

def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))

def map_table_schemas(verbose):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) > 0 # Benchmark tables must be initialized.
    table_to_schema_map = {}
    for table_name, in tables:
        table_to_schema_map[table_name] = duckdb.sql(f"SHOW {table_name}").fetchall()
    if verbose:
        print(f"Benchmark tables: {table_to_schema_map.keys()}")
    return table_to_schema_map
