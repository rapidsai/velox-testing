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
import os
import prestodb


def create_tables(presto_cursor, schema_name, schemas_dir_path, data_sub_directory, analyze_tables_flag):
    drop_schema(presto_cursor, schema_name)
    presto_cursor.execute(f"CREATE SCHEMA hive.{schema_name}")

    schemas = get_table_schemas(schemas_dir_path)
    table_names = []
    for table_name, schema in schemas:
        presto_cursor.execute(
            schema.format(file_path=f"/var/lib/presto/data/hive/data/{data_sub_directory}/{table_name}"))
        table_names.append(table_name)
    
    if analyze_tables_flag:
        analyze_tables(presto_cursor, schema_name, table_names)


def analyze_tables(presto_cursor, schema_name, table_names=None):
    if table_names is None:
        try:
            tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
            table_names = [table_name for table_name, in tables]
        except Exception as e:
            print(f"Warning: Could not list tables in schema {schema_name}: {e}")
            return
    
    for table_name in table_names:
        try:
            presto_cursor.execute(f"ANALYZE TABLE hive.{schema_name}.{table_name}")
        except Exception as e:
            print(f"Warning: Failed to analyze table {table_name}: {e}")


def get_table_schemas(schemas_dir):
    result = []
    for file_name in os.listdir(schemas_dir):
        with open(os.path.join(schemas_dir, file_name), "r") as file:
            result.append((file_name.replace(".sql", ""), file.read()))
    return result


def drop_schema(presto_cursor, schema_name):
    schemas = presto_cursor.execute(f"SHOW SCHEMAS FROM hive").fetchall()
    if [schema_name] in schemas:
        tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
        for table, in tables:
            presto_cursor.execute(f"DROP TABLE IF EXISTS hive.{schema_name}.{table}")
        presto_cursor.execute(f"DROP SCHEMA hive.{schema_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Hive tables based on the table schema files inside the given schema.")
    parser.add_argument("--schema-name", type=str, required=True,
                        help="Name of the schema that will contain the created Hive tables.")
    parser.add_argument("--schemas-dir-path", type=str, required=True,
                        help="The path to the directory that will contain the schema files.")
    parser.add_argument("--data-dir-name", type=str, required=True,
                        help="The name of the directory that contains the benchmark data.")
    parser.add_argument("--no-analyze-tables", action="store_true", default=False,
                        help="Skip ANALYZE TABLE step (analysis runs by default)")
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(host="localhost", port=8080, user="test_user", catalog="hive")
    cursor = conn.cursor()
    data_sub_directory = f"user_data/{args.data_dir_name}"
    create_tables(cursor, args.schema_name, args.schemas_dir_path, data_sub_directory, 
                  analyze_tables_flag=not args.no_analyze_tables)
