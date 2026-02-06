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


def _default_port():
    env_port = os.getenv("PRESTO_COORDINATOR_PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    return 8080


DEFAULT_HOST = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
DEFAULT_PORT = _default_port()


def create_tables(presto_cursor, schema_name, schemas_dir_path, data_sub_directory):
    drop_schema(presto_cursor, schema_name)
    presto_cursor.execute(f"CREATE SCHEMA hive.{schema_name}")

    schemas = get_table_schemas(schemas_dir_path)
    for table_name, schema in schemas:
        presto_cursor.execute(
            schema.format(file_path=f"/var/lib/presto/data/hive/data/{data_sub_directory}/{table_name}",
                          schema=schema_name))


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
    parser.add_argument("--host", type=str, default=DEFAULT_HOST,
                        help="Presto coordinator hostname (default: PRESTO_COORDINATOR_HOST or localhost)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="Presto coordinator port (default: PRESTO_COORDINATOR_PORT or 8080)")
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(host=args.host, port=args.port, user="test_user", catalog="hive")
    cursor = conn.cursor()
    data_sub_directory = f"user_data/{args.data_dir_name}"
    create_tables(cursor, args.schema_name, args.schemas_dir_path, data_sub_directory)
