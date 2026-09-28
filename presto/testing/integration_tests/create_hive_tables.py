# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os

import prestodb


def create_tables(presto_cursor, schema_name, schemas_dir_path, data_sub_directory, remote_data_dir_path=None):
    drop_schema(presto_cursor, schema_name)
    presto_cursor.execute(f"CREATE SCHEMA hive.{schema_name}")

    schemas = get_table_schemas(schemas_dir_path)
    for table_name, schema in schemas:
        # When remote_data_dir_path is set (e.g. s3://bucket/prefix/sf100), point the
        # table at its subdirectory there. Otherwise fall back to the local bind-mounted file path.
        if remote_data_dir_path:
            location = f"{remote_data_dir_path.rstrip('/')}/{table_name}"
        else:
            location = f"file:/var/lib/presto/data/hive/data/{data_sub_directory}/{table_name}"
        presto_cursor.execute(schema.format(location=location, schema=schema_name))


def get_table_schemas(schemas_dir):
    result = []
    for file_name in os.listdir(schemas_dir):
        with open(os.path.join(schemas_dir, file_name), "r") as file:
            result.append((file_name.replace(".sql", ""), file.read()))
    return result


def drop_schema(presto_cursor, schema_name):
    schemas = presto_cursor.execute("SHOW SCHEMAS FROM hive").fetchall()
    if [schema_name] in schemas:
        tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
        for (table,) in tables:
            presto_cursor.execute(f"DROP TABLE IF EXISTS hive.{schema_name}.{table}")
        presto_cursor.execute(f"DROP SCHEMA hive.{schema_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Hive tables based on the table schema files inside the given schema."
    )
    parser.add_argument(
        "--schema-name", type=str, required=True, help="Name of the schema that will contain the created Hive tables."
    )
    parser.add_argument(
        "--schemas-dir-path",
        type=str,
        required=True,
        help="The path to the directory that will contain the schema files.",
    )
    parser.add_argument(
        "--data-dir-name",
        type=str,
        required=False,
        default="",
        help="The name of the directory that contains the benchmark data. Only used to build the local "
        "file: location. Not needed when --remote-data-dir-path is set.",
    )
    parser.add_argument(
        "--remote-data-dir-path",
        type=str,
        required=False,
        default=None,
        help="URI of the remote directory that contains one subdirectory per table (e.g. s3://bucket/prefix/sf100). "
        "Each table's EXTERNAL_LOCATION is '<remote-data-dir-path>/<table_name>'. If omitted, the default local "
        "file: path is used.",
    )
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(
        host=os.environ.get("HOSTNAME", "localhost"),
        port=int(os.environ.get("PORT", "8080")),
        user="test_user",
        catalog="hive",
    )
    cursor = conn.cursor()
    data_sub_directory = "" if args.remote_data_dir_path else f"user_data/{args.data_dir_name}"
    create_tables(cursor, args.schema_name, args.schemas_dir_path, data_sub_directory, args.remote_data_dir_path)
