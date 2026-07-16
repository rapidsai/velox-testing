# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import os

import prestodb


def create_tables(presto_cursor, schema_name, schemas_dir_path, data_sub_directory, external_location_base=None):
    drop_schema(presto_cursor, schema_name)
    presto_cursor.execute(f"CREATE SCHEMA hive.{schema_name}")

    schemas = get_table_schemas(schemas_dir_path)
    for table_name, schema in schemas:
        # When external_location_base is set (e.g. s3://bucket/prefix/sf100), point the
        # table at that base. Otherwise fall back to the local bind-mounted file path.
        if external_location_base:
            location = f"{external_location_base}/{table_name}"
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
        "file: location. Not needed when --external-location-base is set.",
    )
    parser.add_argument(
        "--external-location-base",
        type=str,
        required=False,
        default=None,
        help="Full URI base for the table EXTERNAL_LOCATION (e.g. s3://bucket/prefix/sf100). For each table "
        "'/<table_name>' is appended. If omitted, the default local file: path is used.",
    )
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(
        host=os.environ.get("HOSTNAME", "localhost"),
        port=int(os.environ.get("PORT", "8080")),
        user="test_user",
        catalog="hive",
    )
    cursor = conn.cursor()
    data_sub_directory = f"user_data/{args.data_dir_name}"
    create_tables(cursor, args.schema_name, args.schemas_dir_path, data_sub_directory, args.external_location_base)
