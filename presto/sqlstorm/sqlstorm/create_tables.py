# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Registers the StackOverflow Parquet tables as Hive tables on a running Presto cluster."""

import argparse
import os
import sys
import tempfile

import prestodb

from sqlstorm.schema import load_tables

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "testing", "integration_tests"))
from create_hive_tables import create_tables  # noqa: E402


def write_table_schemas(tables, schemas_dir):
    for table in tables:
        columns = ",\n".join(f"    {name} {presto_type}" for name, presto_type in table.columns)
        with open(os.path.join(schemas_dir, f"{table.name.lower()}.sql"), "w") as f:
            f.write(
                f"CREATE TABLE hive.{{schema}}.{table.name.lower()} (\n{columns}\n) "
                "WITH (FORMAT = 'PARQUET', EXTERNAL_LOCATION = '{location}')\n"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlstorm-dir", required=True, help="Checkout of github.com/SQL-Storm/SQLStorm")
    parser.add_argument("--schema-name", default="stackoverflow_dba")
    parser.add_argument("--data-dir-name", default="stackoverflow_dba", help="Data directory under PRESTO_DATA_DIR")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    tables = load_tables(args.sqlstorm_dir)
    cursor = prestodb.dbapi.connect(host=args.host, port=args.port, user="sqlstorm", catalog="hive").cursor()
    with tempfile.TemporaryDirectory() as schemas_dir:
        write_table_schemas(tables, schemas_dir)
        create_tables(cursor, args.schema_name, schemas_dir, f"user_data/{args.data_dir_name}")

    for table in tables:
        cursor.execute(f"SELECT count(*) FROM hive.{args.schema_name}.{table.name.lower()}")
        print(f"{table.name.lower():20} {cursor.fetchone()[0]:>10}")


if __name__ == "__main__":
    main()
