# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Reads the SQLStorm StackOverflow schema (PostgreSQL DDL) and maps it to Presto types."""

import os
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

POSTGRES_TO_PRESTO = {
    exp.DataType.Type.SMALLINT: "SMALLINT",
    exp.DataType.Type.INT: "INTEGER",
    exp.DataType.Type.BIGINT: "BIGINT",
    exp.DataType.Type.BOOLEAN: "BOOLEAN",
    exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
    exp.DataType.Type.VARCHAR: "VARCHAR",
    exp.DataType.Type.TEXT: "VARCHAR",
}


@dataclass
class Table:
    name: str
    columns: list[tuple[str, str]]  # (lowercase column name, Presto type)


def schema_path(sqlstorm_dir):
    return os.path.join(sqlstorm_dir, "v1.0", "stackoverflow", "schema.sql")


def load_tables(sqlstorm_dir):
    with open(schema_path(sqlstorm_dir)) as f:
        statements = sqlglot.parse(f.read(), read="postgres")
    tables = []
    for statement in statements:
        if not isinstance(statement, exp.Create):
            continue
        columns = [
            (column.name.lower(), POSTGRES_TO_PRESTO[column.args["kind"].this])
            for column in statement.find_all(exp.ColumnDef)
        ]
        tables.append(Table(statement.this.this.name, columns))
    return tables
