# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import re

import duckdb


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def init_benchmark_tables(benchmark_type, scale_factor, conn=duckdb):
    tables = conn.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        function_name = "dbgen"
    else:
        assert benchmark_type == "tpcds"
        function_name = "dsdgen"

    conn.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type}; CALL {function_name}(sf = {scale_factor});")


def drop_benchmark_tables():
    tables = duckdb.sql("SHOW TABLES").fetchall()
    for (table,) in tables:
        duckdb.sql(f"DROP TABLE {quote_ident(table)}")


def create_table(table_name, data_path):
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet';")


# Generates a sample table with a small limit.
# This is mainly used to extract the schema from the parquet files.
def create_not_null_table_from_sample(table_name, data_path):
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet' LIMIT 10;")
    ret = duckdb.sql(f"DESCRIBE TABLE {quote_ident(table_name)}").fetchall()
    for row in ret:
        duckdb.sql(f"ALTER TABLE {quote_ident(table_name)} ALTER COLUMN {row[0]} SET NOT NULL;")


def create_table_from_sample(table_name, data_path):
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet' LIMIT 10;")


def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))


def copy_to_parquet(select_query, file_path, row_group_rows=None, conn=duckdb):
    options = "FORMAT parquet, PARQUET_VERSION 'V2'"
    if row_group_rows is not None:
        options += f", ROW_GROUP_SIZE {row_group_rows}"
    conn.sql(f"COPY ({select_query}) TO '{file_path}' ({options})")


def get_select_query(table_name, convert_decimals_to_floats, conn=duckdb):
    if convert_decimals_to_floats:
        column_metadata_rows = conn.query(f"DESCRIBE {table_name}").fetchall()
        column_projections = [get_column_projection(column_metadata) for column_metadata in column_metadata_rows]
        query = f"SELECT {','.join(column_projections)} FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name}"
    return query


def get_column_projection(column_metadata):
    col_name, col_type, *_ = column_metadata
    if is_decimal_column(col_type):
        projection = f"CAST({col_name} AS DOUBLE) AS {col_name}"
    else:
        projection = col_name
    return projection
