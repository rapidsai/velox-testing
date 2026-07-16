# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import re

import duckdb

_s3_configured = False


def ensure_remote_access(path) -> None:
    """Configure DuckDB to read from a remote object store if ``path`` needs it.

    The region must be provided via AWS_DEFAULT_REGION (or AWS_REGION). Do nothing for local
    paths.
    """
    global _s3_configured
    if _s3_configured or not str(path).startswith("s3://"):
        return
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if not region:
        raise RuntimeError(
            "Reading s3:// data requires the AWS region: set AWS_DEFAULT_REGION "
            "(or AWS_REGION), e.g. `export AWS_DEFAULT_REGION=us-east-2`."
        )
    # Enable DuckDB to access S3 storage
    duckdb.sql("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
    # Register an S3 access profile named s3_from_env that pulls credentials from the
    # standard AWS chain and targets region <region>.
    duckdb.sql(f"CREATE SECRET IF NOT EXISTS s3_from_env (TYPE s3, PROVIDER credential_chain, REGION '{region}')")
    _s3_configured = True


def read_scale_factor(metadata_uri: str):
    """Read the scale_factor field from a metadata.json at ``metadata_uri``."""
    # For local data
    if not str(metadata_uri).startswith("s3://"):
        with open(metadata_uri) as file:
            return json.load(file)["scale_factor"]
    # For remote data
    ensure_remote_access(metadata_uri)
    return duckdb.sql(f"SELECT scale_factor FROM read_json_auto('{metadata_uri}')").fetchone()[0]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def init_benchmark_tables(benchmark_type, scale_factor):
    tables = duckdb.sql("SHOW TABLES").fetchall()
    assert len(tables) == 0

    if benchmark_type == "tpch":
        function_name = "dbgen"
    else:
        assert benchmark_type == "tpcds"
        function_name = "dsdgen"

    duckdb.sql(f"INSTALL {benchmark_type}; LOAD {benchmark_type}; CALL {function_name}(sf = {scale_factor});")


def drop_benchmark_tables():
    tables = duckdb.sql("SHOW TABLES").fetchall()
    for (table,) in tables:
        duckdb.sql(f"DROP TABLE {quote_ident(table)}")


def create_table(table_name, data_path):
    ensure_remote_access(data_path)
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet';")


# Generates a sample table with a small limit.
# This is mainly used to extract the schema from the parquet files.
def create_not_null_table_from_sample(table_name, data_path):
    ensure_remote_access(data_path)
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet' LIMIT 10;")
    ret = duckdb.sql(f"DESCRIBE TABLE {quote_ident(table_name)}").fetchall()
    for row in ret:
        duckdb.sql(f"ALTER TABLE {quote_ident(table_name)} ALTER COLUMN {row[0]} SET NOT NULL;")


def create_table_from_sample(table_name, data_path):
    ensure_remote_access(data_path)
    duckdb.sql(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")
    duckdb.sql(f"CREATE TABLE {quote_ident(table_name)} AS SELECT * FROM '{data_path}/*.parquet' LIMIT 10;")


def is_decimal_column(column_type):
    return bool(re.match(r"^DECIMAL\(\d+,\d+\)$", column_type))
