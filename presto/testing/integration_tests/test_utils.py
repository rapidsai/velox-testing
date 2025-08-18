import os
import sys

def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))

sys.path.append(get_abs_file_path("../../../benchmark_data_tools"))

import duckdb
import json
import pytest
import sqlglot

from duckdb_utils import init_benchmark_tables


def get_table_schemas(benchmark_type):
    result = []
    schemas_dir = get_abs_file_path(f"schemas/{benchmark_type}")
    for file_name in os.listdir(schemas_dir):
        with open(os.path.join(schemas_dir, file_name), "r") as file:
            result.append((file_name.replace(".sql", ""), file.read()))
    return result


def create_tables(presto_cursor, schemas, benchmark_type):
    drop_tables(presto_cursor, schemas, benchmark_type)
    presto_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS hive.{benchmark_type}_test")

    for table_name, schema in schemas:
        presto_cursor.execute(
            schema.format(file_path=f"/var/lib/presto/data/hive/data/integration_test/{benchmark_type}/{table_name}"))


def get_queries(benchmark_type):
    with open(get_abs_file_path(f"queries/{benchmark_type}/queries.json"), "r") as file:
        return json.load(file)


def drop_tables(presto_cursor, schemas, benchmark_type):
    for table, _ in schemas:
        presto_cursor.execute(f"DROP TABLE IF EXISTS hive.{benchmark_type}_test.{table}")
    presto_cursor.execute(f"DROP SCHEMA IF EXISTS hive.{benchmark_type}_test")


def execute_query_and_compare_results(presto_cursor, queries, query_id):
    query = queries[query_id]

    presto_cursor.execute(query)
    presto_rows = presto_cursor.fetchall()
    duckdb_rows, types = execute_duckdb_query(query)

    compare_results(presto_rows, duckdb_rows, types, get_is_sorted_query(query))


def get_is_sorted_query(query):
    return any(isinstance(expr, sqlglot.exp.Order) for expr in sqlglot.parse_one(query).iter_expressions())


def compare_results(presto_rows, duckdb_rows, types, is_sorted_query):
    row_count = len(presto_rows)
    assert row_count == len(duckdb_rows)

    duckdb_rows = normalize_rows(duckdb_rows, types)
    presto_rows = normalize_rows(presto_rows, types)

    if not is_sorted_query:
        duckdb_rows = sorted(duckdb_rows)
        presto_rows = sorted(presto_rows)

    approx_floats(duckdb_rows, types)

    assert presto_rows == duckdb_rows


def init_duckdb_tables(benchmark_type):
    init_benchmark_tables(benchmark_type, get_scale_factor(benchmark_type))


def execute_duckdb_query(query):
    relation = duckdb.sql(query)
    return relation.fetchall(), relation.types


def get_scale_factor(benchmark_type):
    with open(get_abs_file_path(f"data/{benchmark_type}/metadata.json"), "r") as file:
        metadata = json.load(file)
        return metadata["scale_factor"]


def normalize_rows(rows, types):
    return [normalize_row(row, types) for row in rows]


def normalize_row(row, types):
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id
        if type_id in ("decimal", "date"):
            normalized_row.append(str(value))
        elif type_id in ("double", "float"):
            normalized_row.append(float(value))
        else:
            normalized_row.append(value)
    return normalized_row


def approx_floats(rows, types):
    for col_index, type in enumerate(types):
        if type.id in ("double", "float"):
            for row_index in range(len(rows)):
                rows[row_index][col_index] = pytest.approx(rows[row_index][col_index], abs=0.02)
