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


def get_queries(benchmark_type):
    with open(get_abs_file_path(f"queries/{benchmark_type}/queries.json"), "r") as file:
        return json.load(file)


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


FLOATING_POINT_TYPES = ("double", "float", "decimal")


def normalize_row(row, types):
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id
        if type_id == "date":
            normalized_row.append(str(value))
        elif type_id in FLOATING_POINT_TYPES:
            normalized_row.append(float(value))
        else:
            normalized_row.append(value)
    return normalized_row


def approx_floats(rows, types):
    for col_index, type in enumerate(types):
        if type.id in FLOATING_POINT_TYPES:
            for row_index in range(len(rows)):
                rows[row_index][col_index] = pytest.approx(rows[row_index][col_index], abs=0.02)
