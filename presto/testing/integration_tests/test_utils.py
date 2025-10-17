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

from duckdb_utils import create_table


def execute_query_and_compare_results(presto_cursor, queries, query_id):
    query = queries[query_id]

    presto_cursor.execute(query)
    presto_rows = presto_cursor.fetchall()
    duckdb_rows, types, columns = execute_duckdb_query(query)

    compare_results(presto_rows, duckdb_rows, types, query, columns)


def get_is_sorted_query(query):
    return any(isinstance(expr, sqlglot.exp.Order) for expr in sqlglot.parse_one(query).iter_expressions())


def compare_results(presto_rows, duckdb_rows, types, query, column_names):
    row_count = len(presto_rows)
    assert row_count == len(duckdb_rows)

    duckdb_rows = normalize_rows(duckdb_rows, types)
    presto_rows = normalize_rows(presto_rows, types)

    # We need a full sort for all non-ORDER BY columns because some ORDER BY comparison
    # will be equal and the resulting order of non-ORDER BY columns will be ambiguous.
    sorted_duckdb_rows = sorted(duckdb_rows)
    sorted_presto_rows = sorted(presto_rows)
    approx_floats(sorted_duckdb_rows, types)
    assert sorted_presto_rows == sorted_duckdb_rows

    # If we have an ORDER BY clause we want to test that the resulting order of those
    # columns is correct, in addition to overall values being correct.
    order_indices = get_orderby_indices(query, column_names)
    # Only a sorted query should have ORDER BY indicies.
    assert not (order_indices == get_is_sorted_query(query))
    approx_floats(duckdb_rows, types)
    # Project both results to ORDER BY columns and compare in original order
    duckdb_proj = [[row[i] for i in order_indices] for row in duckdb_rows]
    presto_proj = [[row[i] for i in order_indices] for row in presto_rows]
    assert presto_proj == duckdb_proj


def get_orderby_indices(query, column_names):
    expr = sqlglot.parse_one(query)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return []

    indices = []
    for ordered in order.expressions:
        key = ordered.this
        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in column_names:
                indices.append(column_names.index(name))
                continue
        raise AssertionError(f"ORDER BY expression does not match any column names: {key}")
    return indices

def create_duckdb_table(table_name, data_path):
    create_table(table_name, get_abs_file_path(data_path))


def execute_duckdb_query(query):
    relation = duckdb.sql(query)
    return relation.fetchall(), relation.types, relation.columns


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
