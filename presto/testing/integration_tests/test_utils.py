# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import sys


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))

sys.path.append(get_abs_file_path("../../../benchmark_data_tools"))

import duckdb
import decimal
import json
import pytest
import sqlglot

from duckdb_utils import create_table


def execute_query_and_compare_results(presto_cursor, queries, query_id):
    query = queries[query_id]

    presto_cursor.execute(query)
    presto_rows = presto_cursor.fetchall()
    duckdb_rows, types, columns = execute_duckdb_query(query)

    debug_info = None
    if _is_single_none_result(presto_rows) and not _is_single_none_result(duckdb_rows):
        debug_info = _debug_none_result(presto_cursor, query_id, query)
        print(debug_info)

    try:
        compare_results(presto_rows, duckdb_rows, types, query, columns)
    except AssertionError as e:
        if debug_info:
            raise AssertionError(f"{e}\n{debug_info}") from e
        raise


def get_is_sorted_query(query):
    return any(isinstance(expr, sqlglot.exp.Order) for expr in sqlglot.parse_one(query).iter_expressions())


def none_safe_sort_key(row):
    """Sort key that treats None as less than any other value."""
    return tuple((0, x) if x is not None else (1, None) for x in row)


def compare_results(presto_rows, duckdb_rows, types, query, column_names):
    row_count = len(presto_rows)
    assert row_count == len(duckdb_rows)

    duckdb_rows = normalize_rows(duckdb_rows, types)
    presto_rows = normalize_rows(presto_rows, types)

    # We need a full sort for all non-ORDER BY columns because some ORDER BY comparison
    # will be equal and the resulting order of non-ORDER BY columns will be ambiguous.
    sorted_duckdb_rows = sorted(duckdb_rows, key=none_safe_sort_key)
    sorted_presto_rows = sorted(presto_rows, key=none_safe_sort_key)
    approx_floats(sorted_duckdb_rows, types)
    assert sorted_presto_rows == sorted_duckdb_rows

    # If we have an ORDER BY clause we want to test that the resulting order of those
    # columns is correct, in addition to overall values being correct.
    # However, we can only validate the ORDER BY if we can extract column indices.
    # For complex ORDER BY expressions (aggregates, CASE statements, etc.), we skip
    # the ORDER BY validation but still validate overall result correctness.
    order_indices = get_orderby_indices(query, column_names)
    if order_indices:
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

        # Handle numeric literals (e.g., ORDER BY 1, 2)
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(column_names):
                    indices.append(col_num - 1)  # Convert to 0-based index
                    continue
            except (ValueError, TypeError):
                pass

        # Handle simple column references
        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in column_names:
                indices.append(column_names.index(name))
                continue

        # For complex expressions (CASE, SUM, etc.), skip ORDER BY validation
        # We still validate overall result correctness with full sorting
        # Just don't validate the specific ORDER BY column ordering
        pass

    return indices

def create_duckdb_table(table_name, data_path):
    create_table(table_name, get_abs_file_path(data_path))


def execute_duckdb_query(query):
    relation = duckdb.sql(query)
    return relation.fetchall(), relation.types, relation.columns


def _is_single_none_result(rows):
    return len(rows) == 1 and len(rows[0]) == 1 and rows[0][0] is None


def _debug_none_result(presto_cursor, query_id, query):
    lines = []
    try:
        if query_id == "Q6":
            where_clause = query.split(" WHERE ", 1)[1]
            count_query = (
                "SELECT count(*) AS match_count FROM lineitem WHERE "
                + where_clause
            )
            presto_count = presto_cursor.execute(count_query).fetchone()[0]
            duckdb_count = duckdb.sql(count_query).fetchone()[0]
            lines.append(
                f"Q6 debug: match_count presto={presto_count} duckdb={duckdb_count}"
            )
        elif query_id == "Q19":
            where_clause = query.split(" WHERE ", 1)[1]
            full_count_query = (
                "SELECT count(*) AS match_count FROM lineitem, part WHERE "
                + where_clause
            )
            presto_full = presto_cursor.execute(full_count_query).fetchone()[0]
            duckdb_full = duckdb.sql(full_count_query).fetchone()[0]
            lines.append(
                f"Q19 debug: full match_count presto={presto_full} duckdb={duckdb_full}"
            )

            where_clause = where_clause.strip()
            if where_clause.startswith("(") and where_clause.endswith(")"):
                inner = where_clause[1:-1]
                branches = inner.split(") OR (")
            else:
                branches = [where_clause]
            for idx, branch in enumerate(branches, 1):
                branch_query = (
                    "SELECT count(*) AS match_count FROM lineitem, part WHERE "
                    + branch
                )
                presto_branch = presto_cursor.execute(branch_query).fetchone()[0]
                duckdb_branch = duckdb.sql(branch_query).fetchone()[0]
                lines.append(
                    "Q19 debug: branch "
                    + str(idx)
                    + f" match_count presto={presto_branch} duckdb={duckdb_branch}"
                )
    except Exception as exc:
        lines.append(f"Debug query failed: {exc}")

    if not lines:
        return "No debug info available."
    return "\n".join(lines)

def normalize_rows(rows, types):
    return [normalize_row(row, types) for row in rows]


FLOATING_POINT_TYPES = ("double", "float")
DECIMAL_TYPE = "decimal"


def normalize_row(row, types):
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id
        if type_id == "date":
            normalized_row.append(str(value))
        elif type_id == DECIMAL_TYPE:
            if isinstance(value, decimal.Decimal):
                normalized_row.append(value)
            else:
                normalized_row.append(decimal.Decimal(str(value)))
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
