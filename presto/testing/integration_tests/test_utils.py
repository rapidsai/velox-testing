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
from collections import Counter

from duckdb_utils import create_table


def execute_query_and_compare_results(presto_cursor, queries, query_id):
    query = queries[query_id]

    presto_cursor.execute(query)
    presto_rows = presto_cursor.fetchall()
    duckdb_rows, types, columns = execute_duckdb_query(query)

    debug_info = None
    if _is_single_none_result(presto_rows) and not _is_single_none_result(duckdb_rows):
        debug_info = _debug_none_result(presto_cursor, query_id, query)
        print(debug_info, flush=True)

    try:
        compare_results(presto_rows, duckdb_rows, types, query, columns)
    except AssertionError as e:
        mismatch_debug = _debug_result_mismatch(
            presto_cursor,
            query_id,
            query,
            presto_rows,
            duckdb_rows,
            types,
        )
        details = [str(e)]
        if debug_info:
            details.append(debug_info)
        if mismatch_debug:
            details.append(mismatch_debug)
        raise AssertionError("\n".join(details)) from e


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


def _format_row_sample(rows, limit=5):
    sample = [list(row) for row in rows[:limit]]
    return json.dumps(sample, default=str)


def _row_to_key(row):
    key = []
    for value in row:
        if isinstance(value, decimal.Decimal):
            key.append(str(value))
        else:
            key.append(value)
    return tuple(key)


def _run_debug_query(presto_cursor, query):
    presto_result = presto_cursor.execute(query).fetchall()
    duckdb_result = duckdb.sql(query).fetchall()
    return presto_result, duckdb_result


def _append_debug_query(lines, presto_cursor, label, query):
    try:
        presto_result, duckdb_result = _run_debug_query(presto_cursor, query)
        lines.append(
            f"{label}: presto={json.dumps(presto_result, default=str)} "
            f"duckdb={json.dumps(duckdb_result, default=str)}"
        )
    except Exception as exc:
        lines.append(f"{label}: debug query failed: {exc}")


def _debug_q17_mismatch(presto_cursor):
    lines = ["Q17 deep debug:"]
    q17_rewritten = (
        "WITH thresholds AS ( "
        "  SELECT l_partkey, 0.2 * avg(l_quantity) AS threshold "
        "  FROM lineitem "
        "  GROUP BY l_partkey "
        ") "
        "SELECT "
        "  count(*) AS qualifying_rows, "
        "  count(DISTINCT l.l_partkey) AS qualifying_parts, "
        "  sum(l.l_extendedprice) AS sum_extendedprice, "
        "  sum(l.l_extendedprice) / 7.0 AS avg_yearly "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        "JOIN thresholds t ON t.l_partkey = l.l_partkey "
        "WHERE p.p_brand = 'Brand#23' "
        "  AND p.p_container = 'MED BOX' "
        "  AND l.l_quantity < t.threshold"
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 rewritten aggregate check",
        q17_rewritten,
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 qualifying parts",
        "SELECT count(*) FROM part WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 quantity stats for qualifying parts",
        "SELECT count(*) AS rows, avg(l.l_quantity), min(l.l_quantity), max(l.l_quantity) "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        "WHERE p.p_brand = 'Brand#23' AND p.p_container = 'MED BOX'",
    )
    return "\n".join(lines)


def _debug_q22_mismatch(presto_cursor):
    lines = ["Q22 deep debug:"]
    avg_threshold_query = (
        "SELECT avg(c_acctbal) AS avg_positive_bal "
        "FROM customer "
        "WHERE c_acctbal > 0.00 "
        "  AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')"
    )
    _append_debug_query(lines, presto_cursor, "Q22 threshold", avg_threshold_query)
    _append_debug_query(
        lines,
        presto_cursor,
        "Q22 candidate customers",
        "SELECT count(*) "
        "FROM customer "
        "WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q22 customers above threshold",
        "WITH avg_bal AS ( "
        "  SELECT avg(c_acctbal) AS v "
        "  FROM customer "
        "  WHERE c_acctbal > 0.00 "
        "    AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') "
        ") "
        "SELECT count(*) "
        "FROM customer, avg_bal "
        "WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') "
        "  AND c_acctbal > avg_bal.v",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q22 customers above threshold with no orders",
        "WITH avg_bal AS ( "
        "  SELECT avg(c_acctbal) AS v "
        "  FROM customer "
        "  WHERE c_acctbal > 0.00 "
        "    AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') "
        ") "
        "SELECT count(*) "
        "FROM customer, avg_bal "
        "WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') "
        "  AND c_acctbal > avg_bal.v "
        "  AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)",
    )
    return "\n".join(lines)


def _debug_result_mismatch(
    presto_cursor,
    query_id,
    query,
    presto_rows,
    duckdb_rows,
    types,
):
    lines = [
        f"Mismatch debug for {query_id}: presto_rows={len(presto_rows)} "
        f"duckdb_rows={len(duckdb_rows)}",
        f"Query text: {query}",
    ]
    try:
        normalized_presto_rows = normalize_rows(presto_rows, types)
        normalized_duckdb_rows = normalize_rows(duckdb_rows, types)
        lines.append(
            "Presto first rows: "
            + _format_row_sample(normalized_presto_rows)
        )
        lines.append(
            "DuckDB first rows: "
            + _format_row_sample(normalized_duckdb_rows)
        )

        presto_counter = Counter(_row_to_key(row) for row in normalized_presto_rows)
        duckdb_counter = Counter(_row_to_key(row) for row in normalized_duckdb_rows)
        presto_only = list((presto_counter - duckdb_counter).elements())[:5]
        duckdb_only = list((duckdb_counter - presto_counter).elements())[:5]
        if presto_only:
            lines.append(
                "Rows only in Presto sample: "
                + json.dumps([list(row) for row in presto_only], default=str)
            )
        if duckdb_only:
            lines.append(
                "Rows only in DuckDB sample: "
                + json.dumps([list(row) for row in duckdb_only], default=str)
            )
    except Exception as exc:
        lines.append(f"Mismatch normalization debug failed: {exc}")

    if query_id == "Q17":
        lines.append(_debug_q17_mismatch(presto_cursor))
    elif query_id == "Q22":
        lines.append(_debug_q22_mismatch(presto_cursor))

    return "\n".join(lines)

def normalize_rows(rows, types):
    return [normalize_row(row, types) for row in rows]


FLOATING_POINT_TYPES = ("double", "float")
DECIMAL_TYPE = "decimal"
FLOAT_ABS_TOLERANCE = 0.02
FLOAT_REL_TOLERANCE = 1e-9


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
                rows[row_index][col_index] = pytest.approx(
                    rows[row_index][col_index],
                    abs=FLOAT_ABS_TOLERANCE,
                    rel=FLOAT_REL_TOLERANCE,
                )
