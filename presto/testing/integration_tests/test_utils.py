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


def _debug_q3_mismatch(presto_cursor):
    lines = ["Q3 deep debug:"]
    grouped = (
        "SELECT "
        "  l_orderkey, "
        "  sum(l_extendedprice * (1 - l_discount)) AS revenue, "
        "  o_orderdate, "
        "  o_shippriority "
        "FROM customer, orders, lineitem "
        "WHERE c_mktsegment = 'BUILDING' "
        "  AND c_custkey = o_custkey "
        "  AND l_orderkey = o_orderkey "
        "  AND o_orderdate < CAST('1995-03-15' AS date) "
        "  AND l_shipdate > CAST('1995-03-15' AS date) "
        "GROUP BY l_orderkey, o_orderdate, o_shippriority"
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q3 grouped row count",
        f"SELECT count(*) FROM ({grouped}) q3_grouped",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q3 original top15",
        f"{grouped} ORDER BY revenue DESC, o_orderdate LIMIT 15",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q3 deterministic top15",
        f"{grouped} ORDER BY revenue DESC, o_orderdate, l_orderkey LIMIT 15",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q3 boundary tie count at rank 10",
        "WITH grouped AS ( "
        + grouped
        + "), ranked AS ( "
        "  SELECT "
        "    l_orderkey, "
        "    revenue, "
        "    o_orderdate, "
        "    o_shippriority, "
        "    row_number() OVER (ORDER BY revenue DESC, o_orderdate) AS rn "
        "  FROM grouped "
        "), boundary AS ( "
        "  SELECT revenue, o_orderdate "
        "  FROM ranked "
        "  WHERE rn = 10 "
        ") "
        "SELECT count(*) "
        "FROM grouped, boundary "
        "WHERE grouped.revenue = boundary.revenue "
        "  AND grouped.o_orderdate = boundary.o_orderdate",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q3 boundary tie rows",
        "WITH grouped AS ( "
        + grouped
        + "), ranked AS ( "
        "  SELECT "
        "    l_orderkey, "
        "    revenue, "
        "    o_orderdate, "
        "    o_shippriority, "
        "    row_number() OVER (ORDER BY revenue DESC, o_orderdate) AS rn "
        "  FROM grouped "
        "), boundary AS ( "
        "  SELECT revenue, o_orderdate "
        "  FROM ranked "
        "  WHERE rn = 10 "
        ") "
        "SELECT l_orderkey, revenue, o_orderdate, o_shippriority "
        "FROM grouped, boundary "
        "WHERE grouped.revenue = boundary.revenue "
        "  AND grouped.o_orderdate = boundary.o_orderdate "
        "ORDER BY l_orderkey "
        "LIMIT 20",
    )
    return "\n".join(lines)


def _debug_q18_mismatch(presto_cursor):
    lines = ["Q18 deep debug:"]
    qualifying_orders = (
        "SELECT l_orderkey "
        "FROM lineitem "
        "GROUP BY l_orderkey "
        "HAVING sum(l_quantity) > 300"
    )
    qualifying_orders_double = (
        "SELECT l_orderkey "
        "FROM lineitem "
        "GROUP BY l_orderkey "
        "HAVING sum(CAST(l_quantity AS DOUBLE)) > 300"
    )
    per_order_sums = (
        "SELECT "
        "  l_orderkey, "
        "  sum(l_quantity) AS sum_quantity_dec, "
        "  sum(CAST(l_quantity AS DOUBLE)) AS sum_quantity_double "
        "FROM lineitem "
        "GROUP BY l_orderkey"
    )
    grouped = (
        "SELECT "
        "  c_name, "
        "  c_custkey, "
        "  o_orderkey, "
        "  o_orderdate, "
        "  o_totalprice, "
        "  sum(l_quantity) AS sum_quantity "
        "FROM customer, orders, lineitem "
        "WHERE o_orderkey IN ( "
        + qualifying_orders
        + ") "
        "  AND c_custkey = o_custkey "
        "  AND o_orderkey = l_orderkey "
        "GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice"
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 qualifying orderkey count",
        f"SELECT count(*) FROM ({qualifying_orders}) q18_orders",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 qualifying orderkey count (double HAVING)",
        f"SELECT count(*) FROM ({qualifying_orders_double}) q18_orders_double",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 per-order sum threshold stats",
        "WITH per_order AS ( "
        + per_order_sums
        + ") "
        "SELECT "
        "  count(*) AS order_count, "
        "  min(sum_quantity_dec) AS min_sum_quantity, "
        "  max(sum_quantity_dec) AS max_sum_quantity, "
        "  avg(sum_quantity_dec) AS avg_sum_quantity, "
        "  sum(CASE WHEN sum_quantity_dec > 300 THEN 1 ELSE 0 END) AS gt_300_count, "
        "  sum(CASE WHEN sum_quantity_dec = 300 THEN 1 ELSE 0 END) AS eq_300_count, "
        "  sum(CASE WHEN sum_quantity_dec < 300 THEN 1 ELSE 0 END) AS lt_300_count "
        "FROM per_order",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 decimal-vs-double qualifying overlap",
        "WITH per_order AS ( "
        + per_order_sums
        + "), dec_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_dec > 300 "
        "), dbl_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_double > 300 "
        ") "
        "SELECT "
        "  (SELECT count(*) FROM dec_q) AS dec_count, "
        "  (SELECT count(*) FROM dbl_q) AS dbl_count, "
        "  (SELECT count(*) FROM dec_q d JOIN dbl_q b ON d.l_orderkey = b.l_orderkey) AS overlap_count, "
        "  (SELECT count(*) FROM dec_q d LEFT JOIN dbl_q b ON d.l_orderkey = b.l_orderkey WHERE b.l_orderkey IS NULL) AS dec_only_count, "
        "  (SELECT count(*) FROM dbl_q b LEFT JOIN dec_q d ON d.l_orderkey = b.l_orderkey WHERE d.l_orderkey IS NULL) AS dbl_only_count",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 decimal-only qualifying sample",
        "WITH per_order AS ( "
        + per_order_sums
        + "), dec_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_dec > 300 "
        "), dbl_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_double > 300 "
        ") "
        "SELECT p.l_orderkey, p.sum_quantity_dec, p.sum_quantity_double "
        "FROM per_order p "
        "JOIN dec_q d ON d.l_orderkey = p.l_orderkey "
        "LEFT JOIN dbl_q b ON b.l_orderkey = p.l_orderkey "
        "WHERE b.l_orderkey IS NULL "
        "ORDER BY p.sum_quantity_dec DESC, p.l_orderkey "
        "LIMIT 20",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 double-only qualifying sample",
        "WITH per_order AS ( "
        + per_order_sums
        + "), dec_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_dec > 300 "
        "), dbl_q AS ( "
        "  SELECT l_orderkey FROM per_order WHERE sum_quantity_double > 300 "
        ") "
        "SELECT p.l_orderkey, p.sum_quantity_dec, p.sum_quantity_double "
        "FROM per_order p "
        "JOIN dbl_q b ON b.l_orderkey = p.l_orderkey "
        "LEFT JOIN dec_q d ON d.l_orderkey = p.l_orderkey "
        "WHERE d.l_orderkey IS NULL "
        "ORDER BY p.sum_quantity_double DESC, p.l_orderkey "
        "LIMIT 20",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 grouped row count",
        f"SELECT count(*) FROM ({grouped}) q18_grouped",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 grouped rows violating HAVING threshold count",
        "WITH grouped AS ( "
        + grouped
        + ") "
        "SELECT count(*) "
        "FROM grouped "
        "WHERE sum_quantity <= 300",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 grouped rows violating HAVING threshold sample",
        "WITH grouped AS ( "
        + grouped
        + ") "
        "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_quantity "
        "FROM grouped "
        "WHERE sum_quantity <= 300 "
        "ORDER BY sum_quantity DESC, o_orderkey, c_custkey "
        "LIMIT 20",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 qualifying keys missing in orders",
        "SELECT count(*) "
        "FROM ( "
        "  SELECT q.l_orderkey "
        "  FROM ( "
        + qualifying_orders
        + ") q "
        "  LEFT JOIN orders o ON o.o_orderkey = q.l_orderkey "
        "  WHERE o.o_orderkey IS NULL "
        ") q18_missing_orders",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 IN-vs-join filtered orderkey count",
        "WITH qualifying AS ( "
        + qualifying_orders
        + "), in_filtered AS ( "
        "  SELECT o_orderkey "
        "  FROM orders "
        "  WHERE o_orderkey IN (SELECT l_orderkey FROM qualifying) "
        "), join_filtered AS ( "
        "  SELECT o.o_orderkey "
        "  FROM orders o "
        "  JOIN qualifying q ON q.l_orderkey = o.o_orderkey "
        ") "
        "SELECT "
        "  (SELECT count(*) FROM in_filtered) AS in_count, "
        "  (SELECT count(*) FROM join_filtered) AS join_count, "
        "  (SELECT count(*) FROM in_filtered i JOIN join_filtered j ON i.o_orderkey = j.o_orderkey) AS overlap_count, "
        "  (SELECT count(*) FROM in_filtered i LEFT JOIN join_filtered j ON i.o_orderkey = j.o_orderkey WHERE j.o_orderkey IS NULL) AS in_only_count, "
        "  (SELECT count(*) FROM join_filtered j LEFT JOIN in_filtered i ON i.o_orderkey = j.o_orderkey WHERE i.o_orderkey IS NULL) AS join_only_count",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 original top110",
        f"{grouped} ORDER BY o_totalprice DESC, o_orderdate LIMIT 110",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 deterministic top110",
        f"{grouped} ORDER BY o_totalprice DESC, o_orderdate, o_orderkey, c_custkey LIMIT 110",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 boundary tie count at rank 100",
        "WITH grouped AS ( "
        + grouped
        + "), ranked AS ( "
        "  SELECT "
        "    c_name, "
        "    c_custkey, "
        "    o_orderkey, "
        "    o_orderdate, "
        "    o_totalprice, "
        "    sum_quantity, "
        "    row_number() OVER (ORDER BY o_totalprice DESC, o_orderdate) AS rn "
        "  FROM grouped "
        "), boundary AS ( "
        "  SELECT o_totalprice, o_orderdate "
        "  FROM ranked "
        "  WHERE rn = 100 "
        ") "
        "SELECT count(*) "
        "FROM grouped, boundary "
        "WHERE grouped.o_totalprice = boundary.o_totalprice "
        "  AND grouped.o_orderdate = boundary.o_orderdate",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q18 boundary tie rows",
        "WITH grouped AS ( "
        + grouped
        + "), ranked AS ( "
        "  SELECT "
        "    c_name, "
        "    c_custkey, "
        "    o_orderkey, "
        "    o_orderdate, "
        "    o_totalprice, "
        "    sum_quantity, "
        "    row_number() OVER (ORDER BY o_totalprice DESC, o_orderdate) AS rn "
        "  FROM grouped "
        "), boundary AS ( "
        "  SELECT o_totalprice, o_orderdate "
        "  FROM ranked "
        "  WHERE rn = 100 "
        ") "
        "SELECT "
        "  grouped.c_name, "
        "  grouped.c_custkey, "
        "  grouped.o_orderkey, "
        "  grouped.o_orderdate, "
        "  grouped.o_totalprice, "
        "  grouped.sum_quantity "
        "FROM grouped, boundary "
        "WHERE grouped.o_totalprice = boundary.o_totalprice "
        "  AND grouped.o_orderdate = boundary.o_orderdate "
        "ORDER BY grouped.o_orderkey, grouped.c_custkey "
        "LIMIT 20",
    )
    return "\n".join(lines)


def _debug_q17_mismatch(presto_cursor):
    lines = ["Q17 deep debug (operator-step):"]
    part_keys = (
        "SELECT p_partkey "
        "FROM part "
        "WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'"
    )
    lineitem_filtered = (
        "SELECT l.l_partkey, l.l_quantity, l.l_extendedprice "
        "FROM lineitem l "
        "JOIN ("
        + part_keys
        + ") p ON p.p_partkey = l.l_partkey"
    )
    thresholds_decimal = (
        "SELECT l_partkey, 0.2 * avg(l_quantity) AS threshold_dec "
        "FROM lineitem "
        "GROUP BY l_partkey"
    )
    thresholds_double = (
        "SELECT l_partkey, 0.2 * avg(CAST(l_quantity AS DOUBLE)) AS threshold_dbl "
        "FROM lineitem "
        "GROUP BY l_partkey"
    )
    thresholds_double_round3 = (
        "SELECT "
        "  l_partkey, "
        "  CAST(CAST(0.2 * avg(CAST(l_quantity AS DOUBLE)) AS DECIMAL(16, 3)) AS DOUBLE) "
        "    AS threshold_dbl_round3 "
        "FROM lineitem "
        "GROUP BY l_partkey"
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 01 - part filter (brand/container) key count",
        "SELECT count(*) AS part_key_count FROM (" + part_keys + ") q17_part_keys",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 02 - join input rows after part filter",
        "SELECT "
        "  count(*) AS join_rows, "
        "  sum(l_extendedprice) AS join_sum_extendedprice "
        "FROM (" + lineitem_filtered + ") q17_lineitem_filtered",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 03 - per-part threshold rows (decimal path)",
        "SELECT count(*) AS threshold_rows_dec "
        "FROM (" + thresholds_decimal + ") q17_thresholds_dec",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 04 - per-part threshold rows (DuckDB control: double)",
        "SELECT count(*) AS threshold_rows_dbl "
        "FROM (" + thresholds_double + ") q17_thresholds_dbl",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 05 - aggregate type check on filtered input",
        "SELECT "
        "  avg(l_quantity) AS avg_q_dec, "
        "  typeof(avg(l_quantity)) AS avg_q_dec_type, "
        "  avg(CAST(l_quantity AS DOUBLE)) AS avg_q_dbl, "
        "  typeof(avg(CAST(l_quantity AS DOUBLE))) AS avg_q_dbl_type "
        "FROM (" + lineitem_filtered + ") q17_lineitem_filtered",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 06 - threshold sample per part (dec vs dbl vs dbl_round3)",
        "WITH td AS ("
        + thresholds_decimal
        + "), tb AS ("
        + thresholds_double
        + "), tr AS ("
        + thresholds_double_round3
        + "), pk AS ("
        + part_keys
        + ") "
        "SELECT "
        "  pk.p_partkey, "
        "  td.threshold_dec, "
        "  tb.threshold_dbl, "
        "  tr.threshold_dbl_round3 "
        "FROM pk "
        "JOIN td ON td.l_partkey = pk.p_partkey "
        "JOIN tb ON tb.l_partkey = pk.p_partkey "
        "JOIN tr ON tr.l_partkey = pk.p_partkey "
        "ORDER BY pk.p_partkey "
        "LIMIT 25",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 07 - predicate application row counts (dec vs dbl vs dbl_round3)",
        "WITH lf AS ("
        + lineitem_filtered
        + "), td AS ("
        + thresholds_decimal
        + "), tb AS ("
        + thresholds_double
        + "), tr AS ("
        + thresholds_double_round3
        + "), paired AS ( "
        "  SELECT "
        "    lf.l_partkey, "
        "    lf.l_quantity, "
        "    lf.l_extendedprice, "
        "    td.threshold_dec, "
        "    tb.threshold_dbl, "
        "    tr.threshold_dbl_round3 "
        "  FROM lf "
        "  JOIN td ON td.l_partkey = lf.l_partkey "
        "  JOIN tb ON tb.l_partkey = lf.l_partkey "
        "  JOIN tr ON tr.l_partkey = lf.l_partkey "
        ") "
        "SELECT "
        "  count(*) AS input_rows, "
        "  sum(CASE WHEN l_quantity < threshold_dec THEN 1 ELSE 0 END) AS rows_dec_pred, "
        "  sum(CASE WHEN CAST(l_quantity AS DOUBLE) < threshold_dbl THEN 1 ELSE 0 END) AS rows_dbl_pred, "
        "  sum(CASE WHEN CAST(l_quantity AS DOUBLE) < threshold_dbl_round3 THEN 1 ELSE 0 END) AS rows_dbl_round3_pred, "
        "  sum(CASE WHEN l_quantity < threshold_dec AND NOT (CAST(l_quantity AS DOUBLE) < threshold_dbl) THEN 1 ELSE 0 END) AS dec_only_rows, "
        "  sum(CASE WHEN NOT (l_quantity < threshold_dec) AND CAST(l_quantity AS DOUBLE) < threshold_dbl THEN 1 ELSE 0 END) AS dbl_only_rows "
        "FROM paired",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 08 - predicate mismatch sample rows (decimal vs double thresholds)",
        "WITH lf AS ("
        + lineitem_filtered
        + "), td AS ("
        + thresholds_decimal
        + "), tb AS ("
        + thresholds_double
        + "), tr AS ("
        + thresholds_double_round3
        + "), paired AS ( "
        "  SELECT "
        "    lf.l_partkey, "
        "    lf.l_quantity, "
        "    td.threshold_dec, "
        "    tb.threshold_dbl, "
        "    tr.threshold_dbl_round3 "
        "  FROM lf "
        "  JOIN td ON td.l_partkey = lf.l_partkey "
        "  JOIN tb ON tb.l_partkey = lf.l_partkey "
        "  JOIN tr ON tr.l_partkey = lf.l_partkey "
        ") "
        "SELECT "
        "  l_partkey, "
        "  l_quantity, "
        "  threshold_dec, "
        "  threshold_dbl, "
        "  threshold_dbl_round3, "
        "  CAST(l_quantity AS DOUBLE) - threshold_dbl AS delta_dbl "
        "FROM paired "
        "WHERE (l_quantity < threshold_dec AND NOT (CAST(l_quantity AS DOUBLE) < threshold_dbl)) "
        "   OR (NOT (l_quantity < threshold_dec) AND CAST(l_quantity AS DOUBLE) < threshold_dbl) "
        "ORDER BY l_partkey, l_quantity "
        "LIMIT 25",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 09 - qualifying aggregate (original decimal threshold path)",
        "WITH lf AS ("
        + lineitem_filtered
        + "), td AS ("
        + thresholds_decimal
        + ") "
        "SELECT "
        "  count(*) AS qualifying_rows_dec, "
        "  sum(lf.l_extendedprice) AS sum_extendedprice_dec, "
        "  sum(lf.l_extendedprice) / 7.0 AS avg_yearly_dec "
        "FROM lf "
        "JOIN td ON td.l_partkey = lf.l_partkey "
        "WHERE lf.l_quantity < td.threshold_dec",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 10 - qualifying aggregate (DuckDB control: double threshold)",
        "WITH lf AS ("
        + lineitem_filtered
        + "), tb AS ("
        + thresholds_double
        + ") "
        "SELECT "
        "  count(*) AS qualifying_rows_dbl, "
        "  sum(lf.l_extendedprice) AS sum_extendedprice_dbl, "
        "  sum(lf.l_extendedprice) / 7.0 AS avg_yearly_dbl "
        "FROM lf "
        "JOIN tb ON tb.l_partkey = lf.l_partkey "
        "WHERE CAST(lf.l_quantity AS DOUBLE) < tb.threshold_dbl",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 11 - qualifying aggregate (double threshold rounded to decimal(16,3))",
        "WITH lf AS ("
        + lineitem_filtered
        + "), tr AS ("
        + thresholds_double_round3
        + ") "
        "SELECT "
        "  count(*) AS qualifying_rows_dbl_round3, "
        "  sum(lf.l_extendedprice) AS sum_extendedprice_dbl_round3, "
        "  sum(lf.l_extendedprice) / 7.0 AS avg_yearly_dbl_round3 "
        "FROM lf "
        "JOIN tr ON tr.l_partkey = lf.l_partkey "
        "WHERE CAST(lf.l_quantity AS DOUBLE) < tr.threshold_dbl_round3",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 12 - final query shape rewritten with decimal thresholds",
        "WITH thresholds AS ( "
        "  SELECT l_partkey, 0.2 * avg(l_quantity) AS threshold "
        "  FROM lineitem "
        "  GROUP BY l_partkey "
        ") "
        "SELECT "
        "  count(*) AS qualifying_rows, "
        "  sum(l.l_extendedprice) AS sum_extendedprice, "
        "  sum(l.l_extendedprice) / 7.0 AS avg_yearly "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        "JOIN thresholds t ON t.l_partkey = l.l_partkey "
        "WHERE p.p_brand = 'Brand#23' "
        "  AND p.p_container = 'MED BOX' "
        "  AND l.l_quantity < t.threshold",
    )
    _append_debug_query(
        lines,
        presto_cursor,
        "Q17 step 13 - final query shape rewritten with double-threshold control",
        "SELECT "
        "  count(*) AS qualifying_rows, "
        "  sum(l.l_extendedprice) AS sum_extendedprice, "
        "  sum(l.l_extendedprice) / 7.0 AS avg_yearly "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        "WHERE p.p_brand = 'Brand#23' "
        "  AND p.p_container = 'MED BOX' "
        "  AND CAST(l.l_quantity AS DOUBLE) < ( "
        "    SELECT 0.2 * avg(CAST(li.l_quantity AS DOUBLE)) "
        "    FROM lineitem li "
        "    WHERE li.l_partkey = p.p_partkey "
        "  )",
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

    if query_id == "Q3":
        lines.append(_debug_q3_mismatch(presto_cursor))
    elif query_id == "Q17":
        lines.append(_debug_q17_mismatch(presto_cursor))
    elif query_id == "Q18":
        lines.append(_debug_q18_mismatch(presto_cursor))
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
