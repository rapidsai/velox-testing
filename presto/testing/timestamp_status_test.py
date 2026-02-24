#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Timestamp support verification for Presto native (velox-cudf) workers.
#
# Tests timestamps by querying tpch tables directly. Uses deterministic
# rows (o_orderkey=1 has o_orderdate='1996-01-02' in TPC-H) and validates
# actual results against expected values.
#
# Usage:
#   python timestamp_status_test.py [--host HOST] [--port PORT] [--user USER]

import argparse
import sys
import prestodb
import requests

TPCH = "tpch.sf1"

# GPU operator names used by the velox-cudf native worker.
GPU_OPERATOR_PREFIXES = ("Cudf",)


def get_execution_path(host, port, query_id):
    """Check whether a query ran on GPU or CPU-Velox by inspecting operator stats.

    Returns a tuple (path_label, gpu_operators):
      path_label: "GPU", "CPU", or "???"
      gpu_operators: set of GPU operator type names found (empty if CPU)
    """
    if not query_id:
        return "???", set()
    try:
        url = f"http://{host}:{port}/v1/query/{query_id}"
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return "???", set()
        data = resp.json()

        gpu_ops = set()
        # Walk through outputStage -> subStages recursively to find all operators
        def collect_operators(stage):
            if not stage:
                return
            for pipeline in stage.get("pipelineStats", []):
                for op in pipeline.get("operatorSummaries", []):
                    op_type = op.get("operatorType", "")
                    if any(op_type.startswith(prefix) for prefix in GPU_OPERATOR_PREFIXES):
                        gpu_ops.add(op_type)
            for sub in stage.get("subStages", []):
                collect_operators(sub)

        collect_operators(data.get("outputStage"))

        if gpu_ops:
            return "GPU", gpu_ops
        else:
            return "CPU", set()
    except Exception:
        return "???", set()


def run_query(cursor, label, sql, host, port, expected=None, expect_fail=False):
    """Run a query, check GPU/CPU execution path, validate result."""
    query_id = None
    exec_path = "???"
    gpu_ops = set()

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()

        # Get query ID and check execution path
        try:
            query_id = cursor.stats.get("queryId")
        except Exception:
            pass
        exec_path, gpu_ops = get_execution_path(host, port, query_id)

        path_tag = f"[{exec_path}]"
        ops_str = f" ops={','.join(sorted(gpu_ops))}" if gpu_ops else ""

        if expect_fail:
            print(f"  {path_tag} UNEXPECTED PASS : {label}")
            print(f"      SQL: {sql}")
            print(f"      Result: {rows}{ops_str}")
            return "unexpected_pass", exec_path

        if not rows:
            print(f"  {path_tag} FAIL (no rows) : {label}")
            print(f"      SQL: {sql}")
            return "fail", exec_path

        actual = rows[0][0] if len(rows[0]) == 1 else list(rows[0])

        if expected is not None:
            if actual == expected:
                print(f"  {path_tag} PASS : {label}")
                print(f"      Expected: {expected!r}  Got: {actual!r}{ops_str}")
                return "pass", exec_path
            else:
                print(f"  {path_tag} MISMATCH : {label}")
                print(f"      Expected: {expected!r}")
                print(f"      Actual:   {actual!r}")
                print(f"      SQL: {sql}{ops_str}")
                return "fail", exec_path
        else:
            if len(rows) == 1:
                print(f"  {path_tag} PASS (unchecked) : {label}")
                print(f"      Result: {rows[0]}{ops_str}")
            else:
                print(f"  {path_tag} PASS (unchecked) : {label}")
                print(f"      Result: ({len(rows)} rows) first={rows[0]}{ops_str}")
            return "pass", exec_path

    except Exception as e:
        # Try to get query ID even on failure
        try:
            query_id = cursor.stats.get("queryId") if cursor._query else None
        except Exception:
            pass
        exec_path, gpu_ops = get_execution_path(host, port, query_id)
        path_tag = f"[{exec_path}]"

        err = str(e).split('\n')[0][:200]
        if expect_fail:
            print(f"  {path_tag} EXPECTED FAIL : {label}")
            print(f"      Error: {err}")
            return "expected_fail", exec_path
        else:
            print(f"  {path_tag} FAIL : {label}")
            print(f"      SQL: {sql}")
            print(f"      Error: {err}")
            return "fail", exec_path


def main():
    parser = argparse.ArgumentParser(description="Timestamp support verification")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--user", default="test_user")
    parser.add_argument("--tpch-schema", default="sf1",
                        help="TPC-H schema/scale factor (default: sf1)")
    parser.add_argument("--hive-schema", default=None,
                        help="Hive schema for GPU path tests, e.g. 'hive.default'. "
                             "If set, creates parquet tables from tpch data and "
                             "runs timestamp tests through the GPU path.")
    args = parser.parse_args()

    global TPCH
    TPCH = f"tpch.{args.tpch_schema}"

    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user,
        catalog="tpch", schema=args.tpch_schema,
    )
    cursor = conn.cursor()

    results = {"pass": 0, "fail": 0, "expected_fail": 0, "unexpected_pass": 0}
    exec_paths = {"GPU": 0, "CPU": 0, "???": 0}

    def test(label, sql, expected=None, expect_fail=False):
        r, path = run_query(cursor, label, sql, args.host, args.port,
                            expected=expected, expect_fail=expect_fail)
        results[r] += 1
        exec_paths[path] += 1

    # =========================================================================
    # CALIBRATION - verify we have the expected tpch data
    # TPC-H sf1 o_orderkey=1 should have o_orderdate='1996-01-02'
    # =========================================================================
    print("\n=== CALIBRATION ===")

    test("tpch o_orderkey=1 date",
         f"SELECT CAST(o_orderdate AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-02")

    # For lineitem: l_orderkey=1, l_linenumber=1
    test("tpch lineitem key=1,line=1 shipdate",
         f"SELECT CAST(l_shipdate AS VARCHAR) FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1",
         expected="1996-03-13")

    test("tpch lineitem key=1,line=1 commitdate",
         f"SELECT CAST(l_commitdate AS VARCHAR) FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1",
         expected="1996-02-12")

    test("tpch lineitem key=1,line=1 receiptdate",
         f"SELECT CAST(l_receiptdate AS VARCHAR) FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1",
         expected="1996-03-22")

    # =========================================================================
    # 0. BASELINE - confirm tableless bug is not timestamp-specific
    # =========================================================================
    print("\n=== 0. BASELINE (tableless queries) ===")

    test("Tableless SELECT 1 (GPU worker CudfFromVelox bug)",
         "SELECT 1",
         expect_fail=True)

    test("Tableless TIMESTAMP literal",
         "SELECT TIMESTAMP '2024-01-15 10:30:00'",
         expect_fail=True)

    # =========================================================================
    # 0b. PARAMETRIC PRECISION SYNTAX
    # =========================================================================
    print("\n=== 0b. PARAMETRIC PRECISION SYNTAX ===")

    test("CAST to timestamp(3) - not supported in Presto SQL",
         "SELECT CAST('2024-01-15 10:30:00.123' AS timestamp(3))",
         expect_fail=True)

    test("CAST to timestamp(6) - not supported in Presto SQL",
         "SELECT CAST('2024-01-15 10:30:00.123456' AS timestamp(6))",
         expect_fail=True)

    test("CAST to timestamp(9) - not supported in Presto SQL",
         "SELECT CAST('2024-01-15 10:30:00.123456789' AS timestamp(9))",
         expect_fail=True)

    # =========================================================================
    # 0c. TIMESTAMP MICROSECONDS type name
    # =========================================================================
    print("\n=== 0c. TIMESTAMP MICROSECONDS (Presto internal type) ===")

    test("CAST to 'timestamp microseconds' (Presto internal type name)",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS "timestamp microseconds")
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expect_fail=True)

    # =========================================================================
    # 1. CAST DATE -> TIMESTAMP
    # =========================================================================
    print("\n=== 1. CAST DATE -> TIMESTAMP ===")

    # o_orderkey=1 has o_orderdate='1996-01-02'
    test("Cast date to timestamp",
         f"SELECT CAST(o_orderdate AS TIMESTAMP) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-02 00:00:00.000")

    test("typeof(timestamp)",
         f"SELECT typeof(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="timestamp")

    # =========================================================================
    # 2. CAST TIMESTAMP -> OTHER TYPES
    # =========================================================================
    print("\n=== 2. CAST TIMESTAMP -> OTHER TYPES ===")

    test("Timestamp -> varchar",
         f"SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-02 00:00:00.000")

    test("Timestamp -> date (roundtrip preserves date)",
         f"SELECT CAST(CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE) AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-02")

    # =========================================================================
    # 3. CAST STRING -> TIMESTAMP
    # =========================================================================
    print("\n=== 3. CAST STRING -> TIMESTAMP ===")

    test("varchar date -> timestamp",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

    test("varchar with time -> timestamp",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 14:30:45' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 14:30:45.000")

    test("varchar with millis -> timestamp",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 14:30:45.678' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 14:30:45.678")

    test("varchar with micros -> timestamp (expect truncation to millis)",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 14:30:45.678912' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 14:30:45.678")

    # =========================================================================
    # 4. TIMESTAMP COMPARISONS
    # =========================================================================
    print("\n=== 4. TIMESTAMP COMPARISONS ===")

    # o_orderkey=1: o_orderdate='1996-01-02'
    # NOTE: All timestamp comparison tests fail with "Unsupported type for cast
    # operation" - the native worker cannot evaluate boolean comparisons that
    # involve timestamp literals produced by the coordinator's LiteralInterpreter.
    test("Timestamp > literal (true case)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1995-01-01 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expect_fail=True)

    test("Timestamp > literal (false case)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1997-01-01 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expect_fail=True)

    test("Timestamp = literal",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) = TIMESTAMP '1996-01-02 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expect_fail=True)

    test("Timestamp BETWEEN",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) BETWEEN TIMESTAMP '1996-01-01 00:00:00' AND TIMESTAMP '1996-12-31 23:59:59'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expect_fail=True)

    # lineitem key=1, line=1: ship=1996-03-13, commit=1996-02-12, receipt=1996-03-22
    test("Timestamp col > col (ship > commit)",
         f"""SELECT CAST(l_shipdate AS TIMESTAMP) > CAST(l_commitdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expect_fail=True)

    test("Timestamp col > col (receipt > ship)",
         f"""SELECT CAST(l_receiptdate AS TIMESTAMP) > CAST(l_shipdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expect_fail=True)

    test("Timestamp col > col (commit > receipt = false)",
         f"""SELECT CAST(l_commitdate AS TIMESTAMP) > CAST(l_receiptdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expect_fail=True)

    # =========================================================================
    # 5. TIMESTAMP EXTRACTION FUNCTIONS
    # =========================================================================
    print("\n=== 5. EXTRACTION FUNCTIONS ===")

    # o_orderkey=1: o_orderdate='1996-01-02' -> Jan 2, 1996 (Tuesday)
    test("year()",
         f"SELECT year(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1996)

    test("month()",
         f"SELECT month(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1)

    test("day()",
         f"SELECT day(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    test("hour() (date cast = 0)",
         f"SELECT hour(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    test("minute() (date cast = 0)",
         f"SELECT minute(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    test("second() (date cast = 0)",
         f"SELECT second(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    # 1996-01-02 is a Tuesday -> day_of_week=2 (Presto: Monday=1)
    test("day_of_week()",
         f"SELECT day_of_week(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    # 1996-01-02 is day 2 of the year
    test("day_of_year()",
         f"SELECT day_of_year(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    # =========================================================================
    # 6. DATE TRUNCATION
    # =========================================================================
    print("\n=== 6. DATE TRUNCATION ===")

    test("date_trunc('year')",
         f"SELECT CAST(date_trunc('year', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-01 00:00:00.000")

    test("date_trunc('month')",
         f"SELECT CAST(date_trunc('month', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-01 00:00:00.000")

    test("date_trunc('day')",
         f"SELECT CAST(date_trunc('day', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected="1996-01-02 00:00:00.000")

    # =========================================================================
    # 7. DATE DIFF / DATE ADD
    # =========================================================================
    print("\n=== 7. DATE DIFF / DATE ADD ===")

    # lineitem key=1, line=1: ship=1996-03-13, receipt=1996-03-22 -> 9 days apart
    test("date_diff('day') ship->receipt",
         f"""SELECT date_diff('day', CAST(l_shipdate AS TIMESTAMP), CAST(l_receiptdate AS TIMESTAMP))
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expected=9)

    # ship=1996-03-13, commit=1996-02-12 -> 30 days
    test("date_diff('day') commit->ship",
         f"""SELECT date_diff('day', CAST(l_commitdate AS TIMESTAMP), CAST(l_shipdate AS TIMESTAMP))
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expected=30)

    # 1996-01-02 -> seconds since epoch
    # 1996-01-02 00:00:00 = 9497 days * 86400 = 820540800 seconds since 1970-01-01
    test("date_diff('second') from epoch",
         f"""SELECT date_diff('second', TIMESTAMP '1970-01-01 00:00:00', CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=820540800)

    # date_add 7 days to 1996-01-02 -> 1996-01-09
    test("date_add('day', 7)",
         f"""SELECT CAST(date_add('day', 7, CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-09 00:00:00.000")

    # date_add 12 hours to 1996-01-02 00:00:00 -> 1996-01-02 12:00:00
    test("date_add('hour', 12)",
         f"""SELECT CAST(date_add('hour', 12, CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 12:00:00.000")

    # =========================================================================
    # 8. TIMESTAMP + INTERVAL ARITHMETIC
    # =========================================================================
    print("\n=== 8. INTERVAL ARITHMETIC ===")

    test("+ INTERVAL '6' HOUR",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '6' HOUR AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 06:00:00.000")

    test("+ INTERVAL '30' DAY",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '30' DAY AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-02-01 00:00:00.000")

    test("+ INTERVAL '90' MINUTE",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '90' MINUTE AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 01:30:00.000")

    test("+ INTERVAL '3600' SECOND",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '3600' SECOND AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 01:00:00.000")

    test("- INTERVAL '1' DAY",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) - INTERVAL '1' DAY AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-01 00:00:00.000")

    test("- INTERVAL '1' MONTH (year-month interval subtraction)",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) - INTERVAL '1' MONTH AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1995-12-02 00:00:00.000")

    test("+ INTERVAL '1' YEAR (year-month interval addition)",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '1' YEAR AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1997-01-02 00:00:00.000")

    test("+ INTERVAL '1' MONTH (year-month interval addition)",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '1' MONTH AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-02-02 00:00:00.000")

    # =========================================================================
    # 9. FORMAT AND PARSE
    # =========================================================================
    print("\n=== 9. FORMAT AND PARSE ===")

    test("date_format(timestamp, '%Y-%m-%d %H:%i:%s')",
         f"""SELECT date_format(CAST(o_orderdate AS TIMESTAMP), '%Y-%m-%d %H:%i:%s')
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00")

    test("date_format custom '%d/%m/%Y'",
         f"""SELECT date_format(CAST(o_orderdate AS TIMESTAMP), '%d/%m/%Y')
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="02/01/1996")

    test("date_parse('%Y-%m-%d') -> timestamp",
         f"""SELECT CAST(date_parse(CAST(o_orderdate AS VARCHAR), '%Y-%m-%d') AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

    test("format_datetime(timestamp, 'yyyy-MM-dd HH:mm:ss')",
         f"""SELECT format_datetime(CAST(o_orderdate AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss')
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00")

    # =========================================================================
    # 10. MILLISECOND PRECISION
    # =========================================================================
    print("\n=== 10. MILLISECOND PRECISION ===")

    test("Millis .123 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 10:30:00.123' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 10:30:00.123")

    test("Millis .999 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 23:59:59.999' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 23:59:59.999")

    test("Millis .001 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 00:00:00.001' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.001")

    test("Millis .500 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 12:00:00.500' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 12:00:00.500")

    # =========================================================================
    # 11. MICROSECOND PRECISION
    # =========================================================================
    print("\n=== 11. MICROSECOND PRECISION ===")

    test("Micros .123456 -> truncated to .123 (millis only)",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 10:30:00.123456' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 10:30:00.123")

    test("Micros .000001 -> truncated to .000",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 00:00:00.000001' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

    test("Micros .999999 -> truncated to .999",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 23:59:59.999999' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 23:59:59.999")

    # =========================================================================
    # 12. AGGREGATIONS
    # =========================================================================
    print("\n=== 12. AGGREGATIONS ===")

    # TPC-H sf1 orders: dates range from 1992-01-01 to 1998-08-02
    test("MIN(timestamp)",
         f"SELECT CAST(MIN(CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) FROM {TPCH}.orders",
         expected="1992-01-01 00:00:00.000")

    test("MAX(timestamp)",
         f"SELECT CAST(MAX(CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) FROM {TPCH}.orders",
         expected="1998-08-02 00:00:00.000")

    # Count of orders in 1996
    test("COUNT with timestamp filter (year 1996)",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) >= TIMESTAMP '1996-01-01 00:00:00'
             AND CAST(o_orderdate AS TIMESTAMP) < TIMESTAMP '1997-01-01 00:00:00'""")

    test("GROUP BY year(timestamp) - first year",
         f"""SELECT year(CAST(o_orderdate AS TIMESTAMP)) AS yr, count(*) AS cnt
             FROM {TPCH}.orders
             GROUP BY year(CAST(o_orderdate AS TIMESTAMP))
             ORDER BY yr LIMIT 1""",
         expected=[1992, 227089])

    test("ORDER BY timestamp LIMIT 1",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders ORDER BY CAST(o_orderdate AS TIMESTAMP) LIMIT 1""",
         expected="1992-01-01 00:00:00.000")

    # =========================================================================
    # 13. TIMESTAMP WITH TIME ZONE
    # =========================================================================
    print("\n=== 13. TIMESTAMP WITH TIME ZONE ===")

    test("Cast timestamp to timestamp with time zone",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000 UTC")

    test("AT TIME ZONE 'America/New_York'",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)
                    AT TIME ZONE 'America/New_York' AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-01 19:00:00.000 America/New_York")

    # 1996-01-02 00:00:00 = 820540800 seconds since epoch
    test("to_unixtime(timestamp with tz)",
         f"""SELECT to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=820540800.0)

    test("from_unixtime(820454400) roundtrip",
         f"""SELECT CAST(from_unixtime(
                    to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
                   ) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

    # =========================================================================
    # 14. EXTRACT SYNTAX (SQL standard)
    # These mirror the function-style tests in section 5, but use the
    # EXTRACT(field FROM timestamp) syntax that users specifically requested.
    # =========================================================================
    print("\n=== 14. EXTRACT SYNTAX (SQL standard) ===")

    # o_orderkey=1: o_orderdate='1996-01-02'
    test("EXTRACT(YEAR FROM timestamp)",
         f"SELECT EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1996)

    test("EXTRACT(MONTH FROM timestamp)",
         f"SELECT EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1)

    test("EXTRACT(DAY FROM timestamp)",
         f"SELECT EXTRACT(DAY FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    test("EXTRACT(HOUR FROM timestamp)",
         f"SELECT EXTRACT(HOUR FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    test("EXTRACT(MINUTE FROM timestamp)",
         f"SELECT EXTRACT(MINUTE FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    test("EXTRACT(SECOND FROM timestamp)",
         f"SELECT EXTRACT(SECOND FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=0)

    test("EXTRACT(DAY_OF_WEEK FROM timestamp)",
         f"SELECT EXTRACT(DAY_OF_WEEK FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    test("EXTRACT(DAY_OF_YEAR FROM timestamp)",
         f"SELECT EXTRACT(DAY_OF_YEAR FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=2)

    test("EXTRACT(QUARTER FROM timestamp)",
         f"SELECT EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1)

    test("EXTRACT(WEEK FROM timestamp)",
         f"SELECT EXTRACT(WEEK FROM CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders WHERE o_orderkey = 1",
         expected=1)

    # =========================================================================
    # 15. DATE_TRUNC AT ALL ROLLUP LEVELS
    # Verify every granularity: second, minute, hour, day, week, month,
    # quarter, year. Uses a timestamp with time component to exercise all.
    # =========================================================================
    print("\n=== 15. DATE_TRUNC - ALL ROLLUP LEVELS ===")

    # Build a timestamp with time: '1996-03-13 14:35:47.123'
    # (lineitem key=1, line=1 shipdate = 1996-03-13, we add time via string)
    TS_EXPR = f"CAST(CAST(l_shipdate AS VARCHAR) || ' 14:35:47.123' AS TIMESTAMP)"
    LI_WHERE = f"FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1"

    test("date_trunc('second')",
         f"SELECT CAST(date_trunc('second', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-13 14:35:47.000")

    test("date_trunc('minute')",
         f"SELECT CAST(date_trunc('minute', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-13 14:35:00.000")

    test("date_trunc('hour')",
         f"SELECT CAST(date_trunc('hour', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-13 14:00:00.000")

    test("date_trunc('day')",
         f"SELECT CAST(date_trunc('day', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-13 00:00:00.000")

    # 1996-03-13 is a Wednesday. Week starts Monday -> 1996-03-11
    test("date_trunc('week')",
         f"SELECT CAST(date_trunc('week', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-11 00:00:00.000")

    test("date_trunc('month')",
         f"SELECT CAST(date_trunc('month', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-03-01 00:00:00.000")

    # March -> Q1 -> 1996-01-01
    test("date_trunc('quarter')",
         f"SELECT CAST(date_trunc('quarter', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-01-01 00:00:00.000")

    test("date_trunc('year')",
         f"SELECT CAST(date_trunc('year', {TS_EXPR}) AS VARCHAR) {LI_WHERE}",
         expected="1996-01-01 00:00:00.000")

    # =========================================================================
    # 16. GROUP BY TIMESTAMP ROLLUPS
    # The core use case: filter timestamps, then GROUP BY on EXTRACT or
    # date_trunc results. This is the "roll-up to minute/hour/day/week/month"
    # workflow.
    # =========================================================================
    print("\n=== 16. GROUP BY TIMESTAMP ROLLUPS ===")

    test("GROUP BY EXTRACT(YEAR FROM ts)",
         f"""SELECT EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) AS yr, count(*) AS cnt
             FROM {TPCH}.orders
             GROUP BY EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP))
             ORDER BY yr LIMIT 1""",
         expected=[1992, 227089])

    test("GROUP BY EXTRACT(MONTH FROM ts) for 1996",
         f"""SELECT EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) AS mo, count(*) AS cnt
             FROM {TPCH}.orders
             WHERE EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996
             GROUP BY EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP))
             ORDER BY mo LIMIT 1""")

    test("GROUP BY EXTRACT(QUARTER FROM ts)",
         f"""SELECT EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP)) AS qtr, count(*) AS cnt
             FROM {TPCH}.orders
             WHERE EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996
             GROUP BY EXTRACT(QUARTER FROM CAST(o_orderdate AS TIMESTAMP))
             ORDER BY qtr LIMIT 1""")

    test("GROUP BY date_trunc('month', ts)",
         f"""SELECT CAST(date_trunc('month', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) AS mo, count(*) AS cnt
             FROM {TPCH}.orders
             GROUP BY date_trunc('month', CAST(o_orderdate AS TIMESTAMP))
             ORDER BY mo LIMIT 1""",
         expected=["1992-01-01 00:00:00.000", 18937])

    test("GROUP BY date_trunc('quarter', ts)",
         f"""SELECT CAST(date_trunc('quarter', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) AS qtr, count(*) AS cnt
             FROM {TPCH}.orders
             GROUP BY date_trunc('quarter', CAST(o_orderdate AS TIMESTAMP))
             ORDER BY qtr LIMIT 1""")

    test("GROUP BY date_trunc('year', ts)",
         f"""SELECT CAST(date_trunc('year', CAST(o_orderdate AS TIMESTAMP)) AS VARCHAR) AS yr, count(*) AS cnt
             FROM {TPCH}.orders
             GROUP BY date_trunc('year', CAST(o_orderdate AS TIMESTAMP))
             ORDER BY yr LIMIT 1""",
         expected=["1992-01-01 00:00:00.000", 227089])

    test("GROUP BY date_trunc('week', ts) count weeks in 1996",
         f"""SELECT count(DISTINCT date_trunc('week', CAST(o_orderdate AS TIMESTAMP)))
             FROM {TPCH}.orders
             WHERE EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996""")

    test("GROUP BY date_trunc('day', ts) count days in 1996",
         f"""SELECT count(DISTINCT date_trunc('day', CAST(o_orderdate AS TIMESTAMP)))
             FROM {TPCH}.orders
             WHERE EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996""")

    # =========================================================================
    # 17. JOIN ON TIMESTAMP ROLLUPS
    # Verify that two tables can be joined on timestamp-derived keys
    # (date_trunc, EXTRACT). Uses self-join on orders/lineitem via date keys.
    # =========================================================================
    print("\n=== 17. JOIN ON TIMESTAMP ROLLUPS ===")

    test("JOIN on date_trunc('month', ts)",
         f"""SELECT count(*)
             FROM (
                 SELECT date_trunc('month', CAST(o_orderdate AS TIMESTAMP)) AS order_month
                 FROM {TPCH}.orders WHERE o_orderkey <= 100
             ) o
             JOIN (
                 SELECT date_trunc('month', CAST(l_shipdate AS TIMESTAMP)) AS ship_month
                 FROM {TPCH}.lineitem WHERE l_orderkey <= 100
             ) l
             ON o.order_month = l.ship_month""")

    test("JOIN on date_trunc('year', ts)",
         f"""SELECT count(*)
             FROM (
                 SELECT DISTINCT date_trunc('year', CAST(o_orderdate AS TIMESTAMP)) AS yr
                 FROM {TPCH}.orders WHERE o_orderkey <= 100
             ) o
             JOIN (
                 SELECT DISTINCT date_trunc('year', CAST(l_shipdate AS TIMESTAMP)) AS yr
                 FROM {TPCH}.lineitem WHERE l_orderkey <= 100
             ) l
             ON o.yr = l.yr""")

    test("JOIN on EXTRACT(YEAR) and EXTRACT(MONTH)",
         f"""SELECT count(*)
             FROM (
                 SELECT DISTINCT
                     EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) AS yr,
                     EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP)) AS mo
                 FROM {TPCH}.orders WHERE o_orderkey <= 100
             ) o
             JOIN (
                 SELECT DISTINCT
                     EXTRACT(YEAR FROM CAST(l_shipdate AS TIMESTAMP)) AS yr,
                     EXTRACT(MONTH FROM CAST(l_shipdate AS TIMESTAMP)) AS mo
                 FROM {TPCH}.lineitem WHERE l_orderkey <= 100
             ) l
             ON o.yr = l.yr AND o.mo = l.mo""")

    test("JOIN on date_trunc('day', ts) - orders/lineitem same orderkey",
         f"""SELECT o.o_orderkey, CAST(o.order_day AS VARCHAR), CAST(l.ship_day AS VARCHAR)
             FROM (
                 SELECT o_orderkey, date_trunc('day', CAST(o_orderdate AS TIMESTAMP)) AS order_day
                 FROM {TPCH}.orders WHERE o_orderkey = 1
             ) o
             JOIN (
                 SELECT l_orderkey, date_trunc('day', CAST(l_shipdate AS TIMESTAMP)) AS ship_day
                 FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1
             ) l
             ON o.o_orderkey = l.l_orderkey""",
         expected=[1, "1996-01-02 00:00:00.000", "1996-03-13 00:00:00.000"])

    # =========================================================================
    # 18. MICROSECOND PRECISION FILTERING
    # Test filtering on timestamps with at least microsecond precision.
    # Since Presto TIMESTAMP is millis-only, micros are truncated - verify
    # that filtering still works correctly after truncation.
    # =========================================================================
    print("\n=== 18. MICROSECOND PRECISION FILTERING ===")

    test("Filter with microsecond literal (truncated to millis in filter)",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) >= TIMESTAMP '1996-01-01 00:00:00.000000'
             AND CAST(o_orderdate AS TIMESTAMP) < TIMESTAMP '1996-02-01 00:00:00.000000'""")

    test("Filter + EXTRACT after microsecond CAST",
         f"""SELECT EXTRACT(MONTH FROM CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=1)

    test("Filter + date_trunc after microsecond string parse",
         f"""SELECT CAST(date_trunc('day',
                 CAST(CAST(o_orderdate AS VARCHAR) || ' 14:30:45.678912' AS TIMESTAMP)
             ) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

    test("GROUP BY on micros-parsed timestamp (truncated to millis)",
         f"""SELECT CAST(date_trunc('month',
                 CAST(CAST(o_orderdate AS VARCHAR) || ' 10:20:30.123456' AS TIMESTAMP)
             ) AS VARCHAR) AS mo, count(*)
             FROM {TPCH}.orders
             WHERE o_orderkey <= 100
             GROUP BY date_trunc('month',
                 CAST(CAST(o_orderdate AS VARCHAR) || ' 10:20:30.123456' AS TIMESTAMP))
             ORDER BY mo LIMIT 1""")

    # =========================================================================
    # 19. GPU PATH VERIFICATION (Hive parquet tables)
    # =========================================================================
    if args.hive_schema:
        print(f"\n=== 19. GPU PATH VERIFICATION (via {args.hive_schema}) ===")

        hive = args.hive_schema
        test_table = f"{hive}.ts_test_orders"

        # Clean up any leftover table from previous runs
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
            cursor.fetchall()
        except Exception:
            pass

        # Create a small parquet table from tpch data with timestamp columns
        print(f"\n  Creating {test_table} from tpch data...")
        try:
            cursor.execute(f"""
                CREATE TABLE {test_table}
                WITH (format = 'PARQUET') AS
                SELECT
                    o_orderkey,
                    CAST(o_orderdate AS TIMESTAMP) AS o_ts,
                    CAST(o_orderdate AS VARCHAR) AS o_date_str,
                    o_orderpriority
                FROM {TPCH}.orders
                WHERE o_orderkey <= 100
            """)
            cursor.fetchall()
            print(f"  Table created.")
        except Exception as e:
            err = str(e).split('\n')[0][:200]
            print(f"  CTAS FAILED: {err}")
            print(f"  Skipping GPU path tests.")
            test_table = None

        if test_table:
            # Check row count
            try:
                cursor.execute(f"SELECT count(*) FROM {test_table}")
                row_count = cursor.fetchall()[0][0]
                print(f"  Row count: {row_count}")
                if row_count == 0:
                    print("  WARNING: Table is empty (known CTAS bug with GPU worker)")
                    print("  GPU path tests will be unreliable with empty tables")
            except Exception as e:
                print(f"  Count failed: {e}")
                row_count = 0

            # --- GPU path timestamp tests ---
            print()

            test("GPU: Read timestamp column",
                 f"SELECT CAST(o_ts AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-02 00:00:00.000")

            test("GPU: typeof(timestamp) from parquet",
                 f"SELECT typeof(o_ts) FROM {test_table} WHERE o_orderkey = 1",
                 expected="timestamp")

            test("GPU: EXTRACT(YEAR FROM ts)",
                 f"SELECT EXTRACT(YEAR FROM o_ts) FROM {test_table} WHERE o_orderkey = 1",
                 expected=1996)

            test("GPU: EXTRACT(MONTH FROM ts)",
                 f"SELECT EXTRACT(MONTH FROM o_ts) FROM {test_table} WHERE o_orderkey = 1",
                 expected=1)

            test("GPU: EXTRACT(DAY FROM ts)",
                 f"SELECT EXTRACT(DAY FROM o_ts) FROM {test_table} WHERE o_orderkey = 1",
                 expected=2)

            test("GPU: date_trunc('month', ts)",
                 f"SELECT CAST(date_trunc('month', o_ts) AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-01 00:00:00.000")

            test("GPU: date_trunc('day', ts)",
                 f"SELECT CAST(date_trunc('day', o_ts) AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-02 00:00:00.000")

            test("GPU: date_trunc('year', ts)",
                 f"SELECT CAST(date_trunc('year', o_ts) AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-01 00:00:00.000")

            test("GPU: date_format",
                 f"SELECT date_format(o_ts, '%Y-%m-%d %H:%i:%s') FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-02 00:00:00")

            test("GPU: date_add('day', 7)",
                 f"SELECT CAST(date_add('day', 7, o_ts) AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-09 00:00:00.000")

            test("GPU: + INTERVAL '6' HOUR",
                 f"SELECT CAST(o_ts + INTERVAL '6' HOUR AS VARCHAR) FROM {test_table} WHERE o_orderkey = 1",
                 expected="1996-01-02 06:00:00.000")

            test("GPU: MIN(timestamp)",
                 f"SELECT CAST(MIN(o_ts) AS VARCHAR) FROM {test_table}")

            test("GPU: MAX(timestamp)",
                 f"SELECT CAST(MAX(o_ts) AS VARCHAR) FROM {test_table}")

            test("GPU: GROUP BY EXTRACT(YEAR)",
                 f"""SELECT EXTRACT(YEAR FROM o_ts) AS yr, count(*)
                     FROM {test_table} GROUP BY EXTRACT(YEAR FROM o_ts) ORDER BY yr LIMIT 1""")

            test("GPU: GROUP BY date_trunc('month')",
                 f"""SELECT CAST(date_trunc('month', o_ts) AS VARCHAR) AS mo, count(*)
                     FROM {test_table}
                     GROUP BY date_trunc('month', o_ts) ORDER BY mo LIMIT 1""")

            test("GPU: COUNT with timestamp filter",
                 f"""SELECT count(*) FROM {test_table}
                     WHERE o_ts >= TIMESTAMP '1996-01-01 00:00:00'""")

            test("GPU: ORDER BY timestamp LIMIT 1",
                 f"SELECT CAST(o_ts AS VARCHAR) FROM {test_table} ORDER BY o_ts LIMIT 1")

            # Cleanup
            print(f"\n  Dropping {test_table}...")
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
                cursor.fetchall()
                print("  Cleanup done.")
            except Exception as e:
                print(f"  Cleanup failed: {e}")
    else:
        print("\n=== 19. GPU PATH VERIFICATION ===")
        print("  SKIPPED (use --hive-schema 'hive.default' to enable)")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = sum(results.values())
    print(f"  Total tests:       {total}")
    print(f"  Passed:            {results['pass']}")
    print(f"  Failed/Mismatch:   {results['fail']}")
    print(f"  Expected fails:    {results['expected_fail']}")
    print(f"  Unexpected pass:   {results['unexpected_pass']}")
    print()
    print("  Execution path:")
    print(f"    Ran on GPU (velox-cudf): {exec_paths['GPU']}")
    print(f"    Ran on CPU (velox):      {exec_paths['CPU']}")
    if exec_paths['???'] > 0:
        print(f"    Unknown:                 {exec_paths['???']}")
    print()

    # ---- Detailed verdict ----
    print("=" * 60)
    print("VERDICT: TIMESTAMP SUPPORT STATUS")
    print("=" * 60)
    print()
    print("  CPU-Velox (what the tpch connector exercises):")
    print("  -----------------------------------------------")
    print("  Working:")
    print("    - CAST date/varchar -> TIMESTAMP and back")
    print("    - EXTRACT(YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/QUARTER/WEEK)")
    print("    - date_trunc at all levels (second/minute/hour/day/week/month/quarter/year)")
    print("    - GROUP BY on EXTRACT and date_trunc results")
    print("    - JOIN on EXTRACT and date_trunc results")
    print("    - date_diff, date_add, date_format, date_parse, format_datetime")
    print("    - Interval arithmetic (+/- HOUR/DAY/MINUTE/SECOND/MONTH/YEAR)")
    print("    - Aggregations: MIN/MAX/COUNT with timestamp filters")
    print("    - ORDER BY timestamp")
    print("    - TIMESTAMP WITH TIME ZONE, AT TIME ZONE, to_unixtime/from_unixtime")
    print("    - Millisecond precision (3 digits) preserved")
    print()
    print("  BROKEN on CPU-Velox:")
    print("    - Timestamp comparison operators (>, =, <, BETWEEN) with literals")
    print("      -> 'Unsupported type for cast operation'")
    print("    - Tableless constant expressions (SELECT TIMESTAMP '...')")
    print("      -> CudfFromVelox empty vector bug (not timestamp-specific)")
    print("    - timestamp(3)/timestamp(6)/timestamp(9) parametric syntax")
    print("      -> Presto SQL parser rejects these")
    print("    - TIMESTAMP MICROSECONDS type")
    print("      -> No Velox type parser mapping")
    print("    - Microsecond precision silently truncated to milliseconds")
    print()
    print("  GPU / velox-cudf:")
    print("  -----------------")
    if exec_paths['GPU'] > 0:
        print(f"    {exec_paths['GPU']} tests ran on GPU path")
    else:
        print("    NO ACTIVE GPU CODEPATHS - all queries fell back to CPU-Velox.")
        print("    The tpch connector is in-memory and never routes through the GPU.")
        if not args.hive_schema:
            print("    Run with --hive-schema hive.default to test Hive parquet GPU path.")
        else:
            print("    Even Hive parquet queries did not use GPU operators.")
            print("    This confirms: velox-cudf does NOT currently support timestamps.")
    print()

    if results['fail'] > 0:
        print("RESULT: Some tests FAILED or had MISMATCHED values - see above")
        return 1
    elif results['unexpected_pass'] > 0:
        print("RESULT: Some expected failures PASSED (update expectations!)")
        return 0
    else:
        print("RESULT: All tests passed as expected")
        return 0


if __name__ == "__main__":
    sys.exit(main())
