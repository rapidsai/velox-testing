#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Timestamp support verification for Presto native (velox-cudf) workers.
#
# Tests timestamps by querying tpch tables directly (avoiding CTAS which
# produces empty tables due to CudfFromVelox empty-vector bug).
#
# Usage:
#   python timestamp_status_test.py [--host HOST] [--port PORT] [--user USER]
#                                   [--tpch-schema SCHEMA]

import argparse
import sys
import prestodb

# Use tpch connector tables directly - they have DATE columns we can cast to
# TIMESTAMP, and they have real data.
TPCH = "tpch.sf1"


def run_query(cursor, label, sql, expect_fail=False):
    """Run a single SQL query and report pass/fail."""
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        if expect_fail:
            print(f"  UNEXPECTED PASS : {label}")
            print(f"                    SQL: {sql}")
            print(f"                    Result: {rows}")
            return "unexpected_pass"
        else:
            if not rows or not rows[0]:
                print(f"  PASS (no rows) : {label}")
                print(f"         Result: <no rows returned>")
            elif len(rows) == 1:
                print(f"  PASS : {label}")
                print(f"         Result: {rows[0]}")
            else:
                print(f"  PASS : {label}")
                print(f"         Result: ({len(rows)} rows) first={rows[0]}")
            return "pass"
    except Exception as e:
        err = str(e).split('\n')[0][:200]
        if expect_fail:
            print(f"  EXPECTED FAIL : {label}")
            print(f"                  Error: {err}")
            return "expected_fail"
        else:
            print(f"  FAIL : {label}")
            print(f"         SQL: {sql}")
            print(f"         Error: {err}")
            return "fail"


def main():
    parser = argparse.ArgumentParser(description="Timestamp support verification")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--user", default="test_user")
    parser.add_argument("--tpch-schema", default="sf1",
                        help="TPC-H schema/scale factor to use (default: sf1)")
    args = parser.parse_args()

    global TPCH
    TPCH = f"tpch.{args.tpch_schema}"

    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user,
        catalog="tpch", schema=args.tpch_schema,
    )
    cursor = conn.cursor()

    results = {"pass": 0, "fail": 0, "expected_fail": 0, "unexpected_pass": 0}

    def test(label, sql, expect_fail=False):
        r = run_query(cursor, label, sql, expect_fail)
        results[r] += 1

    # =========================================================================
    # 0. BASELINE - sanity check tpch works, confirm tableless bug
    # =========================================================================
    print("\n=== 0. BASELINE ===")

    test("tpch sanity: SELECT count(*) FROM orders",
         f"SELECT count(*) FROM {TPCH}.orders")

    test("tpch sanity: SELECT o_orderdate FROM orders LIMIT 1",
         f"SELECT o_orderdate FROM {TPCH}.orders LIMIT 1")

    test("Tableless SELECT 1 (expect fail on GPU worker)",
         "SELECT 1",
         expect_fail=True)

    test("Tableless TIMESTAMP literal (expect fail on GPU worker)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00'",
         expect_fail=True)

    # =========================================================================
    # 0b. PARAMETRIC PRECISION SYNTAX (coordinator-level rejection)
    # =========================================================================
    print("\n=== 0b. PARAMETRIC PRECISION SYNTAX ===")

    test("CAST to timestamp(3)",
         "SELECT CAST('2024-01-15 10:30:00.123' AS timestamp(3))",
         expect_fail=True)

    test("CAST to timestamp(6)",
         "SELECT CAST('2024-01-15 10:30:00.123456' AS timestamp(6))",
         expect_fail=True)

    test("CAST to timestamp(9)",
         "SELECT CAST('2024-01-15 10:30:00.123456789' AS timestamp(9))",
         expect_fail=True)

    # =========================================================================
    # 1. CAST DATE -> TIMESTAMP (table-backed)
    # =========================================================================
    print("\n=== 1. CAST DATE -> TIMESTAMP ===")

    test("Cast date col to timestamp",
         f"SELECT CAST(o_orderdate AS TIMESTAMP) FROM {TPCH}.orders LIMIT 1")

    test("Cast date to timestamp - multiple rows",
         f"SELECT CAST(o_orderdate AS TIMESTAMP) FROM {TPCH}.orders LIMIT 5")

    test("typeof(CAST(date AS TIMESTAMP))",
         f"SELECT typeof(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    # =========================================================================
    # 2. CAST TIMESTAMP -> OTHER TYPES (table-backed)
    # =========================================================================
    print("\n=== 2. CAST TIMESTAMP -> OTHER TYPES ===")

    test("Cast timestamp to varchar",
         f"SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS VARCHAR) FROM {TPCH}.orders LIMIT 1")

    test("Cast timestamp to date (roundtrip)",
         f"SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE) FROM {TPCH}.orders LIMIT 1")

    test("Date->timestamp->varchar roundtrip",
         f"""SELECT o_orderdate,
                    CAST(o_orderdate AS TIMESTAMP),
                    CAST(CAST(o_orderdate AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 3""")

    # =========================================================================
    # 3. CAST STRING -> TIMESTAMP (table-backed via subquery)
    # =========================================================================
    print("\n=== 3. CAST STRING -> TIMESTAMP ===")

    test("Cast varchar to timestamp (from date col cast to varchar)",
         f"""SELECT CAST(date_str AS TIMESTAMP)
             FROM (SELECT CAST(o_orderdate AS VARCHAR) AS date_str FROM {TPCH}.orders LIMIT 1) t""")

    test("Cast varchar with concat to add time part",
         f"""SELECT CAST(date_str || ' 10:30:00' AS TIMESTAMP)
             FROM (SELECT CAST(o_orderdate AS VARCHAR) AS date_str FROM {TPCH}.orders LIMIT 1) t""")

    test("Cast varchar with millis",
         f"""SELECT CAST(date_str || ' 10:30:00.123' AS TIMESTAMP)
             FROM (SELECT CAST(o_orderdate AS VARCHAR) AS date_str FROM {TPCH}.orders LIMIT 1) t""")

    test("Cast varchar with micros",
         f"""SELECT CAST(date_str || ' 10:30:00.123456' AS TIMESTAMP)
             FROM (SELECT CAST(o_orderdate AS VARCHAR) AS date_str FROM {TPCH}.orders LIMIT 1) t""")

    # =========================================================================
    # 4. TIMESTAMP COMPARISONS (table-backed)
    # =========================================================================
    print("\n=== 4. TIMESTAMP COMPARISONS ===")

    test("Timestamp > literal",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1995-01-01 00:00:00'""")

    test("Timestamp < literal",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) < TIMESTAMP '1995-01-01 00:00:00'""")

    test("Timestamp = literal",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) = TIMESTAMP '1995-03-15 00:00:00'""")

    test("Timestamp BETWEEN literals",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) BETWEEN TIMESTAMP '1994-01-01 00:00:00' AND TIMESTAMP '1994-12-31 23:59:59'""")

    test("Timestamp col vs col (lineitem ship > commit)",
         f"""SELECT count(*) FROM {TPCH}.lineitem
             WHERE CAST(l_shipdate AS TIMESTAMP) > CAST(l_commitdate AS TIMESTAMP)""")

    test("Timestamp col vs col (lineitem receipt > ship)",
         f"""SELECT count(*) FROM {TPCH}.lineitem
             WHERE CAST(l_receiptdate AS TIMESTAMP) > CAST(l_shipdate AS TIMESTAMP)""")

    # =========================================================================
    # 5. TIMESTAMP EXTRACTION FUNCTIONS
    # =========================================================================
    print("\n=== 5. TIMESTAMP EXTRACTION FUNCTIONS ===")

    test("year(timestamp)",
         f"SELECT year(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("month(timestamp)",
         f"SELECT month(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("day(timestamp)",
         f"SELECT day(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("hour(timestamp) - from date (should be 0)",
         f"SELECT hour(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("minute(timestamp) - from date (should be 0)",
         f"SELECT minute(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("second(timestamp) - from date (should be 0)",
         f"SELECT second(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("day_of_week(timestamp)",
         f"SELECT day_of_week(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("day_of_year(timestamp)",
         f"SELECT day_of_year(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    # =========================================================================
    # 6. DATE TRUNCATION
    # =========================================================================
    print("\n=== 6. DATE TRUNCATION ===")

    test("date_trunc('year', timestamp)",
         f"SELECT date_trunc('year', CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("date_trunc('month', timestamp)",
         f"SELECT date_trunc('month', CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("date_trunc('day', timestamp)",
         f"SELECT date_trunc('day', CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    test("date_trunc('hour', timestamp)",
         f"SELECT date_trunc('hour', CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders LIMIT 1")

    # =========================================================================
    # 7. DATE DIFF / DATE ADD
    # =========================================================================
    print("\n=== 7. DATE DIFF / DATE ADD ===")

    test("date_diff('day') between timestamp cols",
         f"""SELECT date_diff('day',
                    CAST(l_shipdate AS TIMESTAMP),
                    CAST(l_receiptdate AS TIMESTAMP))
             FROM {TPCH}.lineitem LIMIT 1""")

    test("date_diff('second') between timestamp col and literal",
         f"""SELECT date_diff('second',
                    TIMESTAMP '1970-01-01 00:00:00',
                    CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders LIMIT 1""")

    test("date_diff('hour') between two timestamp cols",
         f"""SELECT date_diff('hour',
                    CAST(l_commitdate AS TIMESTAMP),
                    CAST(l_receiptdate AS TIMESTAMP))
             FROM {TPCH}.lineitem LIMIT 1""")

    test("date_add('day', N, timestamp)",
         f"""SELECT date_add('day', 7, CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders LIMIT 1""")

    test("date_add('hour', N, timestamp)",
         f"""SELECT date_add('hour', 12, CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders LIMIT 1""")

    # =========================================================================
    # 8. TIMESTAMP ARITHMETIC WITH INTERVALS
    # =========================================================================
    print("\n=== 8. TIMESTAMP + INTERVAL ARITHMETIC ===")

    test("Timestamp + INTERVAL HOUR",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) + INTERVAL '6' HOUR
             FROM {TPCH}.orders LIMIT 1""")

    test("Timestamp + INTERVAL DAY",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) + INTERVAL '30' DAY
             FROM {TPCH}.orders LIMIT 1""")

    test("Timestamp - INTERVAL DAY",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) - INTERVAL '1' DAY
             FROM {TPCH}.orders LIMIT 1""")

    test("Timestamp + INTERVAL MINUTE",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) + INTERVAL '90' MINUTE
             FROM {TPCH}.orders LIMIT 1""")

    test("Timestamp + INTERVAL SECOND",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) + INTERVAL '3600' SECOND
             FROM {TPCH}.orders LIMIT 1""")

    test("Timestamp - INTERVAL MONTH (year-month interval)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) - INTERVAL '1' MONTH
             FROM {TPCH}.orders LIMIT 1""",
         expect_fail=True)

    test("Timestamp + INTERVAL YEAR (year-month interval)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) + INTERVAL '1' YEAR
             FROM {TPCH}.orders LIMIT 1""",
         expect_fail=True)

    # =========================================================================
    # 9. FORMAT AND PARSE FUNCTIONS
    # =========================================================================
    print("\n=== 9. FORMAT AND PARSE FUNCTIONS ===")

    test("date_format(timestamp, format)",
         f"""SELECT date_format(CAST(o_orderdate AS TIMESTAMP), '%Y-%m-%d %H:%i:%s')
             FROM {TPCH}.orders LIMIT 1""")

    test("date_parse(string, format)",
         f"""SELECT date_parse(CAST(o_orderdate AS VARCHAR), '%Y-%m-%d')
             FROM {TPCH}.orders LIMIT 1""")

    test("date_format with custom format",
         f"""SELECT date_format(CAST(o_orderdate AS TIMESTAMP), '%d/%m/%Y')
             FROM {TPCH}.orders LIMIT 1""")

    test("format_datetime(timestamp, format)",
         f"""SELECT format_datetime(CAST(o_orderdate AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss')
             FROM {TPCH}.orders LIMIT 1""")

    # =========================================================================
    # 10. MILLISECOND PRECISION
    # =========================================================================
    print("\n=== 10. MILLISECOND PRECISION ===")

    test("Millis in concat-cast roundtrip",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 10:30:00.123' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 1""")

    test("Millis: .999 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 23:59:59.999' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 1""")

    test("Millis: .001 preserved",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 00:00:00.001' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 1""")

    # =========================================================================
    # 11. MICROSECOND PRECISION
    # =========================================================================
    print("\n=== 11. MICROSECOND PRECISION ===")

    test("Micros in concat-cast: .123456 - check if preserved or truncated",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 10:30:00.123456' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 1""")

    test("Micros: .000001 - check smallest micro",
         f"""SELECT CAST(CAST(CAST(o_orderdate AS VARCHAR) || ' 00:00:00.000001' AS TIMESTAMP) AS VARCHAR)
             FROM {TPCH}.orders LIMIT 1""")

    # =========================================================================
    # 12. AGGREGATIONS ON TIMESTAMPS
    # =========================================================================
    print("\n=== 12. AGGREGATIONS ON TIMESTAMPS ===")

    test("MIN(timestamp)",
         f"SELECT MIN(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders")

    test("MAX(timestamp)",
         f"SELECT MAX(CAST(o_orderdate AS TIMESTAMP)) FROM {TPCH}.orders")

    test("COUNT with timestamp filter",
         f"""SELECT count(*) FROM {TPCH}.orders
             WHERE CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1996-06-01 00:00:00'""")

    test("GROUP BY year(timestamp)",
         f"""SELECT year(CAST(o_orderdate AS TIMESTAMP)) AS yr, count(*)
             FROM {TPCH}.orders
             GROUP BY year(CAST(o_orderdate AS TIMESTAMP))
             ORDER BY yr LIMIT 5""")

    test("ORDER BY timestamp",
         f"""SELECT o_orderkey, CAST(o_orderdate AS TIMESTAMP)
             FROM {TPCH}.orders ORDER BY 2 LIMIT 3""")

    test("DISTINCT timestamps",
         f"""SELECT DISTINCT CAST(o_orderdate AS TIMESTAMP)
             FROM {TPCH}.orders ORDER BY 1 LIMIT 5""")

    # =========================================================================
    # 13. TIMESTAMP WITH TIME ZONE (table-backed)
    # =========================================================================
    print("\n=== 13. TIMESTAMP WITH TIME ZONE ===")

    test("Cast timestamp to timestamp with time zone",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)
             FROM {TPCH}.orders LIMIT 1""")

    test("AT TIME ZONE on timestamp with tz",
         f"""SELECT CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE)
                    AT TIME ZONE 'America/New_York'
             FROM {TPCH}.orders LIMIT 1""")

    test("to_unixtime(timestamp with tz)",
         f"""SELECT to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
             FROM {TPCH}.orders LIMIT 1""")

    test("from_unixtime on derived value",
         f"""SELECT from_unixtime(
                    to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
                   )
             FROM {TPCH}.orders LIMIT 1""")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total = sum(results.values())
    print(f"  Total tests:      {total}")
    print(f"  Passed:           {results['pass']}")
    print(f"  Failed:           {results['fail']}")
    print(f"  Expected fails:   {results['expected_fail']}")
    print(f"  Unexpected pass:  {results['unexpected_pass']}")
    print()

    if results['fail'] > 0:
        print("RESULT: Some tests FAILED - see details above")
        return 1
    elif results['unexpected_pass'] > 0:
        print("RESULT: Some expected failures PASSED (update expectations!)")
        return 0
    else:
        print("RESULT: All tests passed as expected")
        return 0


if __name__ == "__main__":
    sys.exit(main())
