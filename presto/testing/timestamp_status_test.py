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

TPCH = "tpch.sf1"


def run_query(cursor, label, sql, expected=None, expect_fail=False):
    """Run a query, optionally validate result against expected value."""
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        if expect_fail:
            print(f"  UNEXPECTED PASS : {label}")
            print(f"      SQL: {sql}")
            print(f"      Result: {rows}")
            return "unexpected_pass"

        if not rows:
            print(f"  FAIL (no rows) : {label}")
            print(f"      SQL: {sql}")
            return "fail"

        actual = rows[0][0] if len(rows[0]) == 1 else list(rows[0])

        if expected is not None:
            if actual == expected:
                print(f"  PASS : {label}")
                print(f"      Expected: {expected!r}  Got: {actual!r}")
                return "pass"
            else:
                print(f"  MISMATCH : {label}")
                print(f"      Expected: {expected!r}")
                print(f"      Actual:   {actual!r}")
                print(f"      SQL: {sql}")
                return "fail"
        else:
            # No expected value - just check query succeeded with data
            if len(rows) == 1:
                print(f"  PASS (unchecked) : {label}")
                print(f"      Result: {rows[0]}")
            else:
                print(f"  PASS (unchecked) : {label}")
                print(f"      Result: ({len(rows)} rows) first={rows[0]}")
            return "pass"

    except Exception as e:
        err = str(e).split('\n')[0][:200]
        if expect_fail:
            print(f"  EXPECTED FAIL : {label}")
            print(f"      Error: {err}")
            return "expected_fail"
        else:
            print(f"  FAIL : {label}")
            print(f"      SQL: {sql}")
            print(f"      Error: {err}")
            return "fail"


def main():
    parser = argparse.ArgumentParser(description="Timestamp support verification")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--user", default="test_user")
    parser.add_argument("--tpch-schema", default="sf1",
                        help="TPC-H schema/scale factor (default: sf1)")
    args = parser.parse_args()

    global TPCH
    TPCH = f"tpch.{args.tpch_schema}"

    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user,
        catalog="tpch", schema=args.tpch_schema,
    )
    cursor = conn.cursor()

    results = {"pass": 0, "fail": 0, "expected_fail": 0, "unexpected_pass": 0}

    def test(label, sql, expected=None, expect_fail=False):
        r = run_query(cursor, label, sql, expected=expected, expect_fail=expect_fail)
        results[r] += 1

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
    test("Timestamp > literal (true case)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1995-01-01 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=True)

    test("Timestamp > literal (false case)",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) > TIMESTAMP '1997-01-01 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=False)

    test("Timestamp = literal",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) = TIMESTAMP '1996-01-02 00:00:00'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=True)

    test("Timestamp BETWEEN",
         f"""SELECT CAST(o_orderdate AS TIMESTAMP) BETWEEN TIMESTAMP '1996-01-01 00:00:00' AND TIMESTAMP '1996-12-31 23:59:59'
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=True)

    # lineitem key=1, line=1: ship=1996-03-13, commit=1996-02-12, receipt=1996-03-22
    test("Timestamp col > col (ship > commit)",
         f"""SELECT CAST(l_shipdate AS TIMESTAMP) > CAST(l_commitdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expected=True)

    test("Timestamp col > col (receipt > ship)",
         f"""SELECT CAST(l_receiptdate AS TIMESTAMP) > CAST(l_shipdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expected=True)

    test("Timestamp col > col (commit > receipt = false)",
         f"""SELECT CAST(l_commitdate AS TIMESTAMP) > CAST(l_receiptdate AS TIMESTAMP)
             FROM {TPCH}.lineitem WHERE l_orderkey = 1 AND l_linenumber = 1""",
         expected=False)

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
    # 1996-01-02 00:00:00 UTC = 820454400 seconds since 1970-01-01
    test("date_diff('second') from epoch",
         f"""SELECT date_diff('second', TIMESTAMP '1970-01-01 00:00:00', CAST(o_orderdate AS TIMESTAMP))
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=820454400)

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
         expect_fail=True)

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

    # 1996-01-02 00:00:00 UTC = 820454400 seconds since epoch
    test("to_unixtime(timestamp with tz)",
         f"""SELECT to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected=820454400.0)

    test("from_unixtime(820454400) roundtrip",
         f"""SELECT CAST(from_unixtime(
                    to_unixtime(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP WITH TIME ZONE))
                   ) AS VARCHAR)
             FROM {TPCH}.orders WHERE o_orderkey = 1""",
         expected="1996-01-02 00:00:00.000")

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
