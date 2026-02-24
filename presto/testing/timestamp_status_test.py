#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Timestamp support verification for Presto native (velox-cudf) workers.
# Tests timestamp literals, casting, precision, and functions.
#
# Usage:
#   python timestamp_status_test.py [--host HOST] [--port PORT] [--catalog CATALOG] [--schema SCHEMA]

import argparse
import sys
import prestodb


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
            result_str = rows[0][0] if rows and rows[0] else "<empty>"
            print(f"  PASS : {label}")
            print(f"         Result: {result_str}")
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
    parser.add_argument("--catalog", default="system")
    parser.add_argument("--schema", default="runtime")
    parser.add_argument("--user", default="test_user")
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user,
        catalog=args.catalog, schema=args.schema,
    )
    cursor = conn.cursor()

    results = {"pass": 0, "fail": 0, "expected_fail": 0, "unexpected_pass": 0}

    def test(label, sql, expect_fail=False):
        r = run_query(cursor, label, sql, expect_fail)
        results[r] += 1

    # =========================================================================
    # 1. TIMESTAMP LITERALS
    # =========================================================================
    print("\n=== 1. TIMESTAMP LITERALS ===")

    test("Basic timestamp literal",
         "SELECT TIMESTAMP '2024-01-15 10:30:00'")

    test("Timestamp literal with millis",
         "SELECT TIMESTAMP '2024-01-15 10:30:00.123'")

    test("Timestamp literal with micros",
         "SELECT TIMESTAMP '2024-01-15 10:30:00.123456'")

    test("Timestamp literal with nanos",
         "SELECT TIMESTAMP '2024-01-15 10:30:00.123456789'")

    test("Timestamp literal - typeof",
         "SELECT typeof(TIMESTAMP '2024-01-15 10:30:00')")

    test("Timestamp literal epoch",
         "SELECT TIMESTAMP '1970-01-01 00:00:00'")

    test("Timestamp literal negative (pre-epoch)",
         "SELECT TIMESTAMP '1969-12-31 23:59:59'")

    # =========================================================================
    # 2. TIMESTAMP WITH TIME ZONE LITERALS
    # =========================================================================
    print("\n=== 2. TIMESTAMP WITH TIME ZONE LITERALS ===")

    test("Timestamp with TZ literal (UTC)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00 UTC'")

    test("Timestamp with TZ literal (offset)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00 +05:30'")

    test("Timestamp with TZ literal (named zone)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00 America/New_York'")

    test("Timestamp with TZ - typeof",
         "SELECT typeof(TIMESTAMP '2024-01-15 10:30:00 UTC')")

    # =========================================================================
    # 3. CAST STRING TO TIMESTAMP
    # =========================================================================
    print("\n=== 3. CAST STRING TO TIMESTAMP ===")

    test("Cast string to timestamp",
         "SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)")

    test("Cast string with millis to timestamp",
         "SELECT CAST('2024-01-15 10:30:00.123' AS TIMESTAMP)")

    test("Cast string with micros to timestamp",
         "SELECT CAST('2024-01-15 10:30:00.123456' AS TIMESTAMP)")

    test("Cast string to timestamp with time zone",
         "SELECT CAST('2024-01-15 10:30:00 UTC' AS TIMESTAMP WITH TIME ZONE)")

    # =========================================================================
    # 4. TIMESTAMP PRECISION - timestamp(3) and timestamp(6) SYNTAX
    # =========================================================================
    print("\n=== 4. PARAMETRIC PRECISION SYNTAX ===")

    test("CAST to timestamp(3) - parametric syntax",
         "SELECT CAST('2024-01-15 10:30:00.123' AS timestamp(3))",
         expect_fail=True)

    test("CAST to timestamp(6) - parametric syntax",
         "SELECT CAST('2024-01-15 10:30:00.123456' AS timestamp(6))",
         expect_fail=True)

    test("CAST to timestamp(9) - parametric syntax",
         "SELECT CAST('2024-01-15 10:30:00.123456789' AS timestamp(9))",
         expect_fail=True)

    # =========================================================================
    # 5. TIMESTAMP ARITHMETIC AND FUNCTIONS
    # =========================================================================
    print("\n=== 5. TIMESTAMP ARITHMETIC AND FUNCTIONS ===")

    test("current_timestamp",
         "SELECT current_timestamp")

    test("now()",
         "SELECT now()")

    test("date_add (interval)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00' + INTERVAL '1' HOUR")

    test("date_diff",
         "SELECT date_diff('second', TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-01-15 10:30:00')")

    test("year() extraction",
         "SELECT year(TIMESTAMP '2024-01-15 10:30:00')")

    test("month() extraction",
         "SELECT month(TIMESTAMP '2024-01-15 10:30:00')")

    test("day() extraction",
         "SELECT day(TIMESTAMP '2024-01-15 10:30:00')")

    test("hour() extraction",
         "SELECT hour(TIMESTAMP '2024-01-15 10:30:00')")

    test("minute() extraction",
         "SELECT minute(TIMESTAMP '2024-01-15 10:30:00')")

    test("second() extraction",
         "SELECT second(TIMESTAMP '2024-01-15 10:30:00')")

    test("millisecond() extraction",
         "SELECT millisecond(TIMESTAMP '2024-01-15 10:30:00.123')")

    test("date_trunc",
         "SELECT date_trunc('hour', TIMESTAMP '2024-01-15 10:30:45')")

    test("date_format",
         "SELECT date_format(TIMESTAMP '2024-01-15 10:30:00', '%Y-%m-%d %H:%i:%s')")

    test("date_parse",
         "SELECT date_parse('2024-01-15 10:30:00', '%Y-%m-%d %H:%i:%s')")

    test("from_unixtime",
         "SELECT from_unixtime(1705312200)")

    test("to_unixtime",
         "SELECT to_unixtime(TIMESTAMP '2024-01-15 10:30:00 UTC')")

    test("AT TIME ZONE",
         "SELECT TIMESTAMP '2024-01-15 10:30:00 UTC' AT TIME ZONE 'America/New_York'")

    # =========================================================================
    # 6. TIMESTAMP COMPARISONS
    # =========================================================================
    print("\n=== 6. TIMESTAMP COMPARISONS ===")

    test("Timestamp equality",
         "SELECT TIMESTAMP '2024-01-15 10:30:00' = TIMESTAMP '2024-01-15 10:30:00'")

    test("Timestamp less-than",
         "SELECT TIMESTAMP '2024-01-15 10:30:00' < TIMESTAMP '2024-01-16 10:30:00'")

    test("Timestamp BETWEEN",
         "SELECT TIMESTAMP '2024-01-15 10:30:00' BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-12-31 23:59:59'")

    test("Timestamp in WHERE clause",
         "SELECT 1 WHERE TIMESTAMP '2024-01-15 10:30:00' > TIMESTAMP '2024-01-14 10:30:00'")

    # =========================================================================
    # 7. TIMESTAMP CAST CONVERSIONS
    # =========================================================================
    print("\n=== 7. TIMESTAMP CAST CONVERSIONS ===")

    test("Cast timestamp to date",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00' AS DATE)")

    test("Cast date to timestamp",
         "SELECT CAST(DATE '2024-01-15' AS TIMESTAMP)")

    test("Cast timestamp to varchar",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00' AS VARCHAR)")

    test("Cast timestamp with TZ to timestamp",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00 UTC' AS TIMESTAMP)")

    test("Cast timestamp to timestamp with TZ",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00' AS TIMESTAMP WITH TIME ZONE)")

    # =========================================================================
    # 8. MILLISECOND PRECISION VERIFICATION
    # =========================================================================
    print("\n=== 8. MILLISECOND PRECISION VERIFICATION ===")

    test("Millis preserved in literal",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00.999' AS VARCHAR)")

    test("Millis preserved through cast roundtrip",
         "SELECT CAST(CAST('2024-01-15 10:30:00.123' AS TIMESTAMP) AS VARCHAR)")

    test("Millis in arithmetic",
         "SELECT TIMESTAMP '2024-01-15 10:30:00.500' + INTERVAL '500' MILLISECOND",
         expect_fail=False)

    # =========================================================================
    # 9. MICROSECOND PRECISION VERIFICATION
    # =========================================================================
    print("\n=== 9. MICROSECOND PRECISION VERIFICATION ===")

    test("Micros in literal - check if preserved or truncated",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00.123456' AS VARCHAR)")

    test("Micros roundtrip through cast",
         "SELECT CAST(CAST('2024-01-15 10:30:00.123456' AS TIMESTAMP) AS VARCHAR)")

    # =========================================================================
    # 10. LEGACY TIMESTAMP MODE
    # =========================================================================
    print("\n=== 10. LEGACY TIMESTAMP MODE ===")

    test("Check legacy_timestamp setting",
         "SELECT current_timezone()")

    test("Set legacy_timestamp = false, then use literal",
         "SET SESSION legacy_timestamp = false")
    test("Literal after legacy=false",
         "SELECT CAST(TIMESTAMP '2024-01-15 10:30:00 UTC' AS TIMESTAMP)")

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
        print("RESULT: Some expected failures PASSED (good news, update expectations!)")
        return 0
    else:
        print("RESULT: All tests passed as expected")
        return 0


if __name__ == "__main__":
    sys.exit(main())
