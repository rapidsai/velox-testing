#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
#
# Timestamp support verification for Presto native (velox-cudf) workers.
#
# Tests timestamps via table-backed queries (to avoid the known CudfFromVelox
# empty-vector issue with tableless constant projections).
#
# Creates temporary hive tables with timestamp data, runs tests, then cleans up.
#
# Usage:
#   python timestamp_status_test.py [--host HOST] [--port PORT]

import argparse
import sys
import prestodb

SCHEMA_NAME = "timestamp_test"


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
            result_str = rows[0] if rows and rows[0] else "<empty>"
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


def run_ddl(cursor, label, sql):
    """Run a DDL/DML statement (no result expected)."""
    try:
        cursor.execute(sql)
        cursor.fetchall()
        print(f"  OK   : {label}")
        return True
    except Exception as e:
        err = str(e).split('\n')[0][:200]
        print(f"  FAIL : {label}")
        print(f"         SQL: {sql}")
        print(f"         Error: {err}")
        return False


def cleanup(cursor):
    """Drop the test schema and all tables."""
    print("\n--- Cleanup ---")
    try:
        cursor.execute(f"SHOW TABLES FROM hive.{SCHEMA_NAME}")
        tables = cursor.fetchall()
        for (table,) in tables:
            run_ddl(cursor, f"Drop {table}", f"DROP TABLE IF EXISTS hive.{SCHEMA_NAME}.{table}")
        run_ddl(cursor, "Drop schema", f"DROP SCHEMA IF EXISTS hive.{SCHEMA_NAME}")
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Timestamp support verification")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--user", default="test_user")
    args = parser.parse_args()

    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user,
        catalog="hive", schema=SCHEMA_NAME,
    )
    cursor = conn.cursor()

    results = {"pass": 0, "fail": 0, "expected_fail": 0, "unexpected_pass": 0}

    def test(label, sql, expect_fail=False):
        r = run_query(cursor, label, sql, expect_fail)
        results[r] += 1

    # =========================================================================
    # 0. BASELINE - confirm tableless vs table-backed behavior
    # =========================================================================
    print("\n=== 0. BASELINE (tableless queries) ===")
    print("  These are expected to fail on GPU workers due to CudfFromVelox")
    print("  empty-vector issue with constant projections.\n")

    test("SELECT 1 (tableless, no timestamp)",
         "SELECT 1",
         expect_fail=True)

    test("SELECT 1+1 (tableless, no timestamp)",
         "SELECT 1+1",
         expect_fail=True)

    test("SELECT TIMESTAMP literal (tableless)",
         "SELECT TIMESTAMP '2024-01-15 10:30:00'",
         expect_fail=True)

    # =========================================================================
    # 0b. PARAMETRIC PRECISION SYNTAX (coordinator-level rejection)
    # =========================================================================
    print("\n=== 0b. PARAMETRIC PRECISION SYNTAX ===")
    print("  These fail at the coordinator parser, never reach the worker.\n")

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
    # 1. SETUP - create test schema and tables with timestamp data
    # =========================================================================
    print("\n=== 1. SETUP - creating test tables ===")

    cleanup(cursor)
    if not run_ddl(cursor, "Create schema",
                   f"CREATE SCHEMA IF NOT EXISTS hive.{SCHEMA_NAME}"):
        print("\nFATAL: Cannot create hive schema. Is the hive catalog configured?")
        return 1

    # Table: ts_basic - timestamps from casting tpch dates
    setup_ok = run_ddl(cursor, "Create ts_basic (timestamp from tpch dates)",
        f"""CREATE TABLE hive.{SCHEMA_NAME}.ts_basic
            WITH (FORMAT = 'PARQUET') AS
            SELECT
                o_orderkey AS id,
                CAST(o_orderdate AS TIMESTAMP) AS ts_val,
                o_orderdate AS date_val,
                CAST(o_orderdate AS VARCHAR) AS date_str
            FROM tpch.sf1.orders
            LIMIT 100""")
    if not setup_ok:
        print("\nFATAL: Cannot create test tables. Check tpch catalog & hive connector.")
        cleanup(cursor)
        return 1

    # Table: ts_strings - raw string data for cast-to-timestamp tests
    run_ddl(cursor, "Create ts_strings (varchar timestamps for casting)",
        f"""CREATE TABLE hive.{SCHEMA_NAME}.ts_strings
            WITH (FORMAT = 'PARQUET') AS
            SELECT * FROM (
                VALUES
                    (1, '2024-01-15 10:30:00'),
                    (2, '2024-06-15 23:59:59'),
                    (3, '1970-01-01 00:00:00'),
                    (4, '2024-01-15 10:30:00.123'),
                    (5, '2024-01-15 10:30:00.123456'),
                    (6, '1969-12-31 23:59:59'),
                    (7, '2000-02-29 12:00:00'),
                    (8, '2024-12-31 23:59:59.999')
            ) AS t(id, ts_str)""")

    # Table: ts_millis - timestamps with sub-second precision
    run_ddl(cursor, "Create ts_millis (timestamps with millis from lineitem)",
        f"""CREATE TABLE hive.{SCHEMA_NAME}.ts_millis
            WITH (FORMAT = 'PARQUET') AS
            SELECT
                l_orderkey AS id,
                CAST(l_shipdate AS TIMESTAMP) AS ts_ship,
                CAST(l_commitdate AS TIMESTAMP) AS ts_commit,
                CAST(l_receiptdate AS TIMESTAMP) AS ts_receipt
            FROM tpch.sf1.lineitem
            LIMIT 100""")

    # Table: ts_epoch - test data around epoch boundary
    run_ddl(cursor, "Create ts_epoch (epoch boundary timestamps)",
        f"""CREATE TABLE hive.{SCHEMA_NAME}.ts_epoch
            WITH (FORMAT = 'PARQUET') AS
            SELECT * FROM (
                VALUES
                    (1, TIMESTAMP '2024-01-15 10:30:00'),
                    (2, TIMESTAMP '1970-01-01 00:00:00'),
                    (3, TIMESTAMP '2000-01-01 12:00:00'),
                    (4, TIMESTAMP '2024-06-15 23:59:59')
            ) AS t(id, ts_val)""")

    print()

    # =========================================================================
    # 2. BASIC READS - can we read timestamp columns at all?
    # =========================================================================
    print("\n=== 2. BASIC TIMESTAMP READS ===")

    test("Read timestamp column (ts_basic)",
         f"SELECT ts_val FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("Read timestamp column count",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic")

    test("Read multiple timestamp cols (ts_millis)",
         f"SELECT ts_ship, ts_commit, ts_receipt FROM hive.{SCHEMA_NAME}.ts_millis LIMIT 1")

    test("Read epoch table",
         f"SELECT id, ts_val FROM hive.{SCHEMA_NAME}.ts_epoch ORDER BY id LIMIT 4")

    # =========================================================================
    # 3. CAST STRING -> TIMESTAMP (from table data)
    # =========================================================================
    print("\n=== 3. CAST STRING -> TIMESTAMP (table-backed) ===")

    test("Cast varchar col to timestamp",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 1")

    test("Cast varchar with millis to timestamp",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 4")

    test("Cast varchar with micros to timestamp",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 5")

    test("Cast varchar epoch to timestamp",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 3")

    test("Cast varchar pre-epoch to timestamp",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 6")

    # =========================================================================
    # 4. CAST TIMESTAMP -> other types
    # =========================================================================
    print("\n=== 4. CAST TIMESTAMP -> OTHER TYPES ===")

    test("Cast timestamp to varchar",
         f"SELECT CAST(ts_val AS VARCHAR) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("Cast timestamp to date",
         f"SELECT CAST(ts_val AS DATE) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("Cast date to timestamp",
         f"SELECT CAST(date_val AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    # =========================================================================
    # 5. TIMESTAMP COMPARISONS (table-backed)
    # =========================================================================
    print("\n=== 5. TIMESTAMP COMPARISONS ===")

    test("Timestamp = timestamp",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic WHERE ts_val = CAST(date_val AS TIMESTAMP)")

    test("Timestamp > literal",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic WHERE ts_val > TIMESTAMP '1995-01-01 00:00:00'")

    test("Timestamp < literal",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic WHERE ts_val < TIMESTAMP '1995-01-01 00:00:00'")

    test("Timestamp BETWEEN literals",
         f"""SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic
             WHERE ts_val BETWEEN TIMESTAMP '1994-01-01 00:00:00' AND TIMESTAMP '1996-01-01 00:00:00'""")

    test("Timestamp filter on ts_millis (ship > commit)",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_millis WHERE ts_ship > ts_commit")

    # =========================================================================
    # 6. TIMESTAMP FUNCTIONS (table-backed)
    # =========================================================================
    print("\n=== 6. TIMESTAMP FUNCTIONS ===")

    test("year()",
         f"SELECT year(ts_val) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("month()",
         f"SELECT month(ts_val) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("day()",
         f"SELECT day(ts_val) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("hour()",
         f"SELECT hour(ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("minute()",
         f"SELECT minute(ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("second()",
         f"SELECT second(ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("date_trunc('year')",
         f"SELECT date_trunc('year', ts_val) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("date_trunc('month')",
         f"SELECT date_trunc('month', ts_val) FROM hive.{SCHEMA_NAME}.ts_basic LIMIT 1")

    test("date_trunc('day')",
         f"SELECT date_trunc('day', ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("date_trunc('hour')",
         f"SELECT date_trunc('hour', ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("date_diff('day') between columns",
         f"SELECT date_diff('day', ts_ship, ts_receipt) FROM hive.{SCHEMA_NAME}.ts_millis LIMIT 1")

    test("date_diff('second') between column and literal",
         f"SELECT date_diff('second', TIMESTAMP '1970-01-01 00:00:00', ts_val) FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 2")

    test("date_format",
         f"SELECT date_format(ts_val, '%Y-%m-%d %H:%i:%s') FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("date_parse (string col)",
         f"SELECT date_parse(ts_str, '%Y-%m-%d %H:%i:%s') FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 1")

    # =========================================================================
    # 7. TIMESTAMP ARITHMETIC (table-backed)
    # =========================================================================
    print("\n=== 7. TIMESTAMP ARITHMETIC ===")

    test("Timestamp + INTERVAL HOUR",
         f"SELECT ts_val + INTERVAL '1' HOUR FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("Timestamp + INTERVAL DAY",
         f"SELECT ts_val + INTERVAL '7' DAY FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("Timestamp - INTERVAL MONTH",
         f"SELECT ts_val - INTERVAL '1' MONTH FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    test("Timestamp + INTERVAL YEAR",
         f"SELECT ts_val + INTERVAL '1' YEAR FROM hive.{SCHEMA_NAME}.ts_epoch WHERE id = 1")

    # =========================================================================
    # 8. MILLISECOND PRECISION (table-backed)
    # =========================================================================
    print("\n=== 8. MILLISECOND PRECISION ===")

    test("Millis in cast from string",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 4")

    test("Millis roundtrip (varchar->timestamp->varchar)",
         f"SELECT CAST(CAST(ts_str AS TIMESTAMP) AS VARCHAR) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 4")

    test("Millis preserved: 23:59:59.999",
         f"SELECT CAST(CAST(ts_str AS TIMESTAMP) AS VARCHAR) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 8")

    # =========================================================================
    # 9. MICROSECOND PRECISION (table-backed)
    # =========================================================================
    print("\n=== 9. MICROSECOND PRECISION ===")

    test("Micros in cast from string",
         f"SELECT CAST(ts_str AS TIMESTAMP) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 5")

    test("Micros roundtrip (varchar->timestamp->varchar) - check truncation",
         f"SELECT CAST(CAST(ts_str AS TIMESTAMP) AS VARCHAR) FROM hive.{SCHEMA_NAME}.ts_strings WHERE id = 5")

    # =========================================================================
    # 10. AGGREGATIONS ON TIMESTAMPS
    # =========================================================================
    print("\n=== 10. AGGREGATIONS ON TIMESTAMPS ===")

    test("MIN(timestamp)",
         f"SELECT MIN(ts_val) FROM hive.{SCHEMA_NAME}.ts_basic")

    test("MAX(timestamp)",
         f"SELECT MAX(ts_val) FROM hive.{SCHEMA_NAME}.ts_basic")

    test("COUNT with timestamp filter",
         f"SELECT count(*) FROM hive.{SCHEMA_NAME}.ts_basic WHERE ts_val > TIMESTAMP '1995-06-01 00:00:00'")

    test("GROUP BY year(timestamp)",
         f"SELECT year(ts_val), count(*) FROM hive.{SCHEMA_NAME}.ts_basic GROUP BY year(ts_val) ORDER BY 1 LIMIT 5")

    test("ORDER BY timestamp",
         f"SELECT id, ts_val FROM hive.{SCHEMA_NAME}.ts_basic ORDER BY ts_val LIMIT 3")

    # =========================================================================
    # CLEANUP
    # =========================================================================
    cleanup(cursor)

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
