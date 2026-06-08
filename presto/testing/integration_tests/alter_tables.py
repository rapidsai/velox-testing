# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Run a file of `;`-separated DDL statements (typically ALTER TABLE) against
a Hive schema in Presto.

Connects with catalog='hive' and schema set to --schema-name so unqualified
table references in the SQL resolve under that schema. Reports per-statement
success/failure and exits non-zero if any statement failed.
"""

import argparse
import re
import sys

import prestodb


def _split_statements(sql_text):
    """Split SQL file text into individual statements on `;` boundaries.

    Strips line- (`-- ...`) and block- (`/* ... */`) comments first so a `;`
    inside a comment doesn't end a statement. String literals containing `;`
    aren't expected in ALTER TABLE DDL and aren't handled.
    """
    sql_text = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
    sql_text = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def run_statements(cursor, statements, verbose=False):
    success = 0
    failure = 0
    for i, stmt in enumerate(statements, 1):
        oneline = " ".join(stmt.split())
        preview = oneline if len(oneline) < 120 else oneline[:117] + "..."
        try:
            if verbose:
                print(f"  [{i}/{len(statements)}] {preview} ...", end=" ", flush=True)
            cursor.execute(stmt)
            if verbose:
                print("OK")
            success += 1
        except Exception as e:
            if verbose:
                print("FAILED")
            print(f"  [{i}/{len(statements)}] {preview}", file=sys.stderr)
            print(f"    error: {e}", file=sys.stderr)
            failure += 1
    return success, failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run ALTER TABLE (or any DDL) statements against a Hive schema in Presto."
    )
    parser.add_argument("--schema-name", required=True, help="Hive schema (e.g. tpchsf1000).")
    parser.add_argument("--sql-file", required=True, help="Path to file of `;`-separated SQL statements.")
    parser.add_argument("--host", default="localhost", help="Presto coordinator hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=8080, help="Presto coordinator port (default: 8080)")
    parser.add_argument("--user", default="test_user", help="Presto user (default: test_user)")
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()

    with open(args.sql_file) as f:
        statements = _split_statements(f.read())
    if not statements:
        print(f"No statements found in {args.sql_file}", file=sys.stderr)
        sys.exit(1)

    print(
        f"[AlterTables] Connecting to {args.host}:{args.port} as {args.user}; "
        f"schema=hive.{args.schema_name}; {len(statements)} statement(s)"
    )
    conn = prestodb.dbapi.connect(
        host=args.host, port=args.port, user=args.user, catalog="hive", schema=args.schema_name
    )
    cursor = conn.cursor()
    try:
        success, failure = run_statements(cursor, statements, verbose=args.verbose)
    finally:
        cursor.close()
        conn.close()

    print(f"[AlterTables] Done: {success} succeeded, {failure} failed")
    sys.exit(0 if failure == 0 else 1)
