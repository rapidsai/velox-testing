# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse

import prestodb


def check_tables_analyzed(presto_cursor, schema_name):
    """Check that ANALYZE TABLE has been run on all tables in the given schema.

    Verifies that table statistics exist by checking if SHOW STATS FOR each table
    returns a non-null row_count. Returns True if all tables have statistics,
    raises an error otherwise.

    Args:
        presto_cursor: Presto database cursor
        schema_name: Name of the schema containing tables to check
    """
    tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
    table_names = [table_name for table_name, in tables]

    if not table_names:
        raise RuntimeError(f"No tables found in schema '{schema_name}'")

    tables_missing_stats = []
    for table_name in table_names:
        presto_cursor.execute(
            f"SHOW STATS FOR hive.{schema_name}.{table_name}"
        )
        # Find column indices from the cursor description.
        col_names = [desc[0] for desc in presto_cursor.description]
        distinct_idx = col_names.index("distinct_values_count")
        col_name_idx = col_names.index("column_name")

        stats = presto_cursor.fetchall()
        # Column rows (where column_name is not None) should have a non-null
        # distinct_values_count if ANALYZE TABLE has been run. Check that at
        # least one column has this statistic populated.
        column_rows = [row for row in stats if row[col_name_idx] is not None]
        has_stats = any(row[distinct_idx] is not None for row in column_rows)
        if not column_rows or not has_stats:
            tables_missing_stats.append(table_name)

    if tables_missing_stats:
        missing = ", ".join(tables_missing_stats)
        raise RuntimeError(
            f"ANALYZE TABLE has not been run on the following tables in schema "
            f"'{schema_name}': {missing}. "
            f"Run analyze_tables.sh on a CPU Presto instance before benchmarking."
        )
    print(f"All {len(table_names)} table(s) in schema '{schema_name}' have statistics.")


def analyze_tables(presto_cursor, schema_name, verbose=False):
    """Analyze all tables in the given schema to collect statistics.

    Args:
        presto_cursor: Presto database cursor
        schema_name: Name of the schema containing tables to analyze
        verbose: If True, print detailed progress information
    """
    try:
        if verbose:
            print(f"Discovering tables in schema '{schema_name}'...")
        tables = presto_cursor.execute(f"SHOW TABLES FROM hive.{schema_name}").fetchall()
        table_names = [table_name for (table_name,) in tables]

        if not table_names:
            print(f"Warning: No tables found in schema '{schema_name}'")
            return

        if verbose:
            print(f"Found {len(table_names)} table(s): {', '.join(table_names)}")
            print("\nStarting table analysis...")

        success_count = 0
        failure_count = 0

        for i, table_name in enumerate(table_names, 1):
            try:
                if verbose:
                    print(f"  [{i}/{len(table_names)}] Analyzing table '{table_name}'...", end=" ")
                presto_cursor.execute(f"ANALYZE hive.{schema_name}.{table_name}")
                if verbose:
                    print("OK")
                success_count += 1
            except Exception as e:
                if verbose:
                    print("FAILED")
                print(f"Warning: Failed to analyze table '{table_name}': {e}")
                failure_count += 1

        if verbose or failure_count > 0:
            print(f"\nAnalysis complete: {success_count} succeeded, {failure_count} failed")

    except Exception as e:
        print(f"Error: Could not list tables in schema '{schema_name}': {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze all tables in a Hive schema to collect statistics for query optimization."
    )
    parser.add_argument(
        "--schema-name", type=str, required=True, help="Name of the schema containing the tables to analyze."
    )
    parser.add_argument(
        "--host", type=str, default="localhost", help="Presto coordinator hostname (default: localhost)"
    )
    parser.add_argument("--port", type=int, default=8080, help="Presto coordinator port (default: 8080)")
    parser.add_argument("--user", type=str, default="test_user", help="Presto user (default: test_user)")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Enable verbose output")
    parser.add_argument(
        "--check-only", action="store_true", default=False, help="Only check if tables have been analyzed (do not run ANALYZE)"
    )

    args = parser.parse_args()

    conn = prestodb.dbapi.connect(host=args.host, port=args.port, user=args.user, catalog="hive")
    cursor = conn.cursor()

    try:
        if args.check_only:
            check_tables_analyzed(cursor, args.schema_name)
        else:
            analyze_tables(cursor, args.schema_name, verbose=args.verbose)
    finally:
        cursor.close()
        conn.close()
