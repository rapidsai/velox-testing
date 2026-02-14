# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import decimal
import os
import re
import sys

import duckdb
import prestodb

import create_hive_tables
import test_utils


def _default_port():
    env_port = os.getenv("PRESTO_COORDINATOR_PORT")
    if env_port:
        try:
            return int(env_port)
        except ValueError:
            pass
    return 8080


DEFAULT_HOST = os.getenv("PRESTO_COORDINATOR_HOST", "localhost")
DEFAULT_PORT = _default_port()
DEFAULT_SCHEMA = "tpch_test"
DEFAULT_MAX_PARTKEY = 16777215


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def _get_table_external_location(schema_name, table, presto_cursor):
    create_table_text = presto_cursor.execute(
        f"SHOW CREATE TABLE hive.{schema_name}.{table}"
    ).fetchone()
    assert len(create_table_text) == 1

    test_pattern = (
        r"external_location = 'file:/var/lib/presto/data/hive/data/integration_test/(.*)'"
    )
    user_pattern = (
        r"external_location = 'file:/var/lib/presto/data/hive/data/user_data/(.*)'"
    )

    test_match = re.search(test_pattern, create_table_text[0])
    if test_match:
        external_dir = get_abs_file_path(f"data/{test_match.group(1)}")
    else:
        user_match = re.search(user_pattern, create_table_text[0])
        if not user_match:
            raise RuntimeError(
                "Could not parse external_location from SHOW CREATE TABLE for "
                f"hive.{schema_name}.{table}: {create_table_text[0]}"
            )
        presto_data_dir = os.getenv("PRESTO_DATA_DIR")
        if not presto_data_dir:
            raise RuntimeError(
                "PRESTO_DATA_DIR is required for user_data external locations."
            )
        external_dir = f"{presto_data_dir}/{user_match.group(1)}"

    if not os.path.isdir(external_dir):
        raise RuntimeError(
            f"External location '{external_dir}' for hive.{schema_name}.{table} "
            "does not exist."
        )
    return external_dir


def _setup_tables(presto_cursor, schema_name, create_tables):
    if create_tables:
        schemas_dir = test_utils.get_abs_file_path("../common/schemas/tpch")
        create_hive_tables.create_tables(
            presto_cursor,
            schema_name,
            schemas_dir,
            "integration_test/tpch",
        )

    tables = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchall()
    if not tables:
        raise RuntimeError(
            f"No tables found in schema '{schema_name}'. "
            "Pass --schema-name for an existing schema or omit it to auto-create."
        )

    for (table,) in tables:
        location = _get_table_external_location(schema_name, table, presto_cursor)
        test_utils.create_duckdb_table(table, location)


def _generate_exponential_ranges(max_partkey):
    lower = 1
    width = 1
    while lower <= max_partkey:
        upper = min(max_partkey, lower + width - 1)
        yield lower, upper
        lower = upper + 1
        width *= 2


def _run_range_query(presto_cursor, lower, upper, decimal_cast):
    query = (
        "SELECT "
        "  count(*) AS row_count, "
        f"  avg(CAST(l_quantity AS {decimal_cast})) AS avg_qty_decimal, "
        "  avg(CAST(l_quantity AS DOUBLE)) AS avg_qty_double "
        "FROM lineitem "
        f"WHERE l_partkey BETWEEN {lower} AND {upper}"
    )
    presto_row = presto_cursor.execute(query).fetchone()
    duckdb_row = duckdb.sql(query).fetchone()
    return presto_row, duckdb_row


def _to_decimal(value):
    if value is None:
        return None
    if isinstance(value, decimal.Decimal):
        return value
    return decimal.Decimal(str(value))


def _decimal_abs_diff(left, right):
    if left is None and right is None:
        return decimal.Decimal("0")
    if left is None or right is None:
        return None
    return abs(_to_decimal(left) - _to_decimal(right))


def _double_abs_diff(left, right):
    if left is None and right is None:
        return 0.0
    if left is None or right is None:
        return None
    return abs(float(left) - float(right))


def _format_value(value):
    if value is None:
        return "NULL"
    return str(value)


def _print_header():
    print(
        "range_id,lower,upper,"
        "presto_count,duckdb_count,"
        "presto_avg_decimal,duckdb_avg_decimal,abs_diff_decimal,"
        "presto_avg_double,duckdb_avg_double,abs_diff_double,status",
        flush=True,
    )


def _run_scan(
    presto_cursor,
    max_partkey,
    decimal_cast,
    decimal_abs_tol,
    double_abs_tol,
    stop_on_mismatch,
):
    mismatch_count = 0
    total_ranges = 0
    _print_header()

    for range_id, (lower, upper) in enumerate(_generate_exponential_ranges(max_partkey)):
        total_ranges += 1
        presto_row, duckdb_row = _run_range_query(presto_cursor, lower, upper, decimal_cast)

        count_match = presto_row[0] == duckdb_row[0]
        decimal_diff = _decimal_abs_diff(presto_row[1], duckdb_row[1])
        double_diff = _double_abs_diff(presto_row[2], duckdb_row[2])

        decimal_match = (
            decimal_diff is not None and decimal_diff <= decimal_abs_tol
        )
        double_match = (
            double_diff is not None and double_diff <= double_abs_tol
        )

        is_match = count_match and decimal_match and double_match
        if not is_match:
            mismatch_count += 1

        print(
            ",".join(
                [
                    str(range_id),
                    str(lower),
                    str(upper),
                    _format_value(presto_row[0]),
                    _format_value(duckdb_row[0]),
                    _format_value(presto_row[1]),
                    _format_value(duckdb_row[1]),
                    _format_value(decimal_diff),
                    _format_value(presto_row[2]),
                    _format_value(duckdb_row[2]),
                    _format_value(double_diff),
                    "MATCH" if is_match else "MISMATCH",
                ]
            ),
            flush=True,
        )

        if stop_on_mismatch and not is_match:
            break

    print(
        "\nSummary: "
        f"ranges_scanned={total_ranges}, mismatches={mismatch_count}, "
        f"max_partkey={max_partkey}, decimal_cast={decimal_cast}",
        flush=True,
    )
    return mismatch_count


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run exponential l_partkey range scans and compare Presto vs DuckDB "
            "for decimal avg behavior."
        )
    )
    parser.add_argument("--hostname", default=DEFAULT_HOST)
    parser.add_argument("--port", default=DEFAULT_PORT, type=int)
    parser.add_argument("--user", default="test_user")
    parser.add_argument(
        "--schema-name",
        help=(
            "Existing Hive schema to use. If omitted, script creates tpch_test "
            "from integration test data."
        ),
    )
    parser.add_argument("--keep-tables", action="store_true", default=False)
    parser.add_argument("--max-partkey", type=int, default=DEFAULT_MAX_PARTKEY)
    parser.add_argument(
        "--decimal-cast",
        default="DECIMAL(18, 6)",
        help="Decimal type used in avg(CAST(l_quantity AS <type>)).",
    )
    parser.add_argument(
        "--decimal-abs-tol",
        default="0",
        help="Absolute tolerance for decimal avg comparisons.",
    )
    parser.add_argument(
        "--double-abs-tol",
        default=1e-12,
        type=float,
        help="Absolute tolerance for double avg comparisons.",
    )
    parser.add_argument("--stop-on-mismatch", action="store_true", default=False)
    args = parser.parse_args()

    schema_name = args.schema_name if args.schema_name else DEFAULT_SCHEMA
    should_create_tables = not bool(args.schema_name)
    conn = prestodb.dbapi.connect(
        host=args.hostname,
        port=args.port,
        user=args.user,
        catalog="hive",
        schema=schema_name,
    )
    cursor = conn.cursor()

    try:
        _setup_tables(cursor, schema_name, should_create_tables)
        mismatch_count = _run_scan(
            presto_cursor=cursor,
            max_partkey=args.max_partkey,
            decimal_cast=args.decimal_cast,
            decimal_abs_tol=decimal.Decimal(args.decimal_abs_tol),
            double_abs_tol=args.double_abs_tol,
            stop_on_mismatch=args.stop_on_mismatch,
        )
    finally:
        if should_create_tables and not args.keep_tables:
            create_hive_tables.drop_schema(cursor, schema_name)
        cursor.close()
        conn.close()

    if mismatch_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
