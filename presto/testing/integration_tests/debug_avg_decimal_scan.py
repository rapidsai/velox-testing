# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import argparse
import decimal
import os
import re
import sys
import time

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
DEFAULT_MAX_PARTKEY = 33554431
DEFAULT_REQUIRED_MIN_MAX_PARTKEY = 20000000
DEFAULT_MODE = "q17_predicate"
DEFAULT_DECIMAL_ABS_TOL = "0.000001"
DEFAULT_MAJOR_DECIMAL_ABS_DIFF = decimal.Decimal("0.01")
DEFAULT_MAJOR_DOUBLE_ABS_DIFF = 0.01


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def _progress(message):
    print(f"PROGRESS,{message}", flush=True)


def _escape_sql_string(value):
    return value.replace("'", "''")


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
    _progress(f"phase=setup,event=start,schema={schema_name}")
    if create_tables:
        _progress(f"phase=setup,event=create_hive_tables_start,schema={schema_name}")
        schemas_dir = test_utils.get_abs_file_path("../common/schemas/tpch")
        create_hive_tables.create_tables(
            presto_cursor,
            schema_name,
            schemas_dir,
            "integration_test/tpch",
        )
        _progress(f"phase=setup,event=create_hive_tables_end,schema={schema_name}")

    tables = presto_cursor.execute(f"SHOW TABLES in {schema_name}").fetchall()
    if not tables:
        raise RuntimeError(
            f"No tables found in schema '{schema_name}'. "
            "Pass --schema-name for an existing schema or omit it to auto-create."
        )
    _progress(f"phase=setup,event=discover_tables,count={len(tables)}")

    for index, (table,) in enumerate(tables, start=1):
        table_start = time.time()
        _progress(
            "phase=setup,event=duckdb_register_start,"
            f"table={table},index={index}/{len(tables)}"
        )
        location = _get_table_external_location(schema_name, table, presto_cursor)
        test_utils.create_duckdb_table(table, location)
        _progress(
            "phase=setup,event=duckdb_register_end,"
            f"table={table},index={index}/{len(tables)},"
            f"seconds={time.time() - table_start:.3f}"
        )
    _progress("phase=setup,event=end")


def _get_lineitem_partkey_stats(presto_cursor):
    query = "SELECT min(l_partkey), max(l_partkey), count(*) FROM lineitem"
    presto_stats = presto_cursor.execute(query).fetchone()
    duckdb_stats = duckdb.sql(query).fetchone()
    return presto_stats, duckdb_stats


def _validate_dataset_scale(presto_cursor, required_min_max_partkey):
    presto_stats_raw, duckdb_stats_raw = _get_lineitem_partkey_stats(presto_cursor)
    presto_stats = tuple(presto_stats_raw)
    duckdb_stats = tuple(duckdb_stats_raw)
    print(
        "Dataset stats: "
        f"presto[min,max,count]={presto_stats_raw}, "
        f"duckdb[min,max,count]={duckdb_stats_raw}",
        flush=True,
    )

    if presto_stats != duckdb_stats:
        raise RuntimeError(
            "Presto and DuckDB lineitem stats differ before scan. "
            f"presto={presto_stats} duckdb={duckdb_stats}"
        )

    if required_min_max_partkey > 0 and presto_stats[1] < required_min_max_partkey:
        raise RuntimeError(
            "Dataset does not reach requested SF100-scale partkey range. "
            f"max(l_partkey)={presto_stats[1]} is below required "
            f"{required_min_max_partkey}. "
            "Use an SF100 schema or lower --require-min-max-partkey."
        )

    return presto_stats


def _generate_exponential_prefix_uppers(max_partkey):
    upper = 1
    while upper < max_partkey:
        yield upper
        upper = upper * 2 + 1
    yield max_partkey


def _get_mode_metric_labels(mode):
    if mode == "avg_cast":
        return "avg_qty_decimal", "avg_qty_double"
    if mode == "threshold_correlated_only":
        return "avg_threshold", "sum_threshold"
    if mode == "threshold_grouped_only":
        return "avg_threshold", "sum_threshold"
    assert mode == "q17_predicate"
    return "avg_yearly", "sum_extendedprice"


def _build_prefix_query(
    mode,
    upper,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_threshold_mode,
):
    if mode == "avg_cast":
        return (
            "SELECT "
            "  count(*) AS row_count, "
            f"  avg(CAST(l_quantity AS {decimal_cast})) AS avg_qty_decimal, "
            "  avg(CAST(l_quantity AS DOUBLE)) AS avg_qty_double "
            "FROM lineitem "
            f"WHERE l_partkey BETWEEN 1 AND {upper}"
        )

    if mode == "threshold_correlated_only":
        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.threshold) AS avg_threshold, "
            "  sum(t.threshold) AS sum_threshold "
            "FROM ( "
            "  SELECT "
            "    keys.p_partkey, "
            "    ( "
            "      SELECT 0.2 * avg(li.l_quantity) "
            "      FROM lineitem li "
            "      WHERE li.l_partkey = keys.p_partkey "
            f"        AND li.l_partkey BETWEEN 1 AND {upper} "
            "    ) AS threshold "
            "  FROM ( "
            "    SELECT DISTINCT l_partkey AS p_partkey "
            "    FROM lineitem "
            f"    WHERE l_partkey BETWEEN 1 AND {upper} "
            "  ) keys "
            ") t "
            "WHERE t.threshold IS NOT NULL"
        )

    if mode == "threshold_grouped_only":
        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.threshold) AS avg_threshold, "
            "  sum(t.threshold) AS sum_threshold "
            "FROM ( "
            "  SELECT "
            "    l_partkey, "
            "    0.2 * avg(l_quantity) AS threshold "
            "  FROM lineitem "
            f"  WHERE l_partkey BETWEEN 1 AND {upper} "
            "  GROUP BY l_partkey "
            ") t"
        )

    assert mode == "q17_predicate"
    escaped_brand = _escape_sql_string(q17_brand)
    escaped_container = _escape_sql_string(q17_container)
    if q17_threshold_mode == "native":
        threshold_predicate = (
            "l.l_quantity < ( "
            "  SELECT 0.2 * avg(li.l_quantity) "
            "  FROM lineitem li "
            "  WHERE li.l_partkey = p.p_partkey "
            f"    AND li.l_partkey BETWEEN 1 AND {upper} "
            ")"
        )
    else:
        assert q17_threshold_mode == "cast_decimal"
        threshold_predicate = (
            f"CAST(l.l_quantity AS {decimal_cast}) < ( "
            f"  SELECT 0.2 * avg(CAST(li.l_quantity AS {decimal_cast})) "
            "  FROM lineitem li "
            "  WHERE li.l_partkey = p.p_partkey "
            f"    AND li.l_partkey BETWEEN 1 AND {upper} "
            ")"
        )

    return (
        "SELECT "
        "  count(*) AS qualifying_rows, "
        "  sum(l.l_extendedprice) / 7.0 AS avg_yearly, "
        "  sum(l.l_extendedprice) AS sum_extendedprice "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        f"WHERE p.p_brand = '{escaped_brand}' "
        f"  AND p.p_container = '{escaped_container}' "
        f"  AND l.l_partkey BETWEEN 1 AND {upper} "
        f"  AND {threshold_predicate}"
    )


def _run_prefix_query(presto_cursor, query):
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


def _print_header(metric1_label, metric2_label):
    print(
        "range_id,lower,upper,"
        "presto_count,duckdb_count,"
        f"presto_{metric1_label},duckdb_{metric1_label},abs_diff_{metric1_label},"
        f"presto_{metric2_label},duckdb_{metric2_label},abs_diff_{metric2_label},"
        "query_seconds,status,major_status",
        flush=True,
    )


def _is_major_mismatch(record, major_decimal_abs_diff, major_double_abs_diff):
    if not record["count_match"]:
        return True

    decimal_diff = record["decimal_diff"]
    double_diff = record["double_diff"]

    if decimal_diff is None or double_diff is None:
        return True

    return (
        decimal_diff > major_decimal_abs_diff
        or double_diff > major_double_abs_diff
    )


def _evaluate_prefix(
    presto_cursor,
    upper,
    mode,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_threshold_mode,
    decimal_abs_tol,
    double_abs_tol,
    major_decimal_abs_diff,
    major_double_abs_diff,
):
    query = _build_prefix_query(
        mode=mode,
        upper=upper,
        decimal_cast=decimal_cast,
        q17_brand=q17_brand,
        q17_container=q17_container,
        q17_threshold_mode=q17_threshold_mode,
    )
    presto_row, duckdb_row = _run_prefix_query(presto_cursor, query)

    count_match = presto_row[0] == duckdb_row[0]
    decimal_diff = _decimal_abs_diff(presto_row[1], duckdb_row[1])
    double_diff = _double_abs_diff(presto_row[2], duckdb_row[2])

    decimal_match = decimal_diff is not None and decimal_diff <= decimal_abs_tol
    double_match = double_diff is not None and double_diff <= double_abs_tol
    is_match = count_match and decimal_match and double_match

    record = {
        "lower": 1,
        "upper": upper,
        "presto_row": presto_row,
        "duckdb_row": duckdb_row,
        "count_match": count_match,
        "decimal_diff": decimal_diff,
        "double_diff": double_diff,
        "status": "MATCH" if is_match else "MISMATCH",
    }
    record["major_mismatch"] = _is_major_mismatch(
        record,
        major_decimal_abs_diff,
        major_double_abs_diff,
    )
    return record


def _print_record(range_id, record):
    presto_row = record["presto_row"]
    duckdb_row = record["duckdb_row"]
    print(
        ",".join(
            [
                str(range_id),
                str(record["lower"]),
                str(record["upper"]),
                _format_value(presto_row[0]),
                _format_value(duckdb_row[0]),
                _format_value(presto_row[1]),
                _format_value(duckdb_row[1]),
                _format_value(record["decimal_diff"]),
                _format_value(presto_row[2]),
                _format_value(duckdb_row[2]),
                _format_value(record["double_diff"]),
                _format_value(f"{record['query_seconds']:.3f}"),
                record["status"],
                "MAJOR_MISMATCH" if record["major_mismatch"] else "NOT_MAJOR",
            ]
        ),
        flush=True,
    )


def _run_scan(
    presto_cursor,
    max_partkey,
    single_upper,
    mode,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_threshold_mode,
    decimal_abs_tol,
    double_abs_tol,
    major_decimal_abs_diff,
    major_double_abs_diff,
    stop_on_mismatch,
):
    records = []
    mismatch_count = 0
    major_mismatch_count = 0
    metric1_label, metric2_label = _get_mode_metric_labels(mode)
    if single_upper is not None:
        uppers = [single_upper]
    else:
        uppers = list(_generate_exponential_prefix_uppers(max_partkey))
    total_ranges = len(uppers)
    scan_start = time.time()
    _print_header(metric1_label, metric2_label)

    for range_id, upper in enumerate(uppers):
        range_start = time.time()
        print(
            "PROGRESS,"
            f"phase=scan,event=start,range_index={range_id + 1}/{total_ranges},"
            f"upper={upper},mode={mode}",
            flush=True,
        )
        record = _evaluate_prefix(
            presto_cursor=presto_cursor,
            upper=upper,
            mode=mode,
            decimal_cast=decimal_cast,
            q17_brand=q17_brand,
            q17_container=q17_container,
            q17_threshold_mode=q17_threshold_mode,
            decimal_abs_tol=decimal_abs_tol,
            double_abs_tol=double_abs_tol,
            major_decimal_abs_diff=major_decimal_abs_diff,
            major_double_abs_diff=major_double_abs_diff,
        )
        record["query_seconds"] = time.time() - range_start
        records.append(record)

        if record["status"] != "MATCH":
            mismatch_count += 1
        if record["major_mismatch"]:
            major_mismatch_count += 1

        _print_record(range_id, record)
        print(
            "PROGRESS,"
            f"phase=scan,event=end,range_index={range_id + 1}/{total_ranges},"
            f"upper={upper},query_seconds={record['query_seconds']:.3f},"
            f"elapsed_seconds={time.time() - scan_start:.3f},mode={mode}",
            flush=True,
        )

        if stop_on_mismatch and record["status"] != "MATCH":
            break

    print(
        "\nSummary: "
        f"ranges_scanned={total_ranges}, mismatches={mismatch_count}, "
        f"major_mismatches={major_mismatch_count}, "
        f"max_partkey={max_partkey}, decimal_cast={decimal_cast}, "
        f"mode={mode}, total_scan_seconds={time.time() - scan_start:.3f}",
        flush=True,
    )
    return records, mismatch_count, major_mismatch_count


def _find_first_major_range(records):
    for idx, record in enumerate(records):
        if record["major_mismatch"]:
            return idx, record
    return None, None


def _refine_smallest_major_upper(
    presto_cursor,
    known_non_major_upper,
    known_major_upper,
    mode,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_threshold_mode,
    decimal_abs_tol,
    double_abs_tol,
    major_decimal_abs_diff,
    major_double_abs_diff,
):
    cache = {}

    def eval_upper(upper):
        if upper not in cache:
            cache[upper] = _evaluate_prefix(
                presto_cursor=presto_cursor,
                upper=upper,
                mode=mode,
                decimal_cast=decimal_cast,
                q17_brand=q17_brand,
                q17_container=q17_container,
                q17_threshold_mode=q17_threshold_mode,
                decimal_abs_tol=decimal_abs_tol,
                double_abs_tol=double_abs_tol,
                major_decimal_abs_diff=major_decimal_abs_diff,
                major_double_abs_diff=major_double_abs_diff,
            )
        return cache[upper]

    lo = known_non_major_upper + 1
    hi = known_major_upper
    smallest_major_upper = known_major_upper
    smallest_major_record = eval_upper(known_major_upper)

    print(
        "Refining smallest major prefix with binary search: "
        f"low={lo}, high={hi}",
        flush=True,
    )

    step = 0
    while lo <= hi:
        step += 1
        mid = (lo + hi) // 2
        step_start = time.time()
        print(
            "PROGRESS,"
            f"phase=refine,event=start,step={step},upper={mid}",
            flush=True,
        )
        mid_record = eval_upper(mid)
        print(
            "BSEARCH,"
            f"upper={mid},"
            f"status={mid_record['status']},"
            f"major_status={'MAJOR_MISMATCH' if mid_record['major_mismatch'] else 'NOT_MAJOR'},"
            f"abs_diff_decimal={_format_value(mid_record['decimal_diff'])},"
            f"abs_diff_double={_format_value(mid_record['double_diff'])},"
            f"query_seconds={time.time() - step_start:.3f}",
            flush=True,
        )
        if mid_record["major_mismatch"]:
            smallest_major_upper = mid
            smallest_major_record = mid_record
            hi = mid - 1
        else:
            lo = mid + 1

    return smallest_major_upper, smallest_major_record


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
        "--mode",
        choices=[
            "q17_predicate",
            "avg_cast",
            "threshold_correlated_only",
            "threshold_grouped_only",
        ],
        default=DEFAULT_MODE,
        help=(
            "Scan mode. q17_predicate reproduces Q17-like correlated threshold "
            "behavior; avg_cast runs simple prefix avg casts; "
            "threshold_correlated_only isolates the correlated threshold subquery; "
            "threshold_grouped_only runs grouped-threshold equivalent."
        ),
    )
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
        "--single-upper",
        type=int,
        help=(
            "Run exactly one prefix upper bound instead of exponential scan "
            "(e.g. --single-upper 19412735)."
        ),
    )
    parser.add_argument(
        "--require-min-max-partkey",
        type=int,
        default=DEFAULT_REQUIRED_MIN_MAX_PARTKEY,
        help=(
            "Require lineitem max(l_partkey) to be at least this value. "
            "Set to 0 to disable."
        ),
    )
    parser.add_argument(
        "--decimal-cast",
        default="DECIMAL(18, 6)",
        help=(
            "Decimal type used for casted avg path. "
            "In q17_predicate mode this is used in threshold comparison."
        ),
    )
    parser.add_argument(
        "--q17-brand",
        default="Brand#23",
        help="Part brand filter for q17_predicate mode.",
    )
    parser.add_argument(
        "--q17-container",
        default="MED BOX",
        help="Part container filter for q17_predicate mode.",
    )
    parser.add_argument(
        "--q17-threshold-mode",
        choices=["native", "cast_decimal"],
        default="native",
        help=(
            "Threshold expression for q17_predicate mode. "
            "native matches Q17 shape; cast_decimal forces decimal avg path."
        ),
    )
    parser.add_argument(
        "--decimal-abs-tol",
        default=DEFAULT_DECIMAL_ABS_TOL,
        help="Absolute tolerance for decimal avg comparisons.",
    )
    parser.add_argument(
        "--double-abs-tol",
        default=1e-12,
        type=float,
        help="Absolute tolerance for double avg comparisons.",
    )
    parser.add_argument(
        "--major-decimal-abs-diff",
        default=str(DEFAULT_MAJOR_DECIMAL_ABS_DIFF),
        help=(
            "Threshold for major decimal mismatch. "
            "Values above this are treated as major."
        ),
    )
    parser.add_argument(
        "--major-double-abs-diff",
        default=DEFAULT_MAJOR_DOUBLE_ABS_DIFF,
        type=float,
        help=(
            "Threshold for major double mismatch. "
            "Values above this are treated as major."
        ),
    )
    parser.add_argument(
        "--skip-refine-smallest-major",
        action="store_true",
        default=False,
        help="Skip binary-search refinement for smallest major prefix.",
    )
    parser.add_argument(
        "--fail-on-any-mismatch",
        action="store_true",
        default=False,
        help="Return non-zero for any mismatch, not just major mismatches.",
    )
    parser.add_argument("--stop-on-mismatch", action="store_true", default=False)
    args = parser.parse_args()

    schema_name = args.schema_name if args.schema_name else DEFAULT_SCHEMA
    should_create_tables = not bool(args.schema_name)
    _progress(
        "phase=main,event=connect_start,"
        f"host={args.hostname},port={args.port},schema={schema_name}"
    )
    conn = prestodb.dbapi.connect(
        host=args.hostname,
        port=args.port,
        user=args.user,
        catalog="hive",
        schema=schema_name,
    )
    cursor = conn.cursor()
    _progress("phase=main,event=connect_end")

    try:
        _setup_tables(cursor, schema_name, should_create_tables)
        _progress("phase=main,event=validate_dataset_start")
        _validate_dataset_scale(
            cursor,
            required_min_max_partkey=args.require_min_max_partkey,
        )
        _progress("phase=main,event=validate_dataset_end")
        _progress("phase=main,event=scan_start")
        records, mismatch_count, major_mismatch_count = _run_scan(
            presto_cursor=cursor,
            max_partkey=args.max_partkey,
            single_upper=args.single_upper,
            mode=args.mode,
            decimal_cast=args.decimal_cast,
            q17_brand=args.q17_brand,
            q17_container=args.q17_container,
            q17_threshold_mode=args.q17_threshold_mode,
            decimal_abs_tol=decimal.Decimal(args.decimal_abs_tol),
            double_abs_tol=args.double_abs_tol,
            major_decimal_abs_diff=decimal.Decimal(args.major_decimal_abs_diff),
            major_double_abs_diff=args.major_double_abs_diff,
            stop_on_mismatch=args.stop_on_mismatch,
        )
        _progress("phase=main,event=scan_end")

        first_major_idx, first_major_record = _find_first_major_range(records)
        smallest_major_upper = None
        if first_major_record is not None:
            print(
                "First exponential major mismatch: "
                f"upper={first_major_record['upper']}, "
                f"abs_diff_decimal={_format_value(first_major_record['decimal_diff'])}, "
                f"abs_diff_double={_format_value(first_major_record['double_diff'])}",
                flush=True,
            )
            if args.single_upper is not None:
                _progress("phase=main,event=refine_skipped,single_upper_mode=true")
            elif not args.skip_refine_smallest_major:
                previous_upper = 0 if first_major_idx == 0 else records[first_major_idx - 1]["upper"]
                smallest_major_upper, smallest_major_record = _refine_smallest_major_upper(
                    presto_cursor=cursor,
                    known_non_major_upper=previous_upper,
                    known_major_upper=first_major_record["upper"],
                    mode=args.mode,
                    decimal_cast=args.decimal_cast,
                    q17_brand=args.q17_brand,
                    q17_container=args.q17_container,
                    q17_threshold_mode=args.q17_threshold_mode,
                    decimal_abs_tol=decimal.Decimal(args.decimal_abs_tol),
                    double_abs_tol=args.double_abs_tol,
                    major_decimal_abs_diff=decimal.Decimal(args.major_decimal_abs_diff),
                    major_double_abs_diff=args.major_double_abs_diff,
                )
                print(
                    "Smallest major mismatch prefix found: "
                    f"l_partkey BETWEEN 1 AND {smallest_major_upper}, "
                    f"presto_count={smallest_major_record['presto_row'][0]}, "
                    f"duckdb_count={smallest_major_record['duckdb_row'][0]}, "
                    f"abs_diff_decimal={_format_value(smallest_major_record['decimal_diff'])}, "
                    f"abs_diff_double={_format_value(smallest_major_record['double_diff'])}",
                    flush=True,
                )
        else:
            print(
                "No major mismatch found in scanned prefix ranges.",
                flush=True,
            )
    finally:
        if should_create_tables and not args.keep_tables:
            create_hive_tables.drop_schema(cursor, schema_name)
        cursor.close()
        conn.close()

    if args.fail_on_any_mismatch and mismatch_count > 0:
        return 1
    if major_mismatch_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
