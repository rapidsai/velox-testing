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
DEFAULT_RANGE_STYLE = "between"
DEFAULT_Q17_FILTER_MODE = "brand_and_container"
DEFAULT_SUBQUERY_KEY_SOURCE = "lineitem"
DEFAULT_SUBQUERY_EXPR_VARIANT = "scaled_avg"
RAW_GROUPED_AVG_BATCH_SIZE = 10000
RAW_GROUPED_AVG_PROGRESS_EVERY = 1000000
DEFAULT_AUTO_FORMS = [
    "subquery_from_table_between_avg_only",
    "subquery_from_table_between_scaled_avg",
    "subquery_from_table_bounds_avg_only",
    "subquery_from_table_bounds_scaled_avg",
    "subquery_lineitem_keys_between",
    "subquery_lineitem_keys_bounds",
    "subquery_part_keys_between_no_filters",
    "subquery_part_keys_bounds_no_filters",
    "subquery_part_keys_between_brand_only",
    "subquery_part_keys_bounds_brand_only",
    "subquery_part_keys_between_full",
    "subquery_part_keys_bounds_full",
    "subquery_part_keys_between_full_cast_decimal",
    "subquery_part_keys_bounds_full_cast_decimal",
]
AUTO_FORM_DEFINITIONS = {
    "grouped_avg_double_only": {
        "mode": "grouped_avg_double_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_only": {
        "mode": "grouped_avg_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_cast_decimal_only": {
        "mode": "grouped_avg_cast_decimal_only",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_cast_decimal_only_raw": {
        "mode": "grouped_avg_cast_decimal_only_raw",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "threshold_grouped_only": {
        "mode": "threshold_grouped_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "threshold_correlated_only": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "q17_predicate_native": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "q17_predicate_cast_decimal": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_double_only_between": {
        "mode": "grouped_avg_double_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_double_only_bounds": {
        "mode": "grouped_avg_double_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "grouped_avg_only_between": {
        "mode": "grouped_avg_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_only_bounds": {
        "mode": "grouped_avg_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "grouped_avg_cast_decimal_only_between": {
        "mode": "grouped_avg_cast_decimal_only",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_cast_decimal_only_bounds": {
        "mode": "grouped_avg_cast_decimal_only",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "grouped_avg_cast_decimal_only_raw_between": {
        "mode": "grouped_avg_cast_decimal_only_raw",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "grouped_avg_cast_decimal_only_raw_bounds": {
        "mode": "grouped_avg_cast_decimal_only_raw",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "threshold_grouped_only_between": {
        "mode": "threshold_grouped_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "threshold_grouped_only_bounds": {
        "mode": "threshold_grouped_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "threshold_correlated_only_between": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "threshold_correlated_only_bounds": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "q17_predicate_native_between_no_filters": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "between",
    },
    "q17_predicate_native_bounds_no_filters": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "bounds",
    },
    "q17_predicate_native_between_brand_only": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_only",
        "range_style": "between",
    },
    "q17_predicate_native_bounds_brand_only": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_only",
        "range_style": "bounds",
    },
    "q17_predicate_native_between_full": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
    },
    "q17_predicate_native_bounds_full": {
        "mode": "q17_predicate",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
    },
    "subquery_from_table_between_avg_only": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "between",
        "subquery_key_source": "lineitem_grouped",
        "subquery_expr_variant": "avg_only",
    },
    "subquery_from_table_between_scaled_avg": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "between",
        "subquery_key_source": "lineitem_grouped",
        "subquery_expr_variant": "scaled_avg",
    },
    "subquery_from_table_bounds_avg_only": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "bounds",
        "subquery_key_source": "lineitem_grouped",
        "subquery_expr_variant": "avg_only",
    },
    "subquery_from_table_bounds_scaled_avg": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "bounds",
        "subquery_key_source": "lineitem_grouped",
        "subquery_expr_variant": "scaled_avg",
    },
    "subquery_lineitem_keys_between": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "between",
        "subquery_key_source": "lineitem",
    },
    "subquery_lineitem_keys_bounds": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "bounds",
        "subquery_key_source": "lineitem",
    },
    "subquery_part_keys_between_no_filters": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "between",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_bounds_no_filters": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "none",
        "range_style": "bounds",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_between_brand_only": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_only",
        "range_style": "between",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_bounds_brand_only": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_only",
        "range_style": "bounds",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_between_full": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_bounds_full": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "native",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_between_full_cast_decimal": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "between",
        "subquery_key_source": "part",
    },
    "subquery_part_keys_bounds_full_cast_decimal": {
        "mode": "threshold_correlated_only",
        "q17_threshold_mode": "cast_decimal",
        "q17_filter_mode": "brand_and_container",
        "range_style": "bounds",
        "subquery_key_source": "part",
    },
}


def get_abs_file_path(relative_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def _progress(message):
    print(f"PROGRESS,{message}", flush=True)


def _escape_sql_string(value):
    return value.replace("'", "''")


def _range_predicate(column_name, upper, range_style):
    if range_style == "between":
        return f"{column_name} BETWEEN 1 AND {upper}"
    assert range_style == "bounds"
    return f"{column_name} >= 1 AND {column_name} <= {upper}"


def _q17_filter_predicate(
    q17_filter_mode,
    escaped_brand,
    escaped_container,
):
    if q17_filter_mode == "none":
        return "1 = 1"
    if q17_filter_mode == "brand_only":
        return f"p.p_brand = '{escaped_brand}'"
    assert q17_filter_mode == "brand_and_container"
    return (
        f"p.p_brand = '{escaped_brand}' "
        f"AND p.p_container = '{escaped_container}'"
    )


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
    if mode == "grouped_avg_only":
        return "avg_group_avg", "sum_group_avg"
    if mode == "grouped_avg_cast_decimal_only":
        return "avg_group_avg", "sum_group_avg"
    if mode == "grouped_avg_cast_decimal_only_raw":
        return "avg_group_avg", "sum_group_avg"
    if mode == "grouped_avg_double_only":
        return "avg_group_avg", "sum_group_avg"
    assert mode == "q17_predicate"
    return "avg_yearly", "sum_extendedprice"


def _build_prefix_query(
    mode,
    upper,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_threshold_mode,
    q17_filter_mode,
    range_style,
    subquery_key_source,
    subquery_expr_variant,
):
    lineitem_range = _range_predicate("l.l_partkey", upper, range_style)
    li_range = _range_predicate("li.l_partkey", upper, range_style)
    lineitem_key_range = _range_predicate("l_partkey", upper, range_style)
    part_key_range = _range_predicate("p.p_partkey", upper, range_style)

    if mode == "avg_cast":
        return (
            "SELECT "
            "  count(*) AS row_count, "
            f"  avg(CAST(l_quantity AS {decimal_cast})) AS avg_qty_decimal, "
            "  avg(CAST(l_quantity AS DOUBLE)) AS avg_qty_double "
            "FROM lineitem "
            f"WHERE {lineitem_key_range}"
        )

    if mode == "threshold_correlated_only":
        escaped_brand = _escape_sql_string(q17_brand)
        escaped_container = _escape_sql_string(q17_container)
        part_filter = _q17_filter_predicate(
            q17_filter_mode=q17_filter_mode,
            escaped_brand=escaped_brand,
            escaped_container=escaped_container,
        )
        if subquery_key_source == "part":
            key_source = (
                "SELECT p.p_partkey AS p_partkey "
                "FROM part p "
                f"WHERE {part_key_range} "
                f"  AND {part_filter} "
            )
        elif subquery_key_source == "lineitem_grouped":
            key_source = (
                "SELECT l.l_partkey AS p_partkey "
                "FROM lineitem l "
                f"WHERE {lineitem_range} "
                "GROUP BY l.l_partkey "
            )
        else:
            assert subquery_key_source == "lineitem"
            key_source = (
                "SELECT DISTINCT l_partkey AS p_partkey "
                "FROM lineitem "
                f"WHERE {lineitem_key_range} "
            )

        if q17_threshold_mode == "native":
            avg_expr = "avg(li.l_quantity)"
            if subquery_expr_variant == "scaled_avg":
                subquery_value_expr = "0.2 * " + avg_expr
            else:
                assert subquery_expr_variant == "avg_only"
                subquery_value_expr = avg_expr
            threshold_subquery = (
                f"SELECT {subquery_value_expr} "
                "FROM lineitem li "
                "WHERE li.l_partkey = keys.p_partkey "
                f"  AND {li_range} "
            )
        else:
            assert q17_threshold_mode == "cast_decimal"
            avg_expr = f"avg(CAST(li.l_quantity AS {decimal_cast}))"
            if subquery_expr_variant == "scaled_avg":
                subquery_value_expr = "0.2 * " + avg_expr
            else:
                assert subquery_expr_variant == "avg_only"
                subquery_value_expr = avg_expr
            threshold_subquery = (
                f"SELECT {subquery_value_expr} "
                "FROM lineitem li "
                "WHERE li.l_partkey = keys.p_partkey "
                f"  AND {li_range} "
            )

        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.threshold) AS avg_threshold, "
            "  sum(t.threshold) AS sum_threshold "
            "FROM ( "
            "  SELECT "
            "    keys.p_partkey, "
            "    ( "
            f"      {threshold_subquery}"
            "    ) AS threshold "
            "  FROM ( " + key_source + " ) keys "
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
            f"  WHERE {lineitem_key_range} "
            "  GROUP BY l_partkey "
            ") t"
        )

    if mode == "grouped_avg_only":
        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.group_avg) AS avg_group_avg, "
            "  sum(t.group_avg) AS sum_group_avg "
            "FROM ( "
            "  SELECT "
            "    l_partkey, "
            "    avg(l_quantity) AS group_avg "
            "  FROM lineitem "
            f"  WHERE {lineitem_key_range} "
            "  GROUP BY l_partkey "
            ") t"
        )

    if mode == "grouped_avg_cast_decimal_only":
        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.group_avg) AS avg_group_avg, "
            "  sum(t.group_avg) AS sum_group_avg "
            "FROM ( "
            "  SELECT "
            "    l_partkey, "
            f"    avg(CAST(l_quantity AS {decimal_cast})) AS group_avg "
            "  FROM lineitem "
            f"  WHERE {lineitem_key_range} "
            "  GROUP BY l_partkey "
            ") t"
        )

    if mode == "grouped_avg_cast_decimal_only_raw":
        return (
            "SELECT "
            "  l_partkey, "
            f"  avg(CAST(l_quantity AS {decimal_cast})) AS threshold "
            "FROM lineitem "
            f"WHERE {lineitem_key_range} "
            "GROUP BY l_partkey"
        )

    if mode == "grouped_avg_double_only":
        return (
            "SELECT "
            "  count(*) AS key_count, "
            "  avg(t.group_avg) AS avg_group_avg, "
            "  sum(t.group_avg) AS sum_group_avg "
            "FROM ( "
            "  SELECT "
            "    l_partkey, "
            "    avg(CAST(l_quantity AS DOUBLE)) AS group_avg "
            "  FROM lineitem "
            f"  WHERE {lineitem_key_range} "
            "  GROUP BY l_partkey "
            ") t"
        )

    assert mode == "q17_predicate"
    escaped_brand = _escape_sql_string(q17_brand)
    escaped_container = _escape_sql_string(q17_container)
    q17_filter = _q17_filter_predicate(
        q17_filter_mode=q17_filter_mode,
        escaped_brand=escaped_brand,
        escaped_container=escaped_container,
    )
    if q17_threshold_mode == "native":
        threshold_predicate = (
            "l.l_quantity < ( "
            "  SELECT 0.2 * avg(li.l_quantity) "
            "  FROM lineitem li "
            "  WHERE li.l_partkey = p.p_partkey "
            f"    AND {li_range} "
            ")"
        )
    else:
        assert q17_threshold_mode == "cast_decimal"
        threshold_predicate = (
            f"CAST(l.l_quantity AS {decimal_cast}) < ( "
            f"  SELECT 0.2 * avg(CAST(li.l_quantity AS {decimal_cast})) "
            "  FROM lineitem li "
            "  WHERE li.l_partkey = p.p_partkey "
            f"    AND {li_range} "
            ")"
        )

    return (
        "SELECT "
        "  count(*) AS qualifying_rows, "
        "  sum(l.l_extendedprice) / 7.0 AS avg_yearly, "
        "  sum(l.l_extendedprice) AS sum_extendedprice "
        "FROM lineitem l "
        "JOIN part p ON p.p_partkey = l.l_partkey "
        f"WHERE {q17_filter} "
        f"  AND {lineitem_range} "
        f"  AND {threshold_predicate}"
    )


def _run_prefix_query(presto_cursor, query):
    presto_row = presto_cursor.execute(query).fetchone()
    duckdb_row = duckdb.sql(query).fetchone()
    return presto_row, duckdb_row


def _summarize_grouped_avg_rows(fetch_batch, source_label):
    total_groups = 0
    non_null_count = 0
    sum_values = decimal.Decimal("0")
    next_progress = RAW_GROUPED_AVG_PROGRESS_EVERY
    while True:
        rows = fetch_batch()
        if not rows:
            break
        for row in rows:
            total_groups += 1
            value = row[1]
            if value is None:
                continue
            non_null_count += 1
            sum_values += _to_decimal(value)
        if total_groups >= next_progress:
            print(
                "PROGRESS,"
                f"phase=grouped_avg_raw,event=rows,"
                f"source={source_label},rows={total_groups}",
                flush=True,
            )
            next_progress += RAW_GROUPED_AVG_PROGRESS_EVERY
    avg_value = None
    if non_null_count > 0:
        avg_value = sum_values / decimal.Decimal(str(non_null_count))
    return total_groups, avg_value, sum_values


def _run_grouped_avg_raw_summary(presto_cursor, query):
    presto_cursor.execute(query)

    def presto_fetch():
        return presto_cursor.fetchmany(RAW_GROUPED_AVG_BATCH_SIZE)

    presto_row = _summarize_grouped_avg_rows(presto_fetch, "presto")
    duckdb_rel = duckdb.sql(query)

    def duckdb_fetch():
        return duckdb_rel.fetchmany(RAW_GROUPED_AVG_BATCH_SIZE)

    duckdb_row = _summarize_grouped_avg_rows(duckdb_fetch, "duckdb")
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


def _format_error(exc):
    message = str(exc).replace("\n", " ").replace("\r", " ").strip()
    message = message.replace(",", ";")
    if len(message) > 500:
        return message[:497] + "..."
    return message


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
    q17_filter_mode,
    range_style,
    subquery_key_source,
    subquery_expr_variant,
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
        q17_filter_mode=q17_filter_mode,
        range_style=range_style,
        subquery_key_source=subquery_key_source,
        subquery_expr_variant=subquery_expr_variant,
    )
    if mode == "grouped_avg_cast_decimal_only_raw":
        presto_row, duckdb_row = _run_grouped_avg_raw_summary(
            presto_cursor, query
        )
    else:
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
    q17_filter_mode,
    range_style,
    subquery_key_source,
    subquery_expr_variant,
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
            q17_filter_mode=q17_filter_mode,
            range_style=range_style,
            subquery_key_source=subquery_key_source,
            subquery_expr_variant=subquery_expr_variant,
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
    q17_filter_mode,
    range_style,
    subquery_key_source,
    subquery_expr_variant,
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
                q17_filter_mode=q17_filter_mode,
                range_style=range_style,
                subquery_key_source=subquery_key_source,
                subquery_expr_variant=subquery_expr_variant,
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


def _resolve_auto_forms(auto_forms_arg):
    if auto_forms_arg:
        normalized = re.sub(r"\s+", "", auto_forms_arg)
        form_names = [token for token in normalized.split(",") if token]
    else:
        form_names = list(DEFAULT_AUTO_FORMS)

    if not form_names:
        raise ValueError("No auto forms specified.")

    forms = []
    for form_name in form_names:
        if form_name not in AUTO_FORM_DEFINITIONS:
            valid = ", ".join(sorted(AUTO_FORM_DEFINITIONS.keys()))
            raise ValueError(
                f"Invalid auto form '{form_name}'. Valid forms: {valid}"
            )
        form = {"name": form_name}
        form.update(AUTO_FORM_DEFINITIONS[form_name])
        if "q17_filter_mode" not in form:
            form["q17_filter_mode"] = DEFAULT_Q17_FILTER_MODE
        if "range_style" not in form:
            form["range_style"] = DEFAULT_RANGE_STYLE
        if "subquery_key_source" not in form:
            form["subquery_key_source"] = DEFAULT_SUBQUERY_KEY_SOURCE
        if "subquery_expr_variant" not in form:
            form["subquery_expr_variant"] = DEFAULT_SUBQUERY_EXPR_VARIANT
        forms.append(form)
    return forms


def _run_auto_simplify(
    presto_cursor,
    upper,
    forms,
    decimal_cast,
    q17_brand,
    q17_container,
    q17_filter_mode,
    range_style,
    subquery_key_source,
    subquery_expr_variant,
    decimal_abs_tol,
    double_abs_tol,
    major_decimal_abs_diff,
    major_double_abs_diff,
):
    print(
        "\nAUTO_SIMPLIFY,target_upper="
        f"{upper},forms={','.join(form['name'] for form in forms)}",
        flush=True,
    )
    print(
        "auto_index,form_name,mode,q17_threshold_mode,q17_filter_mode,range_style,subquery_key_source,subquery_expr_variant,upper,"
        "presto_count,duckdb_count,"
        "metric1_label,metric2_label,"
        "abs_diff_metric1,abs_diff_metric2,"
        "status,major_status,query_seconds,result,error",
        flush=True,
    )

    results = []
    for index, form in enumerate(forms, start=1):
        mode = form["mode"]
        q17_threshold_mode = form["q17_threshold_mode"]
        form_q17_filter_mode = form.get("q17_filter_mode", q17_filter_mode)
        form_range_style = form.get("range_style", range_style)
        form_subquery_key_source = form.get(
            "subquery_key_source",
            subquery_key_source,
        )
        form_subquery_expr_variant = form.get(
            "subquery_expr_variant",
            subquery_expr_variant,
        )
        metric1_label, metric2_label = _get_mode_metric_labels(mode)
        _progress(
            "phase=auto_simplify,event=start,"
            f"index={index}/{len(forms)},form={form['name']},upper={upper}"
        )
        start = time.time()
        try:
            record = _evaluate_prefix(
                presto_cursor=presto_cursor,
                upper=upper,
                mode=mode,
                decimal_cast=decimal_cast,
                q17_brand=q17_brand,
                q17_container=q17_container,
                q17_threshold_mode=q17_threshold_mode,
                q17_filter_mode=form_q17_filter_mode,
                range_style=form_range_style,
                subquery_key_source=form_subquery_key_source,
                subquery_expr_variant=form_subquery_expr_variant,
                decimal_abs_tol=decimal_abs_tol,
                double_abs_tol=double_abs_tol,
                major_decimal_abs_diff=major_decimal_abs_diff,
                major_double_abs_diff=major_double_abs_diff,
            )
            query_seconds = time.time() - start
            print(
                ",".join(
                    [
                        str(index),
                        form["name"],
                        mode,
                        q17_threshold_mode,
                        form_q17_filter_mode,
                        form_range_style,
                        form_subquery_key_source,
                        form_subquery_expr_variant,
                        str(upper),
                        _format_value(record["presto_row"][0]),
                        _format_value(record["duckdb_row"][0]),
                        metric1_label,
                        metric2_label,
                        _format_value(record["decimal_diff"]),
                        _format_value(record["double_diff"]),
                        record["status"],
                        "MAJOR_MISMATCH" if record["major_mismatch"] else "NOT_MAJOR",
                        f"{query_seconds:.3f}",
                        "OK",
                        "",
                    ]
                ),
                flush=True,
            )
            _progress(
                "phase=auto_simplify,event=end,"
                f"index={index}/{len(forms)},form={form['name']},"
                f"query_seconds={query_seconds:.3f}"
            )
            results.append(
                {
                    "form_name": form["name"],
                    "mode": mode,
                    "q17_threshold_mode": q17_threshold_mode,
                    "q17_filter_mode": form_q17_filter_mode,
                    "range_style": form_range_style,
                    "subquery_key_source": form_subquery_key_source,
                    "subquery_expr_variant": form_subquery_expr_variant,
                    "record": record,
                    "error": None,
                    "query_seconds": query_seconds,
                }
            )
        except Exception as exc:
            query_seconds = time.time() - start
            error_message = _format_error(exc)
            print(
                ",".join(
                    [
                        str(index),
                        form["name"],
                        mode,
                        q17_threshold_mode,
                        form_q17_filter_mode,
                        form_range_style,
                        form_subquery_key_source,
                        form_subquery_expr_variant,
                        str(upper),
                        "NULL",
                        "NULL",
                        metric1_label,
                        metric2_label,
                        "NULL",
                        "NULL",
                        "ERROR",
                        "ERROR",
                        f"{query_seconds:.3f}",
                        "ERROR",
                        error_message,
                    ]
                ),
                flush=True,
            )
            _progress(
                "phase=auto_simplify,event=error,"
                f"index={index}/{len(forms)},form={form['name']},"
                f"query_seconds={query_seconds:.3f}"
            )
            results.append(
                {
                    "form_name": form["name"],
                    "mode": mode,
                    "q17_threshold_mode": q17_threshold_mode,
                    "q17_filter_mode": form_q17_filter_mode,
                    "range_style": form_range_style,
                    "subquery_key_source": form_subquery_key_source,
                    "subquery_expr_variant": form_subquery_expr_variant,
                    "record": None,
                    "error": error_message,
                    "query_seconds": query_seconds,
                }
            )

    major_results = [
        result
        for result in results
        if result["record"] is not None and result["record"]["major_mismatch"]
    ]

    first_major_in_order = major_results[0] if major_results else None
    tested_subquery_only = any(
        result["mode"] == "threshold_correlated_only" for result in results
    )
    tested_correlated = any(
        result["mode"] in ("threshold_correlated_only", "q17_predicate")
        for result in results
    )
    tested_q17 = any(result["mode"] == "q17_predicate" for result in results)

    first_subquery_only = next(
        (result for result in major_results if result["mode"] == "threshold_correlated_only"),
        None,
    )
    first_correlated = next(
        (
            result
            for result in major_results
            if result["mode"] in ("threshold_correlated_only", "q17_predicate")
        ),
        None,
    )
    first_q17 = next(
        (result for result in major_results if result["mode"] == "q17_predicate"),
        None,
    )

    if first_major_in_order:
        print(
            "AUTO_SIMPLIFY_RESULT_FIRST_IN_ORDER,"
            f"form={first_major_in_order['form_name']},"
            f"mode={first_major_in_order['mode']},"
            f"q17_threshold_mode={first_major_in_order['q17_threshold_mode']},"
            f"q17_filter_mode={first_major_in_order['q17_filter_mode']},"
            f"range_style={first_major_in_order['range_style']},"
            f"subquery_key_source={first_major_in_order['subquery_key_source']},"
            f"subquery_expr_variant={first_major_in_order['subquery_expr_variant']},"
            f"upper={upper},"
            f"abs_diff_metric1={_format_value(first_major_in_order['record']['decimal_diff'])},"
            f"abs_diff_metric2={_format_value(first_major_in_order['record']['double_diff'])}",
            flush=True,
        )
        print(
            "AUTO_SIMPLIFY_NOTE,"
            "first_in_order_depends_on_auto_forms_ordering=true",
            flush=True,
        )
    else:
        print(
            "AUTO_SIMPLIFY_RESULT_FIRST_IN_ORDER,none",
            flush=True,
        )

    if first_subquery_only:
        print(
            "AUTO_SIMPLIFY_RESULT_SUBQUERY_ONLY,"
            f"form={first_subquery_only['form_name']},"
            f"mode={first_subquery_only['mode']},"
            f"q17_threshold_mode={first_subquery_only['q17_threshold_mode']},"
            f"q17_filter_mode={first_subquery_only['q17_filter_mode']},"
            f"range_style={first_subquery_only['range_style']},"
            f"subquery_key_source={first_subquery_only['subquery_key_source']},"
            f"subquery_expr_variant={first_subquery_only['subquery_expr_variant']},"
            f"upper={upper},"
            f"abs_diff_metric1={_format_value(first_subquery_only['record']['decimal_diff'])},"
            f"abs_diff_metric2={_format_value(first_subquery_only['record']['double_diff'])}",
            flush=True,
        )
    else:
        if tested_subquery_only:
            print(
                "AUTO_SIMPLIFY_RESULT_SUBQUERY_ONLY,tested_no_major_repro",
                flush=True,
            )
        else:
            print(
                "AUTO_SIMPLIFY_RESULT_SUBQUERY_ONLY,not_tested",
                flush=True,
            )

    if first_correlated:
        print(
            "AUTO_SIMPLIFY_RESULT_CORRELATED,"
            f"form={first_correlated['form_name']},"
            f"mode={first_correlated['mode']},"
            f"q17_threshold_mode={first_correlated['q17_threshold_mode']},"
            f"q17_filter_mode={first_correlated['q17_filter_mode']},"
            f"range_style={first_correlated['range_style']},"
            f"subquery_key_source={first_correlated['subquery_key_source']},"
            f"subquery_expr_variant={first_correlated['subquery_expr_variant']},"
            f"upper={upper},"
            f"abs_diff_metric1={_format_value(first_correlated['record']['decimal_diff'])},"
            f"abs_diff_metric2={_format_value(first_correlated['record']['double_diff'])}",
            flush=True,
        )
    else:
        if tested_correlated:
            print(
                "AUTO_SIMPLIFY_RESULT_CORRELATED,tested_no_major_repro",
                flush=True,
            )
        else:
            print(
                "AUTO_SIMPLIFY_RESULT_CORRELATED,not_tested",
                flush=True,
            )

    if first_q17:
        print(
            "AUTO_SIMPLIFY_RESULT_Q17,"
            f"form={first_q17['form_name']},"
            f"mode={first_q17['mode']},"
            f"q17_threshold_mode={first_q17['q17_threshold_mode']},"
            f"q17_filter_mode={first_q17['q17_filter_mode']},"
            f"range_style={first_q17['range_style']},"
            f"subquery_key_source={first_q17['subquery_key_source']},"
            f"subquery_expr_variant={first_q17['subquery_expr_variant']},"
            f"upper={upper},"
            f"abs_diff_metric1={_format_value(first_q17['record']['decimal_diff'])},"
            f"abs_diff_metric2={_format_value(first_q17['record']['double_diff'])}",
            flush=True,
        )
    else:
        if tested_q17:
            print("AUTO_SIMPLIFY_RESULT_Q17,tested_no_major_repro", flush=True)
        else:
            print("AUTO_SIMPLIFY_RESULT_Q17,not_tested", flush=True)

    repro_results = [
        result
        for result in results
        if result["record"] is not None and result["record"]["major_mismatch"]
    ]
    non_repro_results = [
        result
        for result in results
        if result["record"] is not None and not result["record"]["major_mismatch"]
    ]
    error_results = [result for result in results if result["record"] is None]

    print(
        "AUTO_SIMPLIFY_FINAL_COUNTS,"
        f"repro_forms={len(repro_results)},"
        f"non_repro_forms={len(non_repro_results)},"
        f"error_forms={len(error_results)}",
        flush=True,
    )

    if repro_results:
        print(
            "AUTO_SIMPLIFY_REPRO_FORMS,"
            + ";".join(
                [
                    (
                        f"{result['form_name']}("
                        f"mode={result['mode']},"
                        f"q17_threshold_mode={result['q17_threshold_mode']},"
                        f"q17_filter_mode={result['q17_filter_mode']},"
                        f"range_style={result['range_style']},"
                        f"subquery_key_source={result['subquery_key_source']},"
                        f"subquery_expr_variant={result['subquery_expr_variant']},"
                        f"abs_diff_metric1={_format_value(result['record']['decimal_diff'])},"
                        f"abs_diff_metric2={_format_value(result['record']['double_diff'])}"
                        ")"
                    )
                    for result in repro_results
                ]
            ),
            flush=True,
        )
    else:
        print("AUTO_SIMPLIFY_REPRO_FORMS,none", flush=True)

    if non_repro_results:
        print(
            "AUTO_SIMPLIFY_NON_REPRO_FORMS,"
            + ";".join(
                [
                    (
                        f"{result['form_name']}("
                        f"mode={result['mode']},"
                        f"q17_threshold_mode={result['q17_threshold_mode']},"
                        f"q17_filter_mode={result['q17_filter_mode']},"
                        f"range_style={result['range_style']},"
                        f"subquery_key_source={result['subquery_key_source']},"
                        f"subquery_expr_variant={result['subquery_expr_variant']},"
                        f"abs_diff_metric1={_format_value(result['record']['decimal_diff'])},"
                        f"abs_diff_metric2={_format_value(result['record']['double_diff'])}"
                        ")"
                    )
                    for result in non_repro_results
                ]
            ),
            flush=True,
        )
    else:
        print("AUTO_SIMPLIFY_NON_REPRO_FORMS,none", flush=True)

    if error_results:
        print(
            "AUTO_SIMPLIFY_ERROR_FORMS,"
            + ";".join(
                [
                    (
                        f"{result['form_name']}("
                        f"mode={result['mode']},"
                        f"q17_threshold_mode={result['q17_threshold_mode']},"
                        f"q17_filter_mode={result['q17_filter_mode']},"
                        f"range_style={result['range_style']},"
                        f"subquery_key_source={result['subquery_key_source']},"
                        f"subquery_expr_variant={result['subquery_expr_variant']},"
                        f"error={result['error']}"
                        ")"
                    )
                    for result in error_results
                ]
            ),
            flush=True,
        )
    else:
        print("AUTO_SIMPLIFY_ERROR_FORMS,none", flush=True)

    return results, first_major_in_order


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
            "grouped_avg_only",
            "grouped_avg_cast_decimal_only",
            "grouped_avg_cast_decimal_only_raw",
            "grouped_avg_double_only",
        ],
        default=DEFAULT_MODE,
        help=(
            "Scan mode. q17_predicate reproduces Q17-like correlated threshold "
            "behavior; avg_cast runs simple prefix avg casts; "
            "threshold_correlated_only isolates the correlated threshold subquery; "
            "threshold_grouped_only runs grouped-threshold equivalent; "
            "grouped_avg_only isolates grouped avg(l_quantity); "
            "grouped_avg_cast_decimal_only is grouped avg(CAST(l_quantity AS DECIMAL)); "
            "grouped_avg_cast_decimal_only_raw runs the grouped decimal avg query "
            "directly (no outer aggregation) and summarizes results in Python; "
            "grouped_avg_double_only is grouped avg(CAST(l_quantity AS DOUBLE)). "
            "Use --q17-filter-mode and --range-style to strip filters or swap "
            "BETWEEN for >=/<= in applicable modes."
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
        "--q17-filter-mode",
        choices=["none", "brand_only", "brand_and_container"],
        default=DEFAULT_Q17_FILTER_MODE,
        help=(
            "Filter set for q17_predicate mode: no filters, brand only, "
            "or brand+container (Q17 default)."
        ),
    )
    parser.add_argument(
        "--range-style",
        choices=["between", "bounds"],
        default=DEFAULT_RANGE_STYLE,
        help=(
            "Range predicate style for l_partkey constraints: "
            "'between' uses BETWEEN 1 AND upper, "
            "'bounds' uses >= 1 AND <= upper."
        ),
    )
    parser.add_argument(
        "--subquery-key-source",
        choices=["lineitem", "lineitem_grouped", "part"],
        default=DEFAULT_SUBQUERY_KEY_SOURCE,
        help=(
            "Key source for threshold_correlated_only mode: "
            "'lineitem' uses DISTINCT keys from lineitem, "
            "'lineitem_grouped' uses GROUP BY keys from lineitem, "
            "'part' uses keys from part with q17-style filters."
        ),
    )
    parser.add_argument(
        "--subquery-expr-variant",
        choices=["scaled_avg", "avg_only"],
        default=DEFAULT_SUBQUERY_EXPR_VARIANT,
        help=(
            "Expression variant for threshold_correlated_only mode: "
            "'scaled_avg' uses 0.2 * avg(...), "
            "'avg_only' uses avg(...) without multiplication."
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
    parser.add_argument(
        "--auto-simplify",
        action="store_true",
        default=False,
        help=(
            "Automatically test multiple simplified forms and report the "
            "simplest form that still reproduces a major mismatch."
        ),
    )
    parser.add_argument(
        "--auto-forms",
        help=(
            "Comma-separated auto simplify form names. "
            "Default order: "
            + ",".join(DEFAULT_AUTO_FORMS)
        ),
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

    decimal_abs_tol = decimal.Decimal(args.decimal_abs_tol)
    major_decimal_abs_diff = decimal.Decimal(args.major_decimal_abs_diff)
    saw_any_mismatch = False
    saw_any_major = False
    saw_any_error = False

    try:
        _setup_tables(cursor, schema_name, should_create_tables)
        _progress("phase=main,event=validate_dataset_start")
        _validate_dataset_scale(
            cursor,
            required_min_max_partkey=args.require_min_max_partkey,
        )
        _progress("phase=main,event=validate_dataset_end")
        if args.auto_simplify:
            target_upper = args.single_upper
            if target_upper is None:
                _progress("phase=main,event=scan_start")
                records, mismatch_count, major_mismatch_count = _run_scan(
                    presto_cursor=cursor,
                    max_partkey=args.max_partkey,
                    single_upper=None,
                    mode=args.mode,
                    decimal_cast=args.decimal_cast,
                    q17_brand=args.q17_brand,
                    q17_container=args.q17_container,
                    q17_threshold_mode=args.q17_threshold_mode,
                    q17_filter_mode=args.q17_filter_mode,
                    range_style=args.range_style,
                    subquery_key_source=args.subquery_key_source,
                    subquery_expr_variant=args.subquery_expr_variant,
                    decimal_abs_tol=decimal_abs_tol,
                    double_abs_tol=args.double_abs_tol,
                    major_decimal_abs_diff=major_decimal_abs_diff,
                    major_double_abs_diff=args.major_double_abs_diff,
                    stop_on_mismatch=args.stop_on_mismatch,
                )
                _progress("phase=main,event=scan_end")
                saw_any_mismatch = saw_any_mismatch or mismatch_count > 0
                saw_any_major = saw_any_major or major_mismatch_count > 0

                first_major_idx, first_major_record = _find_first_major_range(records)
                if first_major_record is None:
                    print(
                        "No major mismatch found in scanned prefix ranges. "
                        "Skipping auto-simplify forms.",
                        flush=True,
                    )
                else:
                    target_upper = first_major_record["upper"]
                    print(
                        "Auto-simplify discovery found major mismatch at "
                        f"upper={target_upper}",
                        flush=True,
                    )
                    if not args.skip_refine_smallest_major:
                        previous_upper = (
                            0 if first_major_idx == 0 else records[first_major_idx - 1]["upper"]
                        )
                        target_upper, smallest_major_record = _refine_smallest_major_upper(
                            presto_cursor=cursor,
                            known_non_major_upper=previous_upper,
                            known_major_upper=first_major_record["upper"],
                            mode=args.mode,
                            decimal_cast=args.decimal_cast,
                            q17_brand=args.q17_brand,
                            q17_container=args.q17_container,
                            q17_threshold_mode=args.q17_threshold_mode,
                            q17_filter_mode=args.q17_filter_mode,
                            range_style=args.range_style,
                            subquery_key_source=args.subquery_key_source,
                            subquery_expr_variant=args.subquery_expr_variant,
                            decimal_abs_tol=decimal_abs_tol,
                            double_abs_tol=args.double_abs_tol,
                            major_decimal_abs_diff=major_decimal_abs_diff,
                            major_double_abs_diff=args.major_double_abs_diff,
                        )
                        print(
                            "Auto-simplify target upper refined to "
                            f"{target_upper} "
                            f"(abs_diff_metric1={_format_value(smallest_major_record['decimal_diff'])}, "
                            f"abs_diff_metric2={_format_value(smallest_major_record['double_diff'])})",
                            flush=True,
                        )
                    else:
                        _progress("phase=main,event=refine_skipped,reason=flag")
            else:
                _progress(
                    f"phase=main,event=auto_target_upper_from_arg,upper={target_upper}"
                )

            if target_upper is not None:
                forms = _resolve_auto_forms(args.auto_forms)
                auto_results, _ = _run_auto_simplify(
                    presto_cursor=cursor,
                    upper=target_upper,
                    forms=forms,
                    decimal_cast=args.decimal_cast,
                    q17_brand=args.q17_brand,
                    q17_container=args.q17_container,
                    q17_filter_mode=args.q17_filter_mode,
                    range_style=args.range_style,
                    subquery_key_source=args.subquery_key_source,
                    subquery_expr_variant=args.subquery_expr_variant,
                    decimal_abs_tol=decimal_abs_tol,
                    double_abs_tol=args.double_abs_tol,
                    major_decimal_abs_diff=major_decimal_abs_diff,
                    major_double_abs_diff=args.major_double_abs_diff,
                )
                saw_any_mismatch = saw_any_mismatch or any(
                    (result["record"] is None)
                    or (result["record"]["status"] != "MATCH")
                    for result in auto_results
                )
                saw_any_major = saw_any_major or any(
                    result["record"] is not None and result["record"]["major_mismatch"]
                    for result in auto_results
                )
                saw_any_error = saw_any_error or any(
                    result["record"] is None for result in auto_results
                )
        else:
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
                q17_filter_mode=args.q17_filter_mode,
                range_style=args.range_style,
                subquery_key_source=args.subquery_key_source,
                subquery_expr_variant=args.subquery_expr_variant,
                decimal_abs_tol=decimal_abs_tol,
                double_abs_tol=args.double_abs_tol,
                major_decimal_abs_diff=major_decimal_abs_diff,
                major_double_abs_diff=args.major_double_abs_diff,
                stop_on_mismatch=args.stop_on_mismatch,
            )
            _progress("phase=main,event=scan_end")
            saw_any_mismatch = saw_any_mismatch or mismatch_count > 0
            saw_any_major = saw_any_major or major_mismatch_count > 0

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
                        q17_filter_mode=args.q17_filter_mode,
                        range_style=args.range_style,
                        subquery_key_source=args.subquery_key_source,
                        subquery_expr_variant=args.subquery_expr_variant,
                        decimal_abs_tol=decimal_abs_tol,
                        double_abs_tol=args.double_abs_tol,
                        major_decimal_abs_diff=major_decimal_abs_diff,
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

    if args.fail_on_any_mismatch and saw_any_mismatch:
        return 1
    if saw_any_error:
        return 1
    if saw_any_major:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
