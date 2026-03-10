# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import duckdb
import pandas as pd
import prestodb
import pytest
import sqlglot

from common.testing.integration_tests.test_utils import (
    assert_rows_equal,
    get_orderby_indices,
    none_safe_sort_key,
    normalize_rows,
)
from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.test_utils import get_queries

from ..common.test_utils import get_table_external_location
from ..integration_tests.analyze_tables import check_tables_analyzed
from .metrics_collector import collect_metrics
from .profiler_utils import start_profiler, stop_profiler


@pytest.fixture(scope="session", autouse=True)
def verify_tables_analyzed(request):
    """Session-scoped setup that verifies ANALYZE TABLE has been run on all tables."""
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    cursor = conn.cursor()
    try:
        check_tables_analyzed(cursor, schema)
    finally:
        cursor.close()
        conn.close()


@pytest.fixture(scope="module")
def presto_cursor(request):
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    return conn.cursor()


@pytest.fixture(scope="module")
def benchmark_query(request, presto_cursor, benchmark_queries, benchmark_result_collector):
    iterations = request.config.getoption("--iterations")
    profile = request.config.getoption("--profile")
    profile_script_path = request.config.getoption("--profile-script-path")
    metrics = request.config.getoption("--metrics")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    bench_output_dir = request.config.getoption("--output-dir")
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")

    if profile:
        assert profile_script_path is not None
        profile_output_dir_path = Path(f"{bench_output_dir}/profiles/{benchmark_type}")
        profile_output_dir_path.mkdir(parents=True, exist_ok=True)

    benchmark_result_collector[benchmark_type] = {
        BenchmarkKeys.RAW_TIMES_KEY: {},
        BenchmarkKeys.FAILED_QUERIES_KEY: {},
    }

    benchmark_dict = benchmark_result_collector[benchmark_type]
    raw_times_dict = benchmark_dict[BenchmarkKeys.RAW_TIMES_KEY]
    assert raw_times_dict == {}

    failed_queries_dict = benchmark_dict[BenchmarkKeys.FAILED_QUERIES_KEY]
    assert failed_queries_dict == {}

    def benchmark_query_function(query_id):
        profile_output_file_path = None
        try:
            if profile:
                # Base path without .nsys-rep extension: {dir}/{query_id}
                profile_output_file_path = f"{profile_output_dir_path.absolute()}/{query_id}"
                start_profiler(profile_script_path, profile_output_file_path)
            result = []
            for iteration_num in range(iterations):
                cursor = presto_cursor.execute(
                    "--" + str(benchmark_type) + "_" + str(query_id) + "--" + "\n" + benchmark_queries[query_id]
                )
                result.append(cursor.stats["elapsedTimeMillis"])

                # Save query results to Parquet (only on first iteration)
                if iteration_num == 0:
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)

                    # Save to Parquet format to match expected results
                    results_dir = Path(f"{bench_output_dir}/query_results")
                    results_dir.mkdir(parents=True, exist_ok=True)
                    parquet_path = results_dir / f"{query_id.lower()}.parquet"
                    df.to_parquet(parquet_path, index=False)

                # Collect metrics after each query iteration if enabled
                if metrics:
                    presto_query_id = cursor._query.query_id
                    if presto_query_id:
                        collect_metrics(
                            query_id=presto_query_id,
                            query_name=str(query_id),
                            hostname=hostname,
                            port=port,
                            output_dir=bench_output_dir,
                        )
            raw_times_dict[query_id] = result
        except Exception as e:
            error_desc = getattr(e, "error_type", type(e).__name__)
            error_name = getattr(e, "error_name", str(e))
            failed_queries_dict[query_id] = f"{error_desc}: {error_name}"
            raw_times_dict[query_id] = None
            raise
        finally:
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function


def _derive_expected_results_dir(hostname, port, user, schema):
    """Derive the expected results directory from the table schema.

    Queries the schema to find a table's external location on the host
    (e.g. $PRESTO_DATA_DIR/tpchsf100/lineitem), goes up one level to
    get the data root, and appends '_expected'.
    """
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    cursor = conn.cursor()
    try:
        table = cursor.execute(f"SHOW TABLES IN {schema}").fetchone()[0]
        table_location = get_table_external_location(schema, table, cursor)
        data_root = os.path.dirname(table_location)
        return f"{data_root}_expected"
    except Exception as e:
        print(f"[Validation] Could not derive expected results directory from schema: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def _classify_limit_query(query_sql):
    """Classify a query's validation strategy based on its LIMIT and ORDER BY.

    Returns:
        "full"              - no LIMIT, compare all columns
        "orderby_only"      - LIMIT with deterministic ORDER BY (raw columns, COUNT, etc.),
                              compare only ORDER BY columns
        "skip"              - LIMIT with non-deterministic ORDER BY (SUM/AVG float aggregates),
                              skip validation because distributed floating-point aggregation
                              can change the ranking and thus which rows appear in the result set
    """
    try:
        expr = sqlglot.parse_one(query_sql)
    except sqlglot.errors.ParseError:
        return "full"

    has_limit = any(isinstance(e, sqlglot.exp.Limit) for e in expr.iter_expressions())
    if not has_limit:
        return "full"

    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return "full"

    order_names = set()
    for ordered in order.expressions:
        key = ordered.this
        if isinstance(key, sqlglot.exp.Column):
            order_names.add(key.name)

    select = expr.find(sqlglot.exp.Select)
    float_aggs = (sqlglot.exp.Sum, sqlglot.exp.Avg)
    for s in select.expressions:
        alias = s.alias if hasattr(s, "alias") else None
        if alias and alias in order_names:
            for node_tuple in s.walk():
                node = node_tuple[0] if isinstance(node_tuple, tuple) else node_tuple
                if isinstance(node, float_aggs):
                    return "skip"

    return "orderby_only"


def _load_query_map(benchmark_types):
    """Build a {lowercase_query_name: sql} map from the benchmark query JSON files."""
    query_map = {}
    for bench_type in benchmark_types:
        try:
            queries = get_queries(bench_type)
            for key, sql in queries.items():
                query_map[key.lower()] = sql
        except (FileNotFoundError, OSError):
            pass
    return query_map


def validate_benchmark_results(config, benchmark_types):
    """Validate benchmark query results against expected parquet files.

    Called from pytest_terminal_summary so output appears after the benchmark summary.
    For queries with LIMIT, only ORDER BY columns are compared since other columns
    can be non-deterministic at the LIMIT boundary.
    """
    expected_results_dir = config.getoption("--expected-results-dir")
    if expected_results_dir is None:
        hostname = config.getoption("--hostname")
        port = config.getoption("--port")
        user = config.getoption("--user")
        schema = config.getoption("--schema-name")
        expected_results_dir = _derive_expected_results_dir(hostname, port, user, schema)

    if expected_results_dir is None:
        print("[Validation] Skipping result validation (could not determine expected results directory).")
        return

    expected_dir = Path(expected_results_dir)
    if not expected_dir.is_dir():
        print(f"[Validation] Skipping result validation (expected results directory '{expected_dir}' not found).")
        return

    expected_files = sorted(expected_dir.glob("*.parquet"))
    if not expected_files:
        print(f"[Validation] Skipping result validation (no parquet files in '{expected_dir}').")
        return

    output_dir = config.getoption("--output-dir")
    actual_results_dir = Path(output_dir) / "query_results"
    if not actual_results_dir.is_dir():
        print(f"[Validation] Skipping result validation (no query results directory at '{actual_results_dir}').")
        return

    query_map = _load_query_map(benchmark_types)

    passed_queries = []
    skipped_queries = []
    failures = []
    for expected_file in expected_files:
        query_name = expected_file.stem
        actual_file = actual_results_dir / expected_file.name
        if not actual_file.exists():
            continue

        query_sql = query_map.get(query_name)
        strategy = _classify_limit_query(query_sql) if query_sql else "full"

        # Queries whose ORDER BY involves float aggregates (e.g. SUM, AVG) are
        # non-deterministic under distributed execution: the partial-aggregate
        # reduction order can change the ranking, so different rows appear in
        # the LIMIT result set across runs.
        if strategy == "skip":
            skipped_queries.append(query_name)
            continue

        try:
            expected_rel = duckdb.from_parquet(str(expected_file))
            actual_rel = duckdb.from_parquet(str(actual_file))
            types = expected_rel.types
            columns = expected_rel.columns

            expected_rows = expected_rel.fetchall()
            actual_rows = actual_rel.fetchall()

            # For LIMIT queries with deterministic ORDER BY (raw columns,
            # COUNT, etc.), only compare the ORDER BY columns — non-ORDER BY
            # columns can differ at the boundary when there are ties.
            if strategy == "orderby_only":
                order_indices = get_orderby_indices(query_sql, columns)
                if order_indices:
                    types = [types[i] for i in order_indices]
                    expected_rows = [tuple(row[i] for i in order_indices) for row in expected_rows]
                    actual_rows = [tuple(row[i] for i in order_indices) for row in actual_rows]

            expected_rows = sorted(normalize_rows(expected_rows, types), key=none_safe_sort_key)
            actual_rows = sorted(normalize_rows(actual_rows, types), key=none_safe_sort_key)

            assert len(actual_rows) == len(expected_rows), (
                f"Row count mismatch: {len(actual_rows)} vs {len(expected_rows)}"
            )
            assert_rows_equal(actual_rows, expected_rows, types)
            passed_queries.append(query_name)
        except AssertionError as e:
            failures.append(f"[Validation] FAILED: {query_name} - {e}")
        except Exception as e:
            failures.append(f"[Validation] ERROR: {query_name} - {e}")

    for line in failures:
        print(line)
    total = len(passed_queries) + len(failures)
    passed_list = ", ".join(passed_queries)
    skipped_list = ", ".join(skipped_queries)
    parts = [f"{len(passed_queries)}/{total} passed ({passed_list})"]
    if skipped_queries:
        parts.append(f"{len(skipped_queries)} skipped non-deterministic ({skipped_list})")
    print(f"[Validation] {'; '.join(parts)}")
