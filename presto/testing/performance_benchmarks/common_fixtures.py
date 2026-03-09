# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import duckdb
import pandas as pd
import prestodb
import pytest

from common.testing.integration_tests.test_utils import (
    assert_rows_equal,
    normalize_rows,
    none_safe_sort_key,
)
from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys

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
            failed_queries_dict[query_id] = f"{e.error_type}: {e.error_name}"
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


@pytest.fixture(scope="session", autouse=True)
def validate_benchmark_results(request):
    """Session-scoped fixture that validates benchmark query results after all queries complete."""
    yield

    expected_results_dir = request.config.getoption("--expected-results-dir")
    if expected_results_dir is None:
        hostname = request.config.getoption("--hostname")
        port = request.config.getoption("--port")
        user = request.config.getoption("--user")
        schema = request.config.getoption("--schema-name")
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

    output_dir = request.config.getoption("--output-dir")
    actual_results_dir = Path(output_dir) / "query_results"
    if not actual_results_dir.is_dir():
        print(f"[Validation] Skipping result validation (no query results directory at '{actual_results_dir}').")
        return

    passed = 0
    failed = 0
    for expected_file in expected_files:
        query_name = expected_file.name
        actual_file = actual_results_dir / query_name
        if not actual_file.exists():
            print(f"[Validation] SKIPPED: {query_name} - no actual result found.")
            continue
        try:
            expected_rel = duckdb.from_parquet(str(expected_file))
            actual_rel = duckdb.from_parquet(str(actual_file))
            types = expected_rel.types

            expected_rows = sorted(normalize_rows(expected_rel.fetchall(), types), key=none_safe_sort_key)
            actual_rows = sorted(normalize_rows(actual_rel.fetchall(), types), key=none_safe_sort_key)

            assert len(actual_rows) == len(expected_rows), (
                f"Row count mismatch: {len(actual_rows)} vs {len(expected_rows)}"
            )
            assert_rows_equal(actual_rows, expected_rows, types)
            print(f"[Validation] PASSED: {query_name}")
            passed += 1
        except AssertionError as e:
            print(f"[Validation] FAILED: {query_name} - {e}")
            failed += 1
        except Exception as e:
            print(f"[Validation] ERROR: {query_name} - {e}")
            failed += 1

    total = passed + failed
    print(f"[Validation] Result validation complete: {passed}/{total} queries passed.")
