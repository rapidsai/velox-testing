# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
import os
from pathlib import Path
import re

import pandas as pd
import prestodb
import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.profiler_utils import start_profiler, stop_profiler

from ..integration_tests.analyze_tables import check_tables_analyzed
from .metrics_collector import collect_metrics
from .run_context import gather_run_context


Q11_RESULT_TABLE = "q11result"
Q11_SOURCE_TABLE = "partsupp"
Q11_RESULT_HOST_ROOT = Path("/raid/dmakkar/tmp/presto_q11result")
PRESTO_USER_DATA_URI = "file:/var/lib/presto/data/hive/data/user_data"
Q11_STAGING_DIRECTORY = ".hive-staging"
Q11_STAGING_SESSION_SQL = f"SET SESSION hive.temporary_staging_directory_path = '{Q11_STAGING_DIRECTORY}'"
Q11_RESULT_TABLE_DDL = """
CREATE TABLE {table} (
    ps_partkey BIGINT,
    value DOUBLE
)
WITH (format = 'PARQUET', external_location = '{external_location}')
"""
EXTERNAL_LOCATION_PATTERN = re.compile(r"external_location = '(file:[^']+)'")


def _get_external_location_uri(presto_cursor, table):
    create_table_text = presto_cursor.execute(f"SHOW CREATE TABLE {table}").fetchone()[0]
    match = EXTERNAL_LOCATION_PATTERN.search(create_table_text)
    return match.group(1) if match else None


def _table_exists(presto_cursor, table):
    return any(table_name == table for (table_name,) in presto_cursor.execute("SHOW TABLES").fetchall())


def _ensure_q11_result_dir(result_host_location):
    result_host_location.mkdir(parents=True, exist_ok=True)
    result_host_location.chmod(0o777)
    staging_location = result_host_location / Q11_STAGING_DIRECTORY
    staging_location.mkdir(parents=True, exist_ok=True)
    staging_location.chmod(0o777)


def _get_q11_result_locations(schema_name):
    presto_data_dir = os.environ.get("PRESTO_DATA_DIR")
    if not presto_data_dir:
        raise RuntimeError("PRESTO_DATA_DIR must be set to map Q11 output into the Presto container")

    result_host_location = Q11_RESULT_HOST_ROOT / schema_name / Q11_RESULT_TABLE
    try:
        relative_location = result_host_location.resolve().relative_to(Path(presto_data_dir).resolve())
    except ValueError as e:
        raise RuntimeError(
            f"Q11 output path {result_host_location} must be under PRESTO_DATA_DIR={presto_data_dir}"
        ) from e

    result_external_location = f"{PRESTO_USER_DATA_URI}/{relative_location.as_posix()}"
    return result_host_location, result_external_location


def _ensure_q11_result_table(presto_cursor, schema_name):
    if not _get_external_location_uri(presto_cursor, Q11_SOURCE_TABLE):
        raise RuntimeError(f"Could not determine external location for {Q11_SOURCE_TABLE}")

    result_host_location, result_external_location = _get_q11_result_locations(schema_name)
    _ensure_q11_result_dir(result_host_location)

    if _table_exists(presto_cursor, Q11_RESULT_TABLE):
        if _get_external_location_uri(presto_cursor, Q11_RESULT_TABLE) == result_external_location:
            return
        presto_cursor.execute(f"DROP TABLE {Q11_RESULT_TABLE}")

    presto_cursor.execute(
        Q11_RESULT_TABLE_DDL.format(table=Q11_RESULT_TABLE, external_location=result_external_location)
    )


@pytest.fixture(scope="session", autouse=True)
def run_context_collector(request):
    """Gather Presto-specific run context and attach it to the session.

    The common pytest_sessionfinish merges session.run_context into the
    benchmark_result.json context section.
    """
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema_name = request.config.getoption("--schema-name")

    ctx = gather_run_context(
        hostname=hostname,
        port=port,
        user=user,
        schema_name=schema_name,
    )
    ctx["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    yield ctx
    request.session.run_context = ctx


@pytest.fixture(scope="session", autouse=True)
def verify_tables_analyzed(request):
    """Session-scoped setup that verifies ANALYZE TABLE has been run on all tables."""
    if request.config.getoption("--skip-analyze-check"):
        print("[Analyze] Skipping analyze check (--skip-analyze-check flag set).")
        return
    hostname = request.config.getoption("--hostname")
    port = request.config.getoption("--port")
    user = request.config.getoption("--user")
    schema = request.config.getoption("--schema-name")
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    cursor = conn.cursor()
    try:
        check_tables_analyzed(cursor, schema, excluded_tables={Q11_RESULT_TABLE})
    except RuntimeError as e:
        pytest.exit(str(e), returncode=1)
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
    schema_name = request.config.getoption("--schema-name")

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
            query = benchmark_queries[query_id]
            if query_id == "Q11":
                presto_cursor.execute(Q11_STAGING_SESSION_SQL)
                _ensure_q11_result_table(presto_cursor, schema_name)
                query = f"INSERT INTO {Q11_RESULT_TABLE} {query}"

            if profile:
                # Base path without .nsys-rep extension: {dir}/{query_id}
                profile_output_file_path = f"{profile_output_dir_path.absolute()}/{query_id}"
                start_profiler(profile_script_path, profile_output_file_path)
            result = []
            for iteration_num in range(iterations):
                cursor = presto_cursor.execute(
                    "--" + str(benchmark_type) + "_" + str(query_id) + "--" + "\n" + query
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
            error_type = getattr(e, "error_type", type(e).__name__)
            error_name = getattr(e, "error_name", str(e))
            failed_queries_dict[query_id] = f"{error_type}: {error_name}"
            raw_times_dict[query_id] = None
            raise
        finally:
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function
