# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import prestodb
import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import get_output_dir
from common.testing.performance_benchmarks.profiler_utils import start_profiler, stop_profiler

from ..integration_tests.analyze_tables import check_tables_analyzed
from .ctas import (
    CTAS_CATALOG,
    CTAS_SCHEMA,
    CtasResults,
    build_ctas_query,
    ctas_table_name,
    drop_results_schema,
    finalize_ctas_results,
)
from .metrics_collector import collect_metrics
from .run_context import gather_run_context


def write_query_result(cursor, output_dir, query_id):
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    frame = pd.DataFrame(rows, columns=columns)

    results_dir = Path(output_dir) / "query_results"
    results_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(results_dir / f"{query_id.lower()}.parquet", index=False)


@pytest.fixture(scope="module")
def ctas_results(request, benchmark_queries):
    if not request.config.getoption("--run-as-ctas-queries"):
        yield None
        return

    scratch_dir = os.environ.get("PRESTO_CTAS_SCRATCH_DIR")
    if not scratch_dir:
        pytest.exit("PRESTO_CTAS_SCRATCH_DIR must be set when --run-as-ctas-queries is enabled.", returncode=2)

    schema = CTAS_SCHEMA
    host_results_dir = Path(scratch_dir).expanduser() / schema

    conn = prestodb.dbapi.connect(
        host=request.config.getoption("--hostname"),
        port=request.config.getoption("--port"),
        user=request.config.getoption("--user"),
        catalog="hive",
    )
    cursor = conn.cursor()
    try:
        # Drop registered managed tables before removing any unregistered files
        # that may have survived an interrupted run.
        drop_results_schema(cursor, CTAS_CATALOG, schema)
        if host_results_dir.exists():
            shutil.rmtree(host_results_dir)
        cursor.execute(f"CREATE SCHEMA {CTAS_CATALOG}.{schema}").fetchall()
    except Exception as error:
        pytest.exit(f"Failed to prepare CTAS result output: {error}", returncode=2)
    finally:
        cursor.close()
        conn.close()

    yield CtasResults(catalog=CTAS_CATALOG, schema=schema)

    output_dir = get_output_dir(request.config) / "query_results"
    print(f"[CTAS] Normalizing temporary results into {output_dir}")
    try:
        outputs = finalize_ctas_results(host_results_dir, output_dir, benchmark_queries)
    except Exception as error:
        pytest.fail(f"Failed to normalize CTAS results: {error}", pytrace=False)
    for output in outputs:
        print(f"Normalized CTAS result: {output}")


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
        check_tables_analyzed(cursor, schema)
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
def benchmark_query(request, presto_cursor, benchmark_queries, benchmark_result_collector, ctas_results):
    iterations = request.config.getoption("--iterations")
    profile = request.config.getoption("--profile")
    profile_script_path = request.config.getoption("--profile-script-path")
    metrics = request.config.getoption("--metrics")
    benchmark_type = request.node.obj.BENCHMARK_TYPE
    bench_output_dir = get_output_dir(request.config)
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
        scratch_table = None
        try:
            if profile:
                # Base path without .nsys-rep extension: {dir}/{query_id}
                profile_output_file_path = f"{profile_output_dir_path.absolute()}/{query_id}"
                start_profiler(profile_script_path, profile_output_file_path)
            result = []
            for iteration_num in range(iterations):
                if ctas_results is not None:
                    # Retain the first iteration as the query result. Later
                    # iterations use short-lived tables so every measured
                    # execution performs the same CTAS work.
                    table = ctas_table_name(query_id, iteration_num)
                    scratch_table = table if iteration_num > 0 else None
                    query = build_ctas_query(
                        benchmark_type,
                        query_id,
                        benchmark_queries[query_id],
                        ctas_results.catalog,
                        ctas_results.schema,
                        table,
                    )
                else:
                    query = "--" + str(benchmark_type) + "_" + str(query_id) + "--" + "\n" + benchmark_queries[query_id]

                cursor = presto_cursor.execute(query)

                if ctas_results is not None:
                    # CTAS returns only its update count to the client. Consuming it
                    # ensures all worker writes have completed before recording stats.
                    cursor.fetchall()
                    result.append(cursor.stats["elapsedTimeMillis"])
                else:
                    # Preserve the historical non-CTAS timing point: record stats
                    # immediately after execute(), before consuming result pages.
                    result.append(cursor.stats["elapsedTimeMillis"])
                    if iteration_num == 0:
                        write_query_result(cursor, bench_output_dir, query_id)

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

                if scratch_table is not None:
                    presto_cursor.execute(
                        f"DROP TABLE IF EXISTS {ctas_results.catalog}.{ctas_results.schema}.{scratch_table}"
                    ).fetchall()
                    scratch_table = None
            raw_times_dict[query_id] = result
        except Exception as e:
            error_type = getattr(e, "error_type", type(e).__name__)
            error_name = getattr(e, "error_name", str(e))
            failed_queries_dict[query_id] = f"{error_type}: {error_name}"
            raw_times_dict[query_id] = None
            raise
        finally:
            # scratch_table is non-None here only when an exception cut the loop
            # short before the in-loop DROP ran. The in-loop path resets it to
            # None after a successful drop, so this path is the exception handler.
            if scratch_table is not None and ctas_results is not None:
                with suppress(Exception):
                    presto_cursor.execute(
                        f"DROP TABLE IF EXISTS {ctas_results.catalog}.{ctas_results.schema}.{scratch_table}"
                    ).fetchall()
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function
