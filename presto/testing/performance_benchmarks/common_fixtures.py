# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import shutil
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import prestodb
import pytest
import sqlglot
from sqlglot import exp

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import get_output_dir
from common.testing.performance_benchmarks.profiler_utils import start_profiler, stop_profiler

from ..integration_tests.analyze_tables import check_tables_analyzed
from .metrics_collector import collect_metrics
from .run_context import gather_run_context

CTAS_CATALOG = "hive_output"


@dataclass(frozen=True)
class CtasResults:
    catalog: str
    schema: str


def ctas_schema_name(tag):
    if tag and not re.fullmatch(r"[A-Za-z0-9_]+", tag):
        raise ValueError("CTAS result tags must contain only alphanumeric and underscore characters")
    return f"benchmark_results_{tag.lower()}" if tag else "benchmark_results"


def strip_trailing_semicolon(query):
    return query.rstrip().removesuffix(";").rstrip()


def ctas_table_name(query_id, iteration_num):
    return query_id.lower() if iteration_num == 0 else f"{query_id.lower()}_iteration_{iteration_num + 1}"


def alias_ctas_output_expressions(query):
    """Give every explicit CTAS output expression a unique column name."""
    query = strip_trailing_semicolon(query)
    parsed = sqlglot.parse_one(query, read="presto")
    select = next(parsed.find_all(exp.Select))
    expressions = list(select.expressions)

    reserved_names = {
        expression.alias_or_name.casefold()
        for expression in expressions
        if isinstance(expression, (exp.Alias, exp.Column)) and not expression.is_star
    }
    used_names = set()
    changed = False

    for position, expression in enumerate(expressions, start=1):
        if expression.is_star:
            continue

        name = expression.alias_or_name if isinstance(expression, (exp.Alias, exp.Column)) else ""
        normalized_name = name.casefold()
        if name and normalized_name not in used_names:
            used_names.add(normalized_name)
            continue

        base_alias = f"result_column_{position}"
        alias = base_alias
        suffix = 2
        while alias.casefold() in reserved_names or alias.casefold() in used_names:
            alias = f"{base_alias}_{suffix}"
            suffix += 1

        if isinstance(expression, exp.Alias):
            expression.set("alias", exp.to_identifier(alias))
        else:
            expressions[position - 1] = exp.alias_(expression, alias, copy=False)
        used_names.add(alias.casefold())
        changed = True

    if not changed:
        return query
    select.set("expressions", expressions)
    return parsed.sql(dialect="presto")


def build_ctas_query(benchmark_type, query_id, query, catalog, schema, table):
    return (
        f"--{benchmark_type}_{query_id}--\n"
        f"CREATE TABLE {catalog}.{schema}.{table} WITH (format = 'PARQUET') AS\n"
        f"{alias_ctas_output_expressions(query)}"
    )


def _drop_results_schema(cursor, catalog, schema):
    schemas = {row[0] for row in cursor.execute(f"SHOW SCHEMAS FROM {catalog}").fetchall()}
    if schema not in schemas:
        return
    tables = cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}").fetchall()
    for (table,) in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {catalog}.{schema}."{table}"').fetchall()
    cursor.execute(f"DROP SCHEMA {catalog}.{schema}").fetchall()


@pytest.fixture(scope="session")
def ctas_results(request):
    if not request.config.getoption("--write-results-to-file"):
        return None

    output_dir = os.environ.get("PRESTO_OUTPUT_DIR")
    if not output_dir:
        pytest.exit("PRESTO_OUTPUT_DIR must be set when --write-results-to-file is enabled.", returncode=2)

    tag = request.config.getoption("--tag")
    schema = ctas_schema_name(tag)
    host_results_dir = Path(output_dir).expanduser() / schema

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
        _drop_results_schema(cursor, CTAS_CATALOG, schema)
        if host_results_dir.exists():
            shutil.rmtree(host_results_dir)
        cursor.execute(f"CREATE SCHEMA {CTAS_CATALOG}.{schema}").fetchall()
    except Exception as error:
        pytest.exit(f"Failed to prepare CTAS result output: {error}", returncode=2)
    finally:
        cursor.close()
        conn.close()

    return CtasResults(catalog=CTAS_CATALOG, schema=schema)


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
                elif iteration_num == 0:
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)

                    # Save to Parquet format to match expected results
                    results_dir = Path(f"{bench_output_dir}/query_results")
                    results_dir.mkdir(parents=True, exist_ok=True)
                    parquet_path = results_dir / f"{query_id.lower()}.parquet"
                    df.to_parquet(parquet_path, index=False)

                result.append(cursor.stats["elapsedTimeMillis"])

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
            if scratch_table is not None and ctas_results is not None:
                with suppress(Exception):
                    presto_cursor.execute(
                        f"DROP TABLE IF EXISTS {ctas_results.catalog}.{ctas_results.schema}.{scratch_table}"
                    ).fetchall()
            if profile and profile_output_file_path is not None:
                stop_profiler(profile_script_path, profile_output_file_path)

    return benchmark_query_function
