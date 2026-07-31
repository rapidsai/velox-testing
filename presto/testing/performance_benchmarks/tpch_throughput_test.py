# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""TPC-H multi-stream throughput benchmark."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.testing.conftest import _format_query_ids, _parse_selected_query_ids
from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import get_output_dir

from .stream_runner import run_throughput


BENCHMARK_TYPE = "tpch"
TPCH_NUM_QUERIES = 22


def _selected_query_ids(config) -> list[str]:
    selected = _parse_selected_query_ids(config.getoption("--queries"), TPCH_NUM_QUERIES)
    if not selected:
        selected = list(range(1, TPCH_NUM_QUERIES + 1))

    exclude_raw = config.getoption("--exclude-queries") or ""
    excluded = set(_parse_selected_query_ids(exclude_raw, TPCH_NUM_QUERIES)) if exclude_raw.strip() else set()
    selected = [qid for qid in selected if qid not in excluded]
    if not selected:
        raise pytest.UsageError("No queries left after applying --queries / --exclude-queries")
    return _format_query_ids(selected)


def test_tpch_throughput(request, benchmark_queries, run_context_collector):
    config = request.config
    query_ids = _selected_query_ids(config)
    output_dir = get_output_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)

    reference = config.getoption("--reference-results-dir")
    reference_dir = Path(reference) if reference else None
    if reference_dir is not None and not reference_dir.is_dir():
        raise pytest.UsageError(f"--reference-results-dir not found: {reference_dir}")

    save_results = not config.getoption("--no-save-results")

    aggregate = run_throughput(
        num_streams=config.getoption("--num-streams"),
        run_seed=config.getoption("--run-seed"),
        hostname=config.getoption("--hostname"),
        port=config.getoption("--port"),
        user=config.getoption("--user"),
        schema=config.getoption("--schema-name"),
        benchmark_type=BENCHMARK_TYPE,
        queries=benchmark_queries,
        query_ids=query_ids,
        suite_repeats=config.getoption("--suite-repeats"),
        repetitions_per_query=config.getoption("--repetitions-per-query"),
        output_dir=output_dir,
        reference_results_dir=reference_dir,
        save_results=save_results,
    )

    json_result = {
        BenchmarkKeys.CONTEXT_KEY: {
            BenchmarkKeys.ITERATIONS_COUNT_KEY: config.getoption("--iterations"),
            BenchmarkKeys.SCHEMA_NAME_KEY: config.getoption("--schema-name"),
            "benchmark": [BENCHMARK_TYPE],
            "mode": "throughput",
            "concurrency_streams": aggregate["config"]["concurrency_streams"],
            "suite_repeats": aggregate["config"]["suite_repeats"],
            "repetitions_per_query": aggregate["config"]["repetitions_per_query"],
            "run_seed": aggregate["config"]["run_seed"],
            "query_ids": query_ids,
        },
        BENCHMARK_TYPE: {
            BenchmarkKeys.RAW_TIMES_KEY: aggregate["raw_times_ms"],
            BenchmarkKeys.FAILED_QUERIES_KEY: aggregate["failed_queries"],
            BenchmarkKeys.AGGREGATE_TIMES_KEY: aggregate["agg_times_ms"],
            "stream_times_ms": aggregate["stream_times_ms"],
            "throughput": aggregate["throughput"],
            "validation": aggregate["validation"],
            "errors_by_stream": aggregate["errors_by_stream"],
        },
    }

    tag = config.getoption("--tag")
    if tag:
        json_result[BenchmarkKeys.CONTEXT_KEY][BenchmarkKeys.TAG_KEY] = tag

    for key, value in run_context_collector.items():
        json_result[BenchmarkKeys.CONTEXT_KEY][key] = value

    result_path = output_dir / "benchmark_result.json"
    result_path.write_text(json.dumps(json_result, indent=2) + "\n", encoding="utf-8")

    # Human-readable summary
    tp = aggregate["throughput"]
    print("")
    print("=" * 72)
    print("TPC-H Throughput Summary")
    print("=" * 72)
    print(f"Streams:              {aggregate['config']['concurrency_streams']}")
    print(f"Suite repeats:        {aggregate['config']['suite_repeats']}")
    print(f"Repetitions/query:    {aggregate['config']['repetitions_per_query']}")
    print(f"Run seed:             {aggregate['config']['run_seed']}")
    print(f"Queries completed:    {tp['queries_completed']}")
    print(f"Total wall (ms):      {tp['total_wall_ms']:.2f}")
    print(f"Queries/sec:          {tp['queries_per_sec']:.4f}")
    print("")
    print(f"{'Query':^8}|{'Avg(ms)':^14}|{'Median(ms)':^14}|{'P99(ms)':^14}|{'N':^8}")
    print("-" * 72)
    for qid in query_ids:
        times = aggregate["raw_times_ms"].get(qid) or []
        avg = aggregate["agg_times_ms"]["avg"].get(qid)
        med = aggregate["agg_times_ms"]["median"].get(qid)
        p99 = aggregate["agg_times_ms"]["p99"].get(qid)
        avg_s = f"{avg:.2f}" if avg is not None else "NULL"
        med_s = f"{med:.2f}" if med is not None else "NULL"
        p99_s = f"{p99:.2f}" if p99 is not None else "NULL"
        print(f"{qid:^8}|{avg_s:^14}|{med_s:^14}|{p99_s:^14}|{len(times):^8}")

    failed = aggregate["failed_queries"]
    if failed:
        print("")
        print(f"Failed query executions: {sum(len(v) for v in failed.values())} across {len(failed)} query ids")
        for qid, entries in failed.items():
            print(f"  {qid}: {len(entries)} failure(s)")

    print(f"\nWrote {result_path}")
    print(f"Per-stream logs under {output_dir / 'stream_logs'}")

    # Soft-fail the pytest if any execution/validation failures occurred.
    if failed:
        pytest.fail(
            f"Throughput run completed with failures for queries: {', '.join(sorted(failed.keys()))}"
        )
