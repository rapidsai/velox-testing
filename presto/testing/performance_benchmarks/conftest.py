# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import statistics

from pathlib import Path
from .benchmark_keys import BenchmarkKeys
from ..common.conftest import *


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--schema-name", required=True)
    parser.addoption("--scale-factor")
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--iterations", default=5, type=int)
    parser.addoption("--output-dir", default="benchmark_output")
    parser.addoption("--tag")
    parser.addoption("--profile", action="store_true", default=False)
    parser.addoption("--profile-script-path")
    parser.addoption("--metrics", action="store_true", default=False)


def pytest_sessionstart(session):
    # Always present, even if collection fails
    if not hasattr(session, "benchmark_results"):
        session.benchmark_results = {}

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    text_report = []
    iterations = config.getoption("--iterations")
    schema_name = config.getoption("--schema-name")
    tag = config.getoption("--tag")
    for benchmark_type, result in terminalreporter._session.benchmark_results.items():
        assert BenchmarkKeys.AGGREGATE_TIMES_KEY in result

        write_line(terminalreporter, text_report, "")
        write_section(terminalreporter, text_report, f"{benchmark_type} Benchmark Summary", sep="-", bold=True,
                      yellow=True)

        write_line(terminalreporter, text_report, "")
        write_line(terminalreporter, text_report, f"Iterations Count: {iterations}")
        write_line(terminalreporter, text_report, f"Schema Name: {schema_name}")
        if tag:
            write_line(terminalreporter, text_report, f"Tag: {tag}")
        write_line(terminalreporter, text_report, "")

        if iterations > 1:
            AGG_HEADERS = ["Avg Hot(ms)", "Min Hot(ms)", "Max Hot(ms)", "Median Hot(ms)", "GMean Hot(ms)",
                           "Lukewarm(ms)"]
        else:
            AGG_HEADERS = ["Lukewarm(ms)"]
        width = max([len(agg_header) for agg_header in AGG_HEADERS])
        width = max(width, result[BenchmarkKeys.FORMAT_WIDTH_KEY]) + 2  # Additional padding on each side
        header = " Query ID "
        for agg_header in AGG_HEADERS:
            header += f"|{agg_header:^{width}}"
        write_line(terminalreporter, text_report, "-" * len(header), bold=True, yellow=True)
        write_line(terminalreporter, text_report, header)
        write_line(terminalreporter, text_report, "-" * len(header), bold=True, yellow=True)
        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            line = f"{query_id:^10}"
            if agg_timings:
                assert len(AGG_HEADERS) == len(agg_timings)
                for agg_timing in agg_timings:
                    line += f"|{agg_timing:^{width}}"
            else:
                line += (f"|{'NULL':^{width}}" * len(AGG_HEADERS))
            write_line(terminalreporter, text_report, line)

        # Print SUM row.
        write_line(terminalreporter, text_report, "-" * len(header))
        agg_sums = result[BenchmarkKeys.AGGREGATE_TIMES_SUM_KEY]
        line = f"{'SUM':^10}"
        if agg_sums:
            assert len(AGG_HEADERS) == len(agg_sums)
            for agg_sum in agg_sums:
                line += f"|{agg_sum:^{width}}"
        else:
            line += (f"|{'NULL':^{width}}" * len(AGG_HEADERS))

        write_line(terminalreporter, text_report, line)
        write_line(terminalreporter, text_report, "")

    bench_output_dir = get_output_dir(config)
    assert bench_output_dir.is_dir()
    with open(f"{bench_output_dir}/benchmark_result.txt", "w") as file:
        file.write(f"{'\n'.join(text_report)}\n")


def write_line(terminalreporter, text_report, content, **kwargs):
    terminalreporter.write_line(content, **kwargs)
    text_report.append(content)


def write_section(terminalreporter, text_report, content, **kwargs):
    terminalreporter.section(content, **kwargs)

    sep = kwargs.get("sep", " ")
    text_report.append(f" {content} ".center(120, sep))


def pytest_sessionfinish(session, exitstatus):
    iterations = session.config.getoption("--iterations")
    schema_name = session.config.getoption("--schema-name")
    json_result = {
        BenchmarkKeys.CONTEXT_KEY: {
            BenchmarkKeys.ITERATIONS_COUNT_KEY: iterations,
            BenchmarkKeys.SCHEMA_NAME_KEY: schema_name,
        },
    }

    tag = session.config.getoption("--tag")
    if tag:
        json_result[BenchmarkKeys.CONTEXT_KEY][BenchmarkKeys.TAG_KEY] = tag

    bench_output_dir = get_output_dir(session.config)
    bench_output_dir.mkdir(parents=True, exist_ok=True)

    if iterations > 1:
        AGG_KEYS = [BenchmarkKeys.AVG_KEY, BenchmarkKeys.MIN_KEY, BenchmarkKeys.MAX_KEY,
                    BenchmarkKeys.MEDIAN_KEY, BenchmarkKeys.GMEAN_KEY, BenchmarkKeys.LUKEWARM_KEY]
    else:
        AGG_KEYS = [BenchmarkKeys.LUKEWARM_KEY]
    for benchmark_type, result in session.benchmark_results.items():
        compute_aggregate_timings(result)
        json_result[benchmark_type] = {
            BenchmarkKeys.AGGREGATE_TIMES_KEY: {},
            BenchmarkKeys.FAILED_QUERIES_KEY: result[BenchmarkKeys.FAILED_QUERIES_KEY],
        }
        json_agg_timings = json_result[benchmark_type][BenchmarkKeys.AGGREGATE_TIMES_KEY]
        for agg_key in AGG_KEYS:
            json_agg_timings[agg_key] = {}

        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            if agg_timings:
                assert len(AGG_KEYS) == len(agg_timings)
                for i, agg_key in enumerate(AGG_KEYS):
                    json_agg_timings[agg_key][query_id] = agg_timings[i]

    with open(f"{bench_output_dir}/benchmark_result.json", "w") as file:
        json.dump(json_result, file, indent=2)
        file.write("\n")


def get_output_dir(config):
    bench_output_dir = config.getoption("--output-dir")
    tag = config.getoption("--tag")
    if tag:
        bench_output_dir = f"{bench_output_dir}/{tag}"
    return Path(bench_output_dir)


def compute_aggregate_timings(benchmark_results):
    raw_times = benchmark_results[BenchmarkKeys.RAW_TIMES_KEY]
    benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY] = {}
    format_width = 0
    for query_id, timings in raw_times.items():
        if timings:
            first_iteration = timings[0]
            if len(timings) > 1:
                hot_timings = timings[1:]
                stats = (round(statistics.mean(hot_timings), 2), min(hot_timings), max(hot_timings),
                         statistics.median(hot_timings), round(statistics.geometric_mean(hot_timings), 2),
                         first_iteration)
            else:
                stats = (first_iteration,)
            format_width = max(format_width, *[len(str(stat)) for stat in stats])
        else:
            stats = None
        benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY][query_id] = stats

    agg_sums = None
    for _, stats in benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
        if stats:
            if agg_sums is None:
                agg_sums = list(stats)
            else:
                assert len(agg_sums) == len(stats)
                for i, stat in enumerate(stats):
                    agg_sums[i] = round(agg_sums[i] + stat, 2)
    benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_SUM_KEY] = agg_sums
    benchmark_results[BenchmarkKeys.FORMAT_WIDTH_KEY] = format_width
