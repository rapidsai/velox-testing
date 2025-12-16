# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    for benchmark_type, result in terminalreporter._session.benchmark_results.items():
        assert BenchmarkKeys.AGGREGATE_TIMES_KEY in result

        terminalreporter.write_line("")
        terminalreporter.section(f"{benchmark_type} Benchmark Summary", sep="-", bold=True, yellow=True)

        AGG_HEADERS = ["Avg(ms)", "Min(ms)", "Max(ms)", "Median(ms)", "GMean(ms)"]
        width = max([len(agg_header) for agg_header in AGG_HEADERS])
        width = max(width, result[BenchmarkKeys.FORMAT_WIDTH_KEY]) + 2  # Additional padding on each side
        header = " Query ID "
        for agg_header in AGG_HEADERS:
            header += f"|{agg_header:^{width}}"
        terminalreporter.write_line(header)
        terminalreporter.write_line("-" * len(header), bold=True, yellow=True)
        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            line = f"{query_id:^10}"
            if agg_timings:
                for agg_timing in agg_timings:
                    line += f"|{agg_timing:^{width}}"
            else:
                line += (f"|{'NULL':^{width}}" * len(AGG_HEADERS))
            terminalreporter.write_line(line)
        terminalreporter.write_line("")


def pytest_sessionfinish(session, exitstatus):
    bench_output_dir = session.config.getoption("--output-dir")
    tag = session.config.getoption("--tag")
    json_result = {}

    if tag:
        bench_output_dir = f"{bench_output_dir}/{tag}"
        json_result[BenchmarkKeys.TAG_KEY] = tag
    Path(bench_output_dir).mkdir(parents=True, exist_ok=True)

    AGG_KEYS = [BenchmarkKeys.AVG_KEY, BenchmarkKeys.MIN_KEY, BenchmarkKeys.MAX_KEY,
                BenchmarkKeys.MEDIAN_KEY, BenchmarkKeys.GMEAN_KEY]
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
                for i, agg_key in enumerate(AGG_KEYS):
                    json_agg_timings[agg_key][query_id] = agg_timings[i]

    with open(f"{bench_output_dir}/benchmark_result.json", "w") as file:
        json.dump(json_result, file, indent=2)
        file.write("\n")


def compute_aggregate_timings(benchmark_results):
    raw_times = benchmark_results[BenchmarkKeys.RAW_TIMES_KEY]
    benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY] = {}
    format_width = 0
    for query_id, timings in raw_times.items():
        if timings:
            stats = (round(statistics.mean(timings), 2), min(timings), max(timings),
                     statistics.median(timings), round(statistics.geometric_mean(timings), 2))
            format_width = max(format_width, *[len(str(stat)) for stat in stats])
        else:
            stats = None
        benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY][query_id] = stats
    benchmark_results[BenchmarkKeys.FORMAT_WIDTH_KEY] = format_width
