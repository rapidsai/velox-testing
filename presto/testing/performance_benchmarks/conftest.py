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
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080, type=int)
    parser.addoption("--user", default="test_user")
    parser.addoption("--iterations", default=5, type=int)
    parser.addoption("--output-dir", default="benchmark_output")
    parser.addoption("--tag")
    parser.addoption("--analyze-tables", action="store_true", default=False,
                     help="Run ANALYZE TABLE before benchmarks to optimize memory usage")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    for benchmark_type, result in terminalreporter._session.benchmark_results.items():
        assert BenchmarkKeys.AGGREGATE_TIMES_KEY in result

        terminalreporter.write_line("")
        terminalreporter.section(f"{benchmark_type} Benchmark Summary", sep="-", bold=True, yellow=True)

        width = result[BenchmarkKeys.FORMAT_WIDTH_KEY]
        header = f" Query ID |{'Avg(ms)':^{width}}|{'Min(ms)':^{width}}|{'Max(ms)':^{width}}|{'Peak Mem(MB)':^{width+2}}"
        terminalreporter.write_line(header)
        terminalreporter.write_line("-" * len(header), bold=True, yellow=True)
        
        agg_timings = result[BenchmarkKeys.AGGREGATE_TIMES_KEY]
        memory_stats = result.get(BenchmarkKeys.MEMORY_STATS_KEY, {})
        
        for query_id, timing_stats in agg_timings.items():
            peak_mem_mb = 0
            if query_id in memory_stats:
                peak_mem_bytes = max(r.get("peakTotalMemoryBytes", 0) for r in memory_stats[query_id])
                peak_mem_mb = round(peak_mem_bytes / (1024 * 1024), 1)
            
            line = (f"{query_id:^10}|{timing_stats[0]:^{width}}|{timing_stats[1]:^{width}}|"
                    f"{timing_stats[2]:^{width}}|{peak_mem_mb:^{width+2}}")
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

    for benchmark_type, result in session.benchmark_results.items():
        compute_aggregate_timings(result)
        json_result[benchmark_type] = {
            BenchmarkKeys.AGGREGATE_TIMES_KEY: {
                BenchmarkKeys.AVG_KEY: {},
                BenchmarkKeys.MIN_KEY: {},
                BenchmarkKeys.MAX_KEY: {},
            },
            BenchmarkKeys.PEAK_MEMORY_KEY: {},
            BenchmarkKeys.FAILED_QUERIES_KEY: result[BenchmarkKeys.FAILED_QUERIES_KEY],
        }
        json_agg_timings = json_result[benchmark_type][BenchmarkKeys.AGGREGATE_TIMES_KEY]
        json_peak_memory = json_result[benchmark_type][BenchmarkKeys.PEAK_MEMORY_KEY]
        memory_stats = result.get(BenchmarkKeys.MEMORY_STATS_KEY, {})
        
        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            json_agg_timings[BenchmarkKeys.AVG_KEY][query_id] = agg_timings[0]
            json_agg_timings[BenchmarkKeys.MIN_KEY][query_id] = agg_timings[1]
            json_agg_timings[BenchmarkKeys.MAX_KEY][query_id] = agg_timings[2]
            
            if query_id in memory_stats:
                peak_mem_bytes = max(r.get("peakTotalMemoryBytes", 0) for r in memory_stats[query_id])
                json_peak_memory[query_id] = peak_mem_bytes

    with open(f"{bench_output_dir}/benchmark_result.json", "w") as file:
        json.dump(json_result, file, indent=2)
        file.write("\n")


def compute_aggregate_timings(benchmark_results):
    raw_times = benchmark_results[BenchmarkKeys.RAW_TIMES_KEY]
    benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY] = {}
    format_width = 8
    for query_id, timings in raw_times.items():
        stats = (round(statistics.mean(timings), 2), min(timings), max(timings))
        benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY][query_id] = stats
        format_width = max(format_width, *[len(str(stat)) for stat in stats])
    benchmark_results[BenchmarkKeys.FORMAT_WIDTH_KEY] = format_width + 2  # Additional padding on each side
