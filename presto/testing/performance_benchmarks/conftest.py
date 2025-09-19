import json
import statistics

from ..common.conftest import *


def pytest_addoption(parser):
    parser.addoption("--queries")
    parser.addoption("--schema-name", default="hive_duckdb_sf1")  # required=True)
    parser.addoption("--hostname", default="localhost")
    parser.addoption("--port", default=8080)
    parser.addoption("--user", default="test_user")
    parser.addoption("--iterations", default=5)
    parser.addoption("--output", default="benchmark_results.json")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    FORMAT_WIDTH_KEY = "format_width"
    AGGREGATE_TIMES_KEY = "agg_times_ms"

    for benchmark_type, result in terminalreporter._session.benchmark_results.items():
        assert AGGREGATE_TIMES_KEY in result

        terminalreporter.write_line("")
        terminalreporter.section(f"{benchmark_type} Benchmark Summary", sep="-", bold=True, yellow=True)

        width = result[FORMAT_WIDTH_KEY]
        header = f" Query ID |{'Avg':^{width}}|{'Min':^{width}}|{'Max':^{width}}|{'StdDev':^{width}}"
        terminalreporter.write_line(header)
        terminalreporter.write_line("-" * len(header), bold=True, yellow=True)
        agg_timings = result[AGGREGATE_TIMES_KEY]
        for query_id, agg_timings in agg_timings.items():
            line = (f"{query_id:^10}|{agg_timings[0]:^{width}}|{agg_timings[1]:^{width}}|"
                    f"{agg_timings[2]:^{width}}|{agg_timings[3]:^{width}}")
            terminalreporter.write_line(line)
        terminalreporter.write_line("")


def pytest_sessionfinish(session, exitstatus):
    bench_results_path = session.config.getoption("--output")
    AGGREGATE_TIMES_KEY = "agg_times_ms"
    AGGREGATE_TIMES_FIELDS_KEY = "agg_times_fields"

    json_result = {
        AGGREGATE_TIMES_KEY: {},
        AGGREGATE_TIMES_FIELDS_KEY: ["AVG", "MIN", "MAX", "STDDEV"],
    }
    for benchmark_type, result in session.benchmark_results.items():
        compute_aggregate_timings(result)
        json_result[AGGREGATE_TIMES_KEY][benchmark_type] = result[AGGREGATE_TIMES_KEY]

    with open(bench_results_path, "w") as file:
        json.dump(json_result, file, indent=2)


def compute_aggregate_timings(benchmark_results):
    RAW_TIMES_KEY = "raw_times_ms"
    AGGREGATE_TIMES_KEY = "agg_times_ms"
    FORMAT_WIDTH_KEY = "format_width"

    raw_times = benchmark_results[RAW_TIMES_KEY]
    benchmark_results[AGGREGATE_TIMES_KEY] = {}
    format_width = 8
    for query_id, timings in raw_times.items():
        stats = (round(statistics.mean(timings), 2), min(timings),
                 max(timings), round(statistics.stdev(timings), 2))
        benchmark_results[AGGREGATE_TIMES_KEY][query_id] = stats
        format_width = max(format_width, *[len(str(stat)) for stat in stats])
    benchmark_results[FORMAT_WIDTH_KEY] = format_width + 2  # Space padding on each side
