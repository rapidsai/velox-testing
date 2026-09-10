# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import statistics
from dataclasses import dataclass
from pathlib import Path

import pytest

from .benchmark_keys import BenchmarkKeys


@dataclass
class DataLocation:
    option_name: str
    display_name: str
    key: str


def _first_sample_label(cache_mode: str) -> str | None:
    """Label for the standalone first-iteration column, or None if the mode has none.

    off/lukewarm report iteration 0 as "Lukewarm"; cold-once resets before each query so
    its iteration 0 is a genuine "Cold" sample followed by hot ones. cold (every
    iteration is reset) and hot (warm-ups are never reported) have no first-sample
    column. Single source of truth for _agg_headers, AGG_KEYS and
    compute_aggregate_timings.
    """
    if cache_mode == "cold-once":
        return "Cold"
    if cache_mode in ("cold", "hot"):
        return None
    return "Lukewarm"


def _agg_headers(cache_mode: str, iterations: int) -> list[str]:
    """Column headers for the terminal report, per --cache-mode (see cache_reset.py for
    the mode definitions and _first_sample_label for what each mode reports)."""
    label = "Cold" if cache_mode == "cold" else "Hot"
    first = _first_sample_label(cache_mode)
    if iterations == 1:
        return [f"{first}(ms)"] if first else [f"{label}(ms)"]
    headers = [f"Avg {label}(ms)", f"Min {label}(ms)", f"Max {label}(ms)", f"Median {label}(ms)", f"GMean {label}(ms)"]
    if first:
        headers.append(f"{first}(ms)")
    return headers


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    text_report = []
    iterations = config.getoption("--iterations")
    tag = config.getoption("--tag")
    # None when the engine doesn't register --cache-mode (spark_gluten): keep its report
    # byte-identical by omitting the mode line and falling back to lukewarm's layout.
    cache_mode_opt = config.getoption("--cache-mode", default=None)
    cache_mode = cache_mode_opt or "off"
    if not hasattr(terminalreporter._session, "benchmark_results"):
        return

    data_location_option = pytest.data_location.option_name
    data_location_display_name = pytest.data_location.display_name
    data_location_name = config.getoption(data_location_option)
    for benchmark_type, result in terminalreporter._session.benchmark_results.items():
        assert BenchmarkKeys.AGGREGATE_TIMES_KEY in result

        _write_line(terminalreporter, text_report, "")
        _write_section(
            terminalreporter, text_report, f"{benchmark_type} Benchmark Summary", sep="-", bold=True, yellow=True
        )

        _write_line(terminalreporter, text_report, "")
        _write_line(terminalreporter, text_report, f"Iterations Count: {iterations}")
        if cache_mode_opt and cache_mode_opt != "off":
            mode_line = f"Cache Mode: {cache_mode_opt}"
            if cache_mode_opt != "cold":
                mode_line += f" (warmup iterations: {config.getoption('--warmup-iterations', default=1)})"
            _write_line(terminalreporter, text_report, mode_line)
        _write_line(terminalreporter, text_report, f"{data_location_display_name} Name: {data_location_name}")
        if tag:
            _write_line(terminalreporter, text_report, f"Tag: {tag}")
        _write_line(terminalreporter, text_report, "")

        AGG_HEADERS = _agg_headers(cache_mode, iterations)
        width = max([len(agg_header) for agg_header in AGG_HEADERS])
        width = max(width, result[BenchmarkKeys.FORMAT_WIDTH_KEY]) + 2  # Additional padding on each side
        header = " Query ID "
        for agg_header in AGG_HEADERS:
            header += f"|{agg_header:^{width}}"
        _write_line(terminalreporter, text_report, "-" * len(header), bold=True, yellow=True)
        _write_line(terminalreporter, text_report, header)
        _write_line(terminalreporter, text_report, "-" * len(header), bold=True, yellow=True)
        for query_id, agg_timings in result[BenchmarkKeys.AGGREGATE_TIMES_KEY].items():
            line = f"{query_id:^10}"
            if agg_timings:
                assert len(AGG_HEADERS) == len(agg_timings)
                for agg_timing in agg_timings:
                    line += f"|{agg_timing:^{width}}"
            else:
                line += f"|{'NULL':^{width}}" * len(AGG_HEADERS)
            _write_line(terminalreporter, text_report, line)

        # Print SUM row.
        _write_line(terminalreporter, text_report, "-" * len(header))
        agg_sums = result[BenchmarkKeys.AGGREGATE_TIMES_SUM_KEY]
        line = f"{'SUM':^10}"
        if agg_sums:
            assert len(AGG_HEADERS) == len(agg_sums)
            for agg_sum in agg_sums:
                line += f"|{agg_sum:^{width}}"
        else:
            line += f"|{'NULL':^{width}}" * len(AGG_HEADERS)

        _write_line(terminalreporter, text_report, line)
        _write_line(terminalreporter, text_report, "")

    bench_output_dir = get_output_dir(config)
    assert bench_output_dir.is_dir()
    with open(f"{bench_output_dir}/benchmark_result.txt", "w") as file:
        report_text = "\n".join(text_report)
        file.write(f"{report_text}\n")


def _write_line(terminalreporter, text_report, content, **kwargs):
    terminalreporter.write_line(content, **kwargs)
    text_report.append(content)


def _write_section(terminalreporter, text_report, content, **kwargs):
    terminalreporter.section(content, **kwargs)

    sep = kwargs.get("sep", " ")
    text_report.append(f" {content} ".center(120, sep))


def build_and_write_benchmark_result(session, json_result):
    """Compute aggregate timings and write benchmark_result.json.

    Callers populate json_result[CONTEXT_KEY] with their own context
    before calling this function.
    """
    iterations = session.config.getoption("--iterations")
    cache_mode = session.config.getoption("--cache-mode", default="off")
    warmup_iterations = session.config.getoption("--warmup-iterations", default=1)

    bench_output_dir = get_output_dir(session.config)
    bench_output_dir.mkdir(parents=True, exist_ok=True)

    first_label = _first_sample_label(cache_mode)
    first_key = BenchmarkKeys.COLD_KEY if first_label == "Cold" else BenchmarkKeys.LUKEWARM_KEY
    if iterations == 1:
        AGG_KEYS = [first_key] if first_label else [BenchmarkKeys.AVG_KEY]
    else:
        AGG_KEYS = [
            BenchmarkKeys.AVG_KEY,
            BenchmarkKeys.MIN_KEY,
            BenchmarkKeys.MAX_KEY,
            BenchmarkKeys.MEDIAN_KEY,
            BenchmarkKeys.GMEAN_KEY,
        ]
        if first_label:
            AGG_KEYS.append(first_key)

    if not hasattr(session, "benchmark_results"):
        return

    for benchmark_type, result in session.benchmark_results.items():
        compute_aggregate_timings(result, cache_mode, warmup_iterations)
        json_result[benchmark_type] = {
            BenchmarkKeys.AGGREGATE_TIMES_KEY: {},
            BenchmarkKeys.RAW_TIMES_KEY: result[BenchmarkKeys.RAW_TIMES_KEY],
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


def pytest_sessionfinish(session, exitstatus):
    iterations = session.config.getoption("--iterations")

    data_location_option = pytest.data_location.option_name
    data_location_key = pytest.data_location.key
    data_location_name = session.config.getoption(data_location_option)
    json_result = {
        BenchmarkKeys.CONTEXT_KEY: {
            BenchmarkKeys.ITERATIONS_COUNT_KEY: iterations,
            data_location_key: data_location_name,
        },
    }

    tag = session.config.getoption("--tag")
    if tag:
        json_result[BenchmarkKeys.CONTEXT_KEY][BenchmarkKeys.TAG_KEY] = tag

    # Only present for engines that register --cache-mode; the reported columns depend
    # on it, so a saved report is ambiguous without it.
    cache_mode = session.config.getoption("--cache-mode", default=None)
    if cache_mode and cache_mode != "off":
        context = json_result[BenchmarkKeys.CONTEXT_KEY]
        context[BenchmarkKeys.CACHE_MODE_KEY] = cache_mode
        if cache_mode != "cold":
            context[BenchmarkKeys.WARMUP_ITERATIONS_KEY] = session.config.getoption("--warmup-iterations", default=1)
        # Which layers the resets actually cleared, as reported by the engine's reset
        # code — a tier that isn't configured, or that --skip-drop-cache skipped, is not
        # listed, so this cannot claim a reset that never happened.
        layers = getattr(session, "_cache_reset_layers", None)
        if layers:
            context[BenchmarkKeys.CACHE_RESET_LAYERS_KEY] = sorted(layers)

    if hasattr(session, "run_context"):
        for key, value in session.run_context.items():
            json_result[BenchmarkKeys.CONTEXT_KEY][key] = value

    if hasattr(session, "benchmark_results"):
        benchmark_types = list(session.benchmark_results.keys())
        json_result[BenchmarkKeys.CONTEXT_KEY]["benchmark"] = benchmark_types

    build_and_write_benchmark_result(session, json_result)


def get_output_dir(config):
    bench_output_dir = config.getoption("--output-dir")
    tag = config.getoption("--tag")
    if tag:
        bench_output_dir = f"{bench_output_dir}/{tag}"
    return Path(bench_output_dir)


def compute_aggregate_timings(benchmark_results, cache_mode="off", warmup_iterations=1):
    """Reduce each query's raw per-iteration timings to the reported aggregate stats.

    cold: stats over all iterations (each was independently reset).
    hot/lukewarm: stats over timings[warmup_iterations:]; lukewarm additionally
    reports iteration 0 alone (see _first_sample_label).
    """
    raw_times = benchmark_results[BenchmarkKeys.RAW_TIMES_KEY]
    benchmark_results[BenchmarkKeys.AGGREGATE_TIMES_KEY] = {}
    format_width = 0
    for query_id, timings in raw_times.items():
        if timings:
            if len(timings) == 1:
                if cache_mode == "hot":
                    print(
                        f"Warning: {query_id} has only 1 iteration under --cache-mode=hot "
                        f"(--warmup-iterations={warmup_iterations}); the reported 'Hot' value is "
                        "actually an unwarmed/priming run, not a true steady-state measurement."
                    )
                # The lone iteration IS the sample under every mode: cold uses all of
                # timings, hot/lukewarm's warmup-slice-or-fallback both reduce to
                # timings[0] too, so there's nothing mode-specific to branch on here.
                stats = (timings[0],)
            else:
                if cache_mode != "cold" and not timings[warmup_iterations:]:
                    print(
                        f"Warning: {query_id}: --warmup-iterations={warmup_iterations} >= its "
                        f"{len(timings)} iterations; falling back to reporting only the last "
                        "iteration, which may still include cache warm-up/priming effects."
                    )
                sample = timings if cache_mode == "cold" else (timings[warmup_iterations:] or timings[-1:])
                stats = (
                    round(statistics.mean(sample), 2),
                    min(sample),
                    max(sample),
                    statistics.median(sample),
                    round(statistics.geometric_mean(sample), 2),
                )
                if _first_sample_label(cache_mode):
                    stats += (timings[0],)
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
