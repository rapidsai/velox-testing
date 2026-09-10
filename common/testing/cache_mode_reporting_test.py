# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for --cache-mode aware reporting in conftest.py."""

import pytest

from common.testing.performance_benchmarks.benchmark_keys import BenchmarkKeys
from common.testing.performance_benchmarks.conftest import _agg_headers, compute_aggregate_timings

CACHE_MODES = ["off", "lukewarm", "cold-once", "cold", "hot"]


def _aggregate(timings, cache_mode="off", warmup_iterations=1):
    """Run compute_aggregate_timings over one query and return (headers, stats)."""
    results = {BenchmarkKeys.RAW_TIMES_KEY: {"Q1": timings}}
    compute_aggregate_timings(results, cache_mode, warmup_iterations)
    iterations = len(timings) if timings else 0
    return _agg_headers(cache_mode, iterations), results[BenchmarkKeys.AGGREGATE_TIMES_KEY]["Q1"]


@pytest.mark.parametrize("cache_mode", CACHE_MODES)
@pytest.mark.parametrize("iterations", [1, 2, 5])
def test_header_and_stats_shapes_match(cache_mode, iterations):
    """Every mode/iteration combination must produce one stat per column.

    pytest_terminal_summary and build_and_write_benchmark_result both assert this, so a
    mismatch fails the benchmark run at reporting time.
    """
    headers, stats = _aggregate([100.0 + i for i in range(iterations)], cache_mode)
    assert len(headers) == len(stats)


def test_default_and_unregistered_modes_match_historical_layout():
    """off is the default and the spark_gluten fallback; both must reproduce the
    pre-cache-mode column layout exactly."""
    headers, stats = _aggregate([100.0, 101.0, 102.0])
    assert headers == ["Avg Hot(ms)", "Min Hot(ms)", "Max Hot(ms)", "Median Hot(ms)", "GMean Hot(ms)", "Lukewarm(ms)"]
    assert len(stats) == 6


def test_lukewarm_reports_first_iteration_separately():
    headers, stats = _aggregate([200.0, 100.0, 102.0], "lukewarm")
    assert headers[-1] == "Lukewarm(ms)"
    assert stats[-1] == 200.0  # iteration 0, reported alone
    assert stats[0] == pytest.approx(101.0)  # avg over iterations[1:]


def test_hot_excludes_warmup_iterations():
    """Warm-up iterations prime the cache and are not reported at all under hot."""
    headers, stats = _aggregate([200.0, 300.0, 100.0, 102.0], "hot", warmup_iterations=2)
    assert "Lukewarm(ms)" not in headers
    assert stats[0] == pytest.approx(101.0)  # avg over iterations[2:] only
    assert stats[2] == 102.0  # max excludes the 300.0 warm-up


def test_cold_once_reports_cold_first_sample_then_hot():
    """cold-once resets per query, so iteration 0 is a real cold sample and the rest are hot."""
    headers, stats = _aggregate([200.0, 100.0, 102.0], "cold-once")
    assert headers[-1] == "Cold(ms)"
    assert headers[0] == "Avg Hot(ms)"
    assert stats[-1] == 200.0  # the cold sample
    assert stats[0] == pytest.approx(101.0)  # hot aggregate excludes it


def test_cold_aggregates_all_iterations():
    """Every cold iteration was independently reset, so none are excluded."""
    headers, stats = _aggregate([100.0, 102.0, 200.0], "cold")
    assert headers == ["Avg Cold(ms)", "Min Cold(ms)", "Max Cold(ms)", "Median Cold(ms)", "GMean Cold(ms)"]
    assert stats[0] == pytest.approx(134.0)
    assert stats[2] == 200.0


@pytest.mark.parametrize("cache_mode", CACHE_MODES)
def test_degenerate_inputs_do_not_break_reporting(cache_mode):
    """A failed query (None timings) and warmup >= iterations must not raise."""
    results = {BenchmarkKeys.RAW_TIMES_KEY: {"Q1": None}}
    compute_aggregate_timings(results, cache_mode, 1)
    assert results[BenchmarkKeys.AGGREGATE_TIMES_KEY]["Q1"] is None

    headers, stats = _aggregate([100.0, 102.0], cache_mode, warmup_iterations=5)
    assert len(headers) == len(stats)
