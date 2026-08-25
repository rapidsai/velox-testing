#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Apply the AWS 102 timing rules to benchmark_result.json files."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def load_result(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    tpch = payload.get("tpch", {})
    failures = tpch.get("failed_queries", {})
    if failures:
        raise ValueError(f"{path}: failed queries are present: {sorted(failures)}")
    raw = tpch.get("raw_times_ms")
    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"{path}: tpch.raw_times_ms is missing or empty")
    return raw


def query_sort_key(name: str) -> tuple[int, str]:
    try:
        return int(name.removeprefix("Q")), name
    except ValueError:
        return 10**9, name


def baseline(path: Path) -> dict[str, Any]:
    raw = load_result(path)
    per_query_ms: dict[str, float] = {}
    for query, values in raw.items():
        if not isinstance(values, list) or len(values) != 5:
            raise ValueError(f"{path}: {query} must contain exactly five iterations")
        per_query_ms[query] = statistics.fmean(values[1:])
    ordered = dict(sorted(per_query_ms.items(), key=lambda item: query_sort_key(item[0])))
    return {
        "mode": "cache_off_s3",
        "source": str(path),
        "warmup_iterations_excluded": 1,
        "measured_iterations": 4,
        "per_query_mean_ms": ordered,
        "suite_runtime_ms": sum(ordered.values()),
        "suite_runtime_seconds": sum(ordered.values()) / 1000,
    }


def cache_series(paths: list[Path]) -> dict[str, Any]:
    if len(paths) != 5:
        raise ValueError("cache series requires one cold-fill and four warm results")
    loaded = [load_result(path) for path in paths]
    query_sets = [set(result) for result in loaded]
    if any(query_set != query_sets[0] for query_set in query_sets[1:]):
        raise ValueError("cache-series result files contain different query sets")
    for path, result in zip(paths, loaded, strict=True):
        for query, values in result.items():
            if not isinstance(values, list) or len(values) != 1:
                raise ValueError(f"{path}: {query} must contain one suite-major iteration")

    cold_per_query = {query: loaded[0][query][0] for query in sorted(loaded[0], key=query_sort_key)}
    warm_per_query = {
        query: statistics.fmean(result[query][0] for result in loaded[1:])
        for query in sorted(loaded[0], key=query_sort_key)
    }
    return {
        "mode": "cache_on_cold_fill_and_warm_reuse",
        "sources": [str(path) for path in paths],
        "cold_fill": {
            "per_query_ms": cold_per_query,
            "suite_runtime_ms": sum(cold_per_query.values()),
            "suite_runtime_seconds": sum(cold_per_query.values()) / 1000,
        },
        "warm_reuse": {
            "measured_suite_passes": 4,
            "per_query_mean_ms": warm_per_query,
            "suite_runtime_ms": sum(warm_per_query.values()),
            "suite_runtime_seconds": sum(warm_per_query.values()) / 1000,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--baseline", type=Path, metavar="RESULT")
    modes.add_argument(
        "--cache-series",
        type=Path,
        nargs=5,
        metavar=("COLD", "WARM1", "WARM2", "WARM3", "WARM4"),
    )
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        summary = baseline(args.baseline) if args.baseline else cache_series(args.cache_series)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"error: {error}") from error
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
