#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Apply benchmark timing rules to benchmark_result.json files."""

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


def summarize(path: Path) -> dict[str, Any]:
    raw = load_result(path)
    per_query_ms: dict[str, float] = {}
    for query, values in raw.items():
        if not isinstance(values, list) or len(values) != 5:
            raise ValueError(f"{path}: {query} must contain exactly five iterations")
        per_query_ms[query] = statistics.fmean(values[1:])
    ordered = dict(sorted(per_query_ms.items(), key=lambda item: query_sort_key(item[0])))
    return {
        "mode": "query_major_iterations",
        "source": str(path),
        "warmup_iterations_excluded": 1,
        "measured_iterations": 4,
        "per_query_mean_ms": ordered,
        "suite_runtime_ms": sum(ordered.values()),
        "suite_runtime_seconds": sum(ordered.values()) / 1000,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path, metavar="RESULT")
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        summary = summarize(args.result)
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
