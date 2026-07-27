#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Summarize coordinator query JSON files into a TSV report.

Usage:
    summarize_coordinator_queries.py <output_dir>

Reads <output_dir>/queries/*.json (one file per query) and writes:
  <output_dir>/summary.tsv               — tab-separated summary of all queries
  <output_dir>/operator_summaries/       — per-query operatorSummaries JSON (if present)
"""

import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
summary_rows = [
    [
        "query_id",
        "state",
        "error_type",
        "error_name",
        "elapsed",
        "execution",
        "query_preview",
    ]
]

for path in sorted((out / "queries").glob("*.json")):
    with path.open() as f:
        query = json.load(f)

    stats = query.get("queryStats") or {}
    error_code = query.get("errorCode") or {}
    sql = (query.get("query") or "").replace("\n", " ")
    if len(sql) > 180:
        sql = sql[:177] + "..."

    summary_rows.append(
        [
            query.get("queryId", path.stem),
            query.get("state", ""),
            query.get("errorType", ""),
            error_code.get("name", ""),
            stats.get("elapsedTime", ""),
            stats.get("executionTime", ""),
            sql,
        ]
    )

    operators = stats.get("operatorSummaries") or []
    if operators:
        operators_dir = out / "operator_summaries"
        operators_dir.mkdir(exist_ok=True)
        with (operators_dir / f"{query.get('queryId', path.stem)}.operators.json").open("w") as f:
            json.dump(operators, f, indent=2)

with (out / "summary.tsv").open("w") as f:
    for row in summary_rows:
        f.write("\t".join(str(value) for value in row) + "\n")
