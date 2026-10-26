# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Summarizes a SQLStorm sweep results directory.

Expects explain_java.csv (conversion check) and run_<engine>.csv files. Every
percentage uses the queries considered in the EXPLAIN sweep as its denominator, so
conversion losses count against every engine.
"""

import argparse
import collections
import csv
import os

ENGINES = [
    ("java", "Presto Java"),
    ("cpu", "Presto native CPU"),
    ("gpu", "Presto GPU, CPU fallback enabled"),
    ("gpu_nofallback", "Presto GPU, CPU fallback disabled"),
]


def read(path):
    with open(path, newline="") as f:
        return {row["query"]: row for row in csv.DictReader(f)}


def percent(n, total):
    return f"{100.0 * n / total:.1f}%" if total else "n/a"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results_dir")
    args = parser.parse_args()

    explain = read(os.path.join(args.results_dir, "explain_java.csv"))
    considered = set(explain)
    total = len(considered)
    converted = sum(row["status"] == "ok" for row in explain.values())

    lines = [
        f"Queries considered: {total}",
        "",
        "| Metric | Queries | % of considered | Status breakdown |",
        "|---|---|---|---|",
        f"| Converted (passes EXPLAIN on Presto Java) | {converted} | {percent(converted, total)} | |",
    ]
    for engine, label in ENGINES:
        path = os.path.join(args.results_dir, f"run_{engine}.csv")
        if not os.path.exists(path):
            continue
        rows = {q: r for q, r in read(path).items() if q in considered}
        statuses = collections.Counter(r["status"] for r in rows.values())
        ran = statuses.get("ok", 0)
        missing = total - len(rows)
        breakdown = ", ".join(f"{k}={v}" for k, v in sorted(statuses.items()))
        if missing:
            breakdown += f", not_run={missing}"
        lines.append(f"| Runs on {label} | {ran} | {percent(ran, total)} | {breakdown} |")

    summary = "\n".join(lines)
    print(summary)
    with open(os.path.join(args.results_dir, "summary.md"), "w") as f:
        f.write(summary + "\n")


if __name__ == "__main__":
    main()
