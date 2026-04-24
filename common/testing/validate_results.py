#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Validate TPC-H/TPC-DS query results against expected parquet files.

Comparison logic lives in common/testing/result_comparison.py and is shared
with the integration test path so that both paths use identical semantics.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# Allow importing from the repo root (common/testing/result_comparison)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from common.testing.result_comparison import ValidationStatus, validate_query_result
from common.testing.test_utils import get_queries

# ---------------------------------------------------------------------------
# Main validation loop
# ---------------------------------------------------------------------------


def validate(
    results_dir: Path,
    expected_dir: Path,
    queries: dict[str, str],
    query_numbers: list[int] | None = None,
) -> dict:
    """Run validation and return a results dict.

    Args:
        results_dir:    Directory containing q1.parquet ... q22.parquet result files.
        expected_dir:   Directory containing expected parquet files.
        queries:        Dict mapping query IDs (e.g. "Q1") to SQL strings.
        query_numbers:  Optional list of query numbers to validate.  When provided,
                        only those queries are checked.

    Returns a dict with keys:
      overall_status: "passed" | "failed" | "expected-failure" | "not-validated"
      queries: { "q1": {"status": ..., "message": ...}, ... }
    """
    query_results: dict[str, dict] = {}
    passed = failed = not_validated = expected_failures = 0

    if query_numbers is not None:
        result_files = sorted(f for q in query_numbers for f in [results_dir / f"q{q}.parquet"] if f.exists())
    else:
        result_files = sorted(results_dir.glob("q*.parquet"))

    if not result_files:
        print(f"No result parquet files found in {results_dir}", file=sys.stderr)
        return {"overall_status": "not-validated", "queries": {}}

    for result_file in result_files:
        query_id = result_file.stem  # e.g. "q1"
        q_num = int(query_id.lstrip("q"))

        # Accepted naming conventions for expected files (tried in order):
        #   q01.parquet  (q-prefixed, zero-padded)
        #   q1.parquet   (q-prefixed, no zero-padding)
        #   01.parquet   (zero-padded, no prefix)
        expected_file = next(
            (
                expected_dir / name
                for name in (f"q{q_num:02d}.parquet", f"q{q_num}.parquet", f"{q_num:02d}.parquet")
                if (expected_dir / name).exists()
            ),
            expected_dir / f"q{q_num:02d}.parquet",  # fallback for the "not found" message
        )

        if not expected_file.exists():
            print(f"[Validation] {query_id.upper():4s}: FAIL     expected file not found: {expected_file.name}")
            query_results[query_id] = {
                "status": "failed",
                "message": f"expected file not found: {expected_file.name}",
            }
            failed += 1
            continue

        actual = pd.read_parquet(result_file)
        expected = pd.read_parquet(expected_file)

        if expected.empty and all(t is object for t in expected.dtypes):
            msg = f"expected file is empty (no schema): {expected_file.name}"
            query_results[query_id] = {"status": "not-validated", "message": msg}
            not_validated += 1
            continue

        # Look up the SQL for this query (keys are "Q1", "Q2", etc.)
        query_sql = queries.get(query_id.upper())
        if query_sql is None:
            msg = f"no SQL found for {query_id}"
            query_results[query_id] = {"status": "not-validated", "message": msg}
            not_validated += 1
            continue

        status, msg = validate_query_result(query_id, actual, expected, query_sql)

        if status == "not-validated":
            query_results[query_id] = {"status": "not-validated", "message": msg}
            not_validated += 1
        elif status == "passed":
            query_results[query_id] = {"status": "passed", "message": None}
            passed += 1
        elif status == "expected-failure":
            print(f"[Validation] {query_id.upper():4s}: XFAIL    {msg}")
            query_results[query_id] = {"status": "expected-failure", "message": msg}
            expected_failures += 1
        else:
            print(f"[Validation] {query_id.upper():4s}: FAIL     {msg}")
            query_results[query_id] = {"status": "failed", "message": msg}
            failed += 1

    print(
        f"[Validation] Results: {passed} passed, {failed} failed, {expected_failures} expected-failure, {not_validated} skipped"
    )

    if failed > 0:
        overall: ValidationStatus = "failed"
    elif not_validated > 0:
        overall = "not-validated"
    elif expected_failures > 0:
        overall = "expected-failure"
    elif passed > 0:
        overall = "passed"
    else:
        overall = "not-validated"

    return {"overall_status": overall, "queries": query_results}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate TPC-H/TPC-DS query results against expected parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Benchmark output directory (same as pytest --output-dir).",
    )
    parser.add_argument(
        "--tag",
        default=None,
        help="Optional tag subdirectory (same as pytest --tag).",
    )
    parser.add_argument(
        "--reference-results-dir",
        required=False,
        default=None,
        help="Directory containing reference (expected) parquet files. "
        "If omitted, validation is skipped and overall_status is 'not-validated'.",
    )
    parser.add_argument(
        "--benchmark-type",
        default="tpch",
        choices=["tpch", "tpcds"],
        help="Benchmark type used to load the canonical SQL queries (default: tpch).",
    )
    parser.add_argument(
        "--queries",
        default=None,
        help="Comma-separated list of query numbers to validate (e.g. '1,6,14'). "
        "When omitted, all result files in results_dir are validated.",
    )
    return parser.parse_args()


def _write_not_validated(results_dir: Path, reason: str) -> None:
    """Write a not-validated sentinel JSON and print the reason."""
    print(f"[Validation] {reason}")
    results = {"overall_status": "not-validated", "queries": {}}
    output_path = results_dir.parent / "validation_results.json"
    output_path.write_text(json.dumps(results, indent=2))
    print(f"[Validation] Results written to {output_path}")


if __name__ == "__main__":
    args = parse_args()

    output_dir = Path(args.output_dir)
    results_dir = output_dir / args.tag / "query_results" if args.tag else output_dir / "query_results"

    if not results_dir.is_dir():
        print(f"Error: results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    if args.reference_results_dir is None:
        _write_not_validated(results_dir, "No reference results directory provided; validation skipped.")
        sys.exit(0)

    expected_dir = Path(args.reference_results_dir)

    if not expected_dir.is_dir():
        print(f"Error: reference results directory not found: {expected_dir}", file=sys.stderr)
        sys.exit(1)

    # Load canonical SQL queries for the benchmark type
    queries = get_queries(args.benchmark_type)

    query_numbers = [int(q.strip()) for q in args.queries.split(",")] if args.queries else None

    results = validate(results_dir, expected_dir, queries, query_numbers=query_numbers)

    # Write validation_results.json next to the query_results/ dir
    output_path = results_dir.parent / "validation_results.json"
    output_path.write_text(json.dumps(results, indent=2))
    print(f"[Validation] Results written to {output_path}")

    sys.exit(0 if results["overall_status"] != "failed" else 1)
