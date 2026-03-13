#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "polars",
# ]
# ///
"""
Validate TPC-H query results against expected parquet files.

Validation logic is ported from cudf_polars's assert_tpch_result_equal
(cudf_polars/experimental/benchmarks/asserts.py) so that both engines use
identical comparison semantics.

Per-query sort_by / limit configuration is taken from
cudf_polars/experimental/benchmarks/pdsh.py.

Key behaviours
--------------
- Column names and row count are always checked.
- Schema (dtypes) is NOT checked — Presto may produce different parquet
  types than polars for the same logical values.
- Decimal columns are cast to Float64 before comparison (same as polars).
- Floating-point values are compared with rel_tol=1e-5, abs_tol=1e-8.
- For queries with ORDER BY (sort_by non-empty):
    - We verify that the actual result is sorted by the sort_by columns.
    - Tie-breaking is resolved by sorting both frames on all *non-float*
      columns, to avoid floating-point sort instability.
- For queries with ORDER BY + LIMIT, rows at the limit boundary (ties on
  the last sort key) are compared only on the sort_by columns, not the
  full row.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import polars as pl
import polars.testing

# ---------------------------------------------------------------------------
# Per-query configuration (sort_by, limit) — from pdsh.py
# sort_by entries: (column_name, descending)
# ---------------------------------------------------------------------------

QUERY_CONFIG: dict[str, dict] = {
    "q1":  {"sort_by": [("l_returnflag", False), ("l_linestatus", False)], "limit": None},
    "q2":  {"sort_by": [("s_acctbal", True), ("n_name", False), ("s_name", False), ("p_partkey", False)], "limit": 100},
    "q3":  {"sort_by": [("revenue", True), ("o_orderdate", False)], "limit": 10},
    "q4":  {"sort_by": [("o_orderpriority", False)], "limit": None},
    "q5":  {"sort_by": [("revenue", True)], "limit": None},
    "q6":  {"sort_by": [], "limit": None},
    "q7":  {"sort_by": [("supp_nation", False), ("cust_nation", False), ("l_year", False)], "limit": None},
    "q8":  {"sort_by": [("o_year", False)], "limit": None},
    "q9":  {"sort_by": [("nation", False), ("o_year", True)], "limit": None},
    "q10": {"sort_by": [("revenue", True)], "limit": 20},
    "q11": {"sort_by": [("value", True)], "limit": None},
    "q12": {"sort_by": [("l_shipmode", False)], "limit": None},
    "q13": {"sort_by": [("custdist", True), ("c_count", True)], "limit": None},
    "q14": {"sort_by": [], "limit": None},
    "q15": {"sort_by": [("s_suppkey", False)], "limit": None, "xfail_if_empty": True},
    "q16": {"sort_by": [("supplier_cnt", True), ("p_brand", False), ("p_type", False), ("p_size", False)], "limit": None},
    "q17": {"sort_by": [], "limit": None},
    "q18": {"sort_by": [("o_totalprice", True), ("o_orderdate", False)], "limit": 100},
    "q19": {"sort_by": [], "limit": None},
    "q20": {"sort_by": [("s_name", False)], "limit": None},
    "q21": {"sort_by": [("numwait", True), ("s_name", False)], "limit": 100},
    "q22": {"sort_by": [("cntrycode", False)], "limit": None},
}

REL_TOL = 1e-5
ABS_TOL = 1e-8


# ---------------------------------------------------------------------------
# Assertion logic — ported from cudf_polars/experimental/benchmarks/asserts.py
# ---------------------------------------------------------------------------

def _polars_assert_frame_equal(left: pl.DataFrame, right: pl.DataFrame, **kwargs: Any) -> None:
    """Call polars.testing.assert_frame_equal, handling rel_tol/abs_tol API differences."""
    try:
        # Polars >= 1.32.3 uses rel_tol / abs_tol
        polars.testing.assert_frame_equal(left, right, **kwargs)
    except TypeError:
        # Older polars uses rtol / atol
        renamed = dict(kwargs)
        renamed["rtol"] = renamed.pop("rel_tol", REL_TOL)
        renamed["atol"] = renamed.pop("abs_tol", ABS_TOL)
        polars.testing.assert_frame_equal(left, right, **renamed)


def _reconcile_presto_col_names(result: pl.DataFrame, expected: pl.DataFrame) -> pl.DataFrame:
    """
    Rename Presto's anonymous aggregate columns (_col0, _col1, ...) to match
    the expected column names, using positional matching.

    Presto names unaliased aggregate expressions `_colN`; other engines (polars,
    DuckDB) use the expression text (e.g. `sum(l_quantity)`).  We only rename
    a column when:
      - it matches `_col\\d+` in the result, AND
      - the expected column at the same position has a different name.
    Non-anonymous columns are left untouched so real name mismatches still fail.
    """
    import re
    renames = {}
    for i, (res_col, exp_col) in enumerate(zip(result.columns, expected.columns)):
        if re.fullmatch(r"_col\d+", res_col) and res_col != exp_col:
            renames[res_col] = exp_col
    if renames:
        result = result.rename(renames)
    return result


def assert_tpch_result_equal(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    sort_by: list[tuple[str, bool]],
    limit: int | None = None,
) -> None:
    """
    Validate computed result against expected answer using the same logic as
    cudf_polars's assert_tpch_result_equal.

    Raises AssertionError (with a descriptive message) on mismatch.
    """
    polars_kwargs: dict[str, Any] = {
        "check_row_order": True,
        "check_column_order": True,
        "check_dtypes": False,   # Presto types may differ from polars types
        "check_exact": False,
        "rel_tol": REL_TOL,
        "abs_tol": ABS_TOL,
        "categorical_as_str": False,
    }

    # 1. Column names — reconcile Presto _colN names before checking
    if len(left.columns) == len(right.columns):
        left = _reconcile_presto_col_names(left, right)

    if left.columns != right.columns:
        extra = set(left.columns) - set(right.columns)
        missing = set(right.columns) - set(left.columns)
        raise AssertionError(
            f"Column names mismatch — extra: {extra}, missing: {missing}\n"
            f"  result columns:   {left.columns}\n"
            f"  expected columns: {right.columns}"
        )

    # 2. Cast Decimal → Float64 (avoids off-by-~1% from different Decimal impls)
    float_casts = [
        pl.col(col).cast(pl.Float64())
        for col in left.columns
        if left.schema[col].is_decimal()
    ]
    if float_casts:
        left = left.with_columns(*float_casts)
        right = right.with_columns(*float_casts)

    # 2b. Normalize temporal columns: Presto writes dates as strings; cast to match expected.
    temporal_casts = [
        pl.col(col).cast(right.schema[col])
        for col in left.columns
        if left.schema[col] == pl.String and right.schema[col].is_temporal()
    ]
    if temporal_casts:
        left = left.with_columns(*temporal_casts)

    if not sort_by:
        # No ORDER BY — straight comparison (row order doesn't matter so sort all cols)
        non_float = [c for c in left.columns if left.schema[c] not in (pl.Float32, pl.Float64)]
        _polars_assert_frame_equal(left.sort(non_float), right.sort(non_float), **polars_kwargs)
        return

    by, descending = zip(*sort_by, strict=True)
    by = list(by)
    descending = list(descending)

    # 3. Verify each frame is sorted on the sort_by columns
    for side, df in [("result", left), ("expected", right)]:
        try:
            polars.testing.assert_frame_equal(
                df.select(by),
                df.select(by).sort(by=by, descending=descending, maintain_order=True),
            )
        except AssertionError as e:
            raise AssertionError(
                f"{side} frame is not sorted by {sort_by}: {e}"
            ) from e

    # 4. Sort both frames by non-float columns to resolve ties deterministically
    non_float_columns = [
        col for col in left.columns
        if left.schema[col] not in (pl.Float32, pl.Float64)
    ]
    left_sorted = left.sort(by=non_float_columns)
    right_sorted = right.sort(by=non_float_columns)

    if limit is None:
        _polars_assert_frame_equal(left_sorted, right_sorted, **polars_kwargs)
        return

    # 5. Handle ORDER BY + LIMIT: split into "non-ties" and "ties at boundary"
    (split_at,) = left.select(by).sort(by=by, descending=descending).tail(1).to_dicts()

    exprs = []
    for (col, val), desc in zip(split_at.items(), descending, strict=True):
        if isinstance(val, float):
            exprs.append(
                pl.col(col).lt(val - 2 * ABS_TOL) | pl.col(col).gt(val + 2 * ABS_TOL)
            )
        else:
            op = pl.col(col).gt if desc else pl.col(col).lt
            exprs.append(op(val))

    expr = pl.Expr.or_(*exprs)

    result_first   = left.filter(expr)
    expected_first = right.filter(expr)
    result_ties    = left.filter(~expr)
    expected_ties  = right.filter(~expr)

    # Non-ties: full comparison
    _polars_assert_frame_equal(
        result_first.sort(by=non_float_columns),
        expected_first.sort(by=non_float_columns),
        **polars_kwargs,
    )

    # Ties: only compare the sort_by columns (other cols may legitimately differ)
    _polars_assert_frame_equal(
        result_ties.sort(non_float_columns).select(by),
        expected_ties.sort(non_float_columns).select(by),
        **polars_kwargs,
    )


# ---------------------------------------------------------------------------
# Per-query validation
# ---------------------------------------------------------------------------

def compare_query(
    query_id: str,
    actual: pl.DataFrame,
    expected: pl.DataFrame,
) -> tuple[str, str | None]:
    """
    Compare actual vs expected for one TPC-H query.

    Returns (status, message) where status is 'passed', 'failed', 'xfail', or 'not-validated'.
    """
    cfg = QUERY_CONFIG.get(query_id)
    if cfg is None:
        return "not-validated", f"no config for {query_id}"

    # Detect known expected failures before running the full comparison.
    if cfg.get("xfail_if_empty", False) and actual.is_empty():
        return "xfail", (
            f"{query_id.upper()} returned no rows: known float calculation mismatch "
            "in MAX(total_revenue) subquery causes empty result with float data"
        )

    try:
        assert_tpch_result_equal(
            actual,
            expected,
            sort_by=cfg["sort_by"],
            limit=cfg["limit"],
        )
        return "passed", None
    except Exception as e:
        return "failed", f"{type(e).__name__}: {e}"[:500]


# ---------------------------------------------------------------------------
# Main validation loop
# ---------------------------------------------------------------------------

def validate(results_dir: Path, expected_dir: Path) -> dict:
    """Run validation and return a results dict.

    Returns a dict with keys:
      overall_status: "passed" | "failed" | "xfail" | "not-validated"
      queries: { "q1": {"status": ..., "message": ...}, ... }
    """
    query_results: dict[str, dict] = {}
    passed = failed = not_validated = xfailed = 0

    result_files = sorted(results_dir.glob("q*.parquet"))
    if not result_files:
        print(f"No result parquet files found in {results_dir}", file=sys.stderr)
        return {"overall_status": "not-validated", "queries": {}}

    for result_file in result_files:
        query_id = result_file.stem  # e.g. "q1"
        # Expected files are zero-padded: q01.parquet, q02.parquet, ...
        q_num = int(query_id.lstrip("q"))
        expected_file = expected_dir / f"q{q_num:02d}.parquet"

        if not expected_file.exists():
            print(f"  {query_id.upper():4s}: MISSING  expected file not found: {expected_file}")
            query_results[query_id] = {"status": "failed", "message": f"expected file not found: {expected_file.name}"}
            failed += 1
            continue

        actual   = pl.read_parquet(result_file)
        expected = pl.read_parquet(expected_file)

        status, msg = compare_query(query_id, actual, expected)

        if status == "not-validated":
            print(f"  {query_id.upper():4s}: SKIP     ({msg})")
            query_results[query_id] = {"status": "not-validated", "message": msg}
            not_validated += 1
        elif status == "passed":
            cfg = QUERY_CONFIG.get(query_id, {})
            info = f"sort_by={cfg.get('sort_by', [])}, limit={cfg.get('limit')}"
            print(f"  {query_id.upper():4s}: PASS     [{info}]")
            query_results[query_id] = {"status": "passed", "message": None}
            passed += 1
        elif status == "xfail":
            print(f"  {query_id.upper():4s}: XFAIL    {msg}")
            query_results[query_id] = {"status": "xfail", "message": msg}
            xfailed += 1
        else:
            print(f"  {query_id.upper():4s}: FAIL     {msg}")
            query_results[query_id] = {"status": "failed", "message": msg}
            failed += 1

    print()
    print(f"Results: {passed} passed, {failed} failed, {xfailed} xfailed, {not_validated} skipped")

    if failed > 0:
        overall = "failed"
    elif xfailed > 0:
        overall = "xfail"
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
        description="Validate TPC-H query results against expected parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "results_dir",
        help="Directory containing q1.parquet ... q22.parquet result files",
    )
    parser.add_argument(
        "--expected-dir",
        required=True,
        help="Directory containing expected parquet files "
             "(e.g. /scratch/prestouser/tpch-rs-no-delta-expected/scale-10000)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    results_dir  = Path(args.results_dir)
    expected_dir = Path(args.expected_dir)

    if not results_dir.is_dir():
        print(f"Error: results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)
    if not expected_dir.is_dir():
        print(f"Error: expected directory not found: {expected_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Validating: {results_dir}")
    print(f"Expected:   {expected_dir}")
    print()

    results = validate(results_dir, expected_dir)

    # Write validation_results.json next to the query_results/ dir
    output_path = results_dir.parent / "validation_results.json"
    output_path.write_text(json.dumps(results, indent=2))
    print(f"Validation results written to {output_path}")

    sys.exit(0 if results["overall_status"] != "failed" else 1)
