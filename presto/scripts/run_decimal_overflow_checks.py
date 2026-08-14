# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Run decimal overflow / div-by-zero queries and classify outcomes for A/B."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import prestodb

OVERFLOW_RE = re.compile(r"overflow", re.IGNORECASE)
DIV0_RE = re.compile(
    r"(division by zero|modulus by zero|divide by zero|div.*zero)",
    re.IGNORECASE,
)


def classify_error(message: str) -> str:
    if DIV0_RE.search(message or ""):
        return "div_by_zero"
    if OVERFLOW_RE.search(message or ""):
        return "overflow"
    return "other_error"


VALID_EXPECTATIONS = ("ok", "overflow", "div_by_zero")


def load_cases(queries_file: Path, expectations_file: Path | None) -> dict[str, dict]:
    """Pair each query with its declared expectation.

    queries.json stays a flat name -> SQL map so the shared harness in
    common/testing/test_utils.py can also consume it. Expectations live in a
    sibling expectations.json ({name: {"expect": ..., "note": ...}}) rather than
    being inferred from query names, since a name like ADD_OVERFLOW says nothing
    about whether the result type is wide enough to absorb the operation.
    """
    queries = json.loads(queries_file.read_text())
    if expectations_file is None:
        expectations_file = queries_file.parent / "expectations.json"
    if not expectations_file.exists():
        raise SystemExit(f"Missing expectations file: {expectations_file}")
    expectations = json.loads(expectations_file.read_text())

    missing = sorted(set(queries) - set(expectations))
    if missing:
        raise SystemExit(
            f"No expectation declared in {expectations_file} for: {', '.join(missing)}"
        )
    unknown = sorted(set(expectations) - set(queries))
    if unknown:
        raise SystemExit(
            f"{expectations_file} declares unknown queries: {', '.join(unknown)}"
        )

    cases: dict[str, dict] = {}
    for name, sql in queries.items():
        entry = expectations[name]
        expect = entry.get("expect")
        if expect not in VALID_EXPECTATIONS:
            raise SystemExit(
                f"{name}: expect must be one of {VALID_EXPECTATIONS}, got {expect!r}"
            )
        cases[name] = {"expect": expect, "sql": sql, "note": entry.get("note", "")}
    return cases


def run_query(cursor, sql: str) -> dict:
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return {
            "status": "ok",
            "row_count": len(rows),
            "rows_preview": rows[:5],
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - classify any Presto failure
        msg = str(exc)
        return {
            "status": classify_error(msg),
            "row_count": 0,
            "rows_preview": [],
            "error": msg,
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Classify decimal overflow / div-by-zero query outcomes."
    )
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--queries-file", required=True)
    parser.add_argument(
        "--expectations-file",
        default=None,
        help="Defaults to expectations.json next to --queries-file.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--tag", default="")
    args = parser.parse_args()

    queries = load_cases(
        Path(args.queries_file),
        Path(args.expectations_file) if args.expectations_file else None,
    )
    conn = prestodb.dbapi.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema,
    )
    cursor = conn.cursor()

    results = {}
    summary = {
        "ok": 0,
        "overflow": 0,
        "div_by_zero": 0,
        "other_error": 0,
        "match_expected": 0,
        "mismatch_expected": 0,
    }

    for name, entry in queries.items():
        expected = entry["expect"]
        sql = entry["sql"]
        outcome = run_query(cursor, sql)
        actual = outcome["status"]
        matched = actual == expected
        summary["match_expected" if matched else "mismatch_expected"] += 1
        summary[actual] = summary.get(actual, 0) + 1

        results[name] = {
            "expected": expected,
            "actual": actual,
            "matched_expectation": matched,
            "sql": sql,
            "note": entry.get("note", ""),
            "error": outcome["error"],
            "row_count": outcome["row_count"],
            "rows_preview": [[str(c) for c in row] for row in outcome["rows_preview"]],
        }
        flag = "PASS" if matched else "DIFF"
        err = f" | {outcome['error'][:120]}" if outcome["error"] else ""
        print(f"[{flag}] {name}: expected={expected} actual={actual}{err}")

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tag": args.tag,
        "coordinator": f"{args.host}:{args.port}",
        "catalog": args.catalog,
        "schema": args.schema,
        "queries_file": args.queries_file,
        "expectations_file": args.expectations_file
        or str(Path(args.queries_file).parent / "expectations.json"),
        "summary": summary,
        "results": results,
    }

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "decimal_overflow_check_result.json"
    out_path.write_text(json.dumps(report, indent=2) + "\n")

    txt_lines = [
        f"tag={args.tag}",
        f"coordinator={args.host}:{args.port}",
        f"schema={args.catalog}.{args.schema}",
        f"summary={json.dumps(summary)}",
        "",
    ]
    for name, r in results.items():
        txt_lines.append(
            f"{name}: expected={r['expected']} actual={r['actual']} "
            f"matched={r['matched_expectation']}"
        )
        if r.get("note"):
            txt_lines.append(f"  note: {r['note']}")
        if r["error"]:
            txt_lines.append(f"  error: {r['error']}")
    (out_dir / "decimal_overflow_check_result.txt").write_text("\n".join(txt_lines) + "\n")

    print()
    print(f"Wrote {out_path}")
    print(f"summary: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
