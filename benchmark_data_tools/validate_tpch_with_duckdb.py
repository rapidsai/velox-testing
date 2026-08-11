#!/usr/bin/env python3
"""Validate archived Presto TPC-H result Parquet files against DuckDB.

The script reads benchmark data through DuckDB views, so it does not copy or
materialize the SF1000 tables. It checkpoints a JSON report after every query
and never deletes or overwrites an existing report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


TPCH_TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--presto-results", type=Path, required=True)
    parser.add_argument("--queries-file", type=Path, required=True)
    parser.add_argument("--actual-types-file", type=Path)
    parser.add_argument("--velox-testing-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scale-factor", type=float, default=1000.0)
    parser.add_argument("--queries", default=",".join(str(i) for i in range(1, 23)))
    parser.add_argument("--threads", type=int, default=112)
    parser.add_argument("--memory-limit", default="1500GB")
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: dict) -> None:
    temp_path = path.with_suffix(path.suffix + ".partial")
    with temp_path.open("x", encoding="utf-8") as output:
        json.dump(payload, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temp_path, path)


def checkpoint(path: Path, payload: dict) -> None:
    if path.exists():
        temp_path = path.with_suffix(path.suffix + ".partial")
        if temp_path.exists():
            raise FileExistsError(f"refusing to replace leftover checkpoint {temp_path}")
        with temp_path.open("x", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_path, path)
    else:
        atomic_write_json(path, payload)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    args = parse_args()
    if args.output.exists() or args.output.with_suffix(args.output.suffix + ".partial").exists():
        raise FileExistsError(f"refusing to overwrite output {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    query_ids = [f"Q{int(value)}" for value in args.queries.split(",")]
    if len(query_ids) != len(set(query_ids)):
        raise ValueError("duplicate query IDs")

    required_paths = [args.data_root, args.presto_results, args.queries_file, args.velox_testing_root]
    if args.actual_types_file:
        required_paths.append(args.actual_types_file)
    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(path)

    sys.path.insert(0, str(args.velox_testing_root))
    import duckdb  # pylint: disable=import-outside-toplevel
    import pandas as pd  # pylint: disable=import-outside-toplevel
    from common.testing.result_comparison import (  # pylint: disable=import-outside-toplevel
        validate_query_result,
    )

    with args.queries_file.open(encoding="utf-8") as source:
        queries = json.load(source)
    actual_types = {}
    if args.actual_types_file:
        with args.actual_types_file.open(encoding="utf-8") as source:
            actual_types = json.load(source)
    queries["Q11"] = queries["Q11"].format(SF_FRACTION=f"{0.0001 / args.scale_factor:.12f}")
    queries["Q15"] = queries["Q15"].replace(" AS supplier_no", "").replace("supplier_no", "l_suppkey")

    report = {
        "contract": {
            "data_root": str(args.data_root.resolve()),
            "actual_types_file": str(args.actual_types_file.resolve()) if args.actual_types_file else None,
            "actual_types_file_sha256": sha256(args.actual_types_file) if args.actual_types_file else None,
            "memory_limit": args.memory_limit,
            "presto_results": str(args.presto_results.resolve()),
            "queries": query_ids,
            "queries_file": str(args.queries_file.resolve()),
            "queries_file_sha256": sha256(args.queries_file),
            "scale_factor": args.scale_factor,
            "threads": args.threads,
            "velox_testing_root": str(args.velox_testing_root.resolve()),
        },
        "environment": {
            "duckdb": duckdb.__version__,
            "pandas": pd.__version__,
            "platform": platform.platform(),
            "python": sys.version,
        },
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "queries": {},
        "overall_status": "running",
    }

    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads={args.threads}")
    connection.execute(f"SET memory_limit={sql_string(args.memory_limit)}")

    table_manifest = {}
    for table in TPCH_TABLES:
        table_dir = args.data_root / table
        files = sorted(table_dir.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"no Parquet files for {table}: {table_dir}")
        glob_path = str(table_dir.resolve() / "*.parquet")
        connection.execute(
            f'CREATE VIEW "{table}" AS '
            f"SELECT * FROM read_parquet({sql_string(glob_path)}, union_by_name=true)"
        )
        table_manifest[table] = {
            "files": len(files),
            "bytes": sum(path.stat().st_size for path in files),
            "glob": glob_path,
        }
    report["tables"] = table_manifest
    checkpoint(args.output, report)

    for query_id in query_ids:
        query = queries[query_id]
        result_path = args.presto_results / f"{query_id.lower()}.parquet"
        if not result_path.is_file():
            raise FileNotFoundError(result_path)

        started = time.perf_counter()
        entry = {
            "presto_result": str(result_path.resolve()),
            "presto_result_sha256": sha256(result_path),
            "status": "running",
        }
        report["queries"][query_id] = entry
        checkpoint(args.output, report)
        try:
            reference_df = connection.sql(query).df()
            presto_df = connection.read_parquet(str(result_path.resolve())).df()
            status, message = validate_query_result(
                query_id,
                presto_df,
                reference_df,
                query,
                actual_types.get(query_id),
            )
            entry.update(
                {
                    "duckdb_rows": len(reference_df),
                    "message": message,
                    "presto_rows": len(presto_df),
                    "status": status,
                }
            )
        except Exception as error:  # Preserve evidence and continue through all queries.
            entry.update(
                {
                    "error_type": type(error).__name__,
                    "message": str(error)[:2000],
                    "status": "failed",
                }
            )
        entry["elapsed_seconds"] = round(time.perf_counter() - started, 6)
        checkpoint(args.output, report)
        print(
            f"{query_id}: {entry['status']} in {entry['elapsed_seconds']:.3f}s"
            + (f" ({entry['message']})" if entry.get("message") else ""),
            flush=True,
        )

    statuses = [report["queries"][query_id]["status"] for query_id in query_ids]
    report["overall_status"] = "passed" if all(status == "passed" for status in statuses) else "failed"
    report["ended_utc"] = datetime.now(timezone.utc).isoformat()
    report["elapsed_seconds"] = round(
        sum(report["queries"][query_id]["elapsed_seconds"] for query_id in query_ids), 6
    )
    checkpoint(args.output, report)
    return 0 if report["overall_status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
