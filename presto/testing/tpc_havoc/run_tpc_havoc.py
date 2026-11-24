#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run TPC-Havoc (BenchBox) query variants against Presto and validate the results.

For every selected TPC-H query, the base query and its TPC-Havoc variants
(semantically equivalent rewrites) are executed sequentially. Each result is
compared with:

  * the base query result from the same run ("base"),
  * optionally, DuckDB executing BenchBox's DuckDB-dialect SQL of the same query
    over the same Parquet data ("duckdb", --duckdb-data-dir DIR),
  * optionally, the results of an earlier run ("reference", --reference-dir DIR),
    e.g. a GPU run compared with a CPU run.

Failures are matched against a known-issues file; the exit code is non-zero only
for failures that are not known issues (or for any failure with --fail-on-any).

With --worker-log, a query that fails because velox-cudf cannot run part of its plan
on the GPU (CPU fallback disabled: "Replacement with cuDF operator failed") gets the
reason the worker logged ("Validation failed at function: ...") as worker_reason, and
its failures are grouped by that reason.

Exit codes: 0 = no new failures, 1 = new failures, 2 = setup/usage error.

Output (under --output-dir[/--tag]):
  queries/<dialect>/<QID>.sql   exact SQL executed
  query_results/<qid>.parquet   Presto results
  duckdb_results/<qid>.parquet  DuckDB reference results (when enabled)
  summary.json                  per-query status, timings, checks, failures grouped by reason
  summary.md                    failures by reason with repro commands (nightly_summary.py format)
"""

from __future__ import annotations

import argparse
import fnmatch
import functools
import glob
import hashlib
import json
import os
import platform
import re
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import havoc_queries
import nightly_summary
import result_utils

TPCH_TABLES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
DEFAULT_KNOWN_ISSUES = Path(__file__).resolve().parent / "known_issues.yaml"
CHECKS = ("execution", "base", "duckdb", "reference")
# Errors that indicate the coordinator went away; these are recorded as errors
# and the run continues (the cluster may come back, or every remaining query
# fails fast with the same grouped message).
CONNECTION_ERROR_NAMES = ("ConnectionError", "ConnectTimeout", "ReadTimeout", "ChunkedEncodingError", "HTTPError")
# velox-cudf's error when it cannot run an operator on the GPU and CPU fallback is disabled.
CUDF_REPLACEMENT_FAILED = "Replacement with cuDF operator failed"


# ---------------------------------------------------------------------------
# Presto execution
# ---------------------------------------------------------------------------


class PrestoRunner:
    def __init__(self, args):
        import prestodb

        self._prestodb = prestodb
        self.args = args
        self.session_properties = dict(args.session or [])
        if args.timeout > 0:
            self.session_properties.setdefault("query_max_run_time", f"{int(args.timeout)}s")
        self.conn = None

    def _connect(self):
        if self.conn is None:
            self.conn = self._prestodb.dbapi.connect(
                host=self.args.hostname,
                port=self.args.port,
                user=self.args.user,
                catalog=self.args.catalog,
                schema=self.args.schema_name,
                session_properties=self.session_properties,
                # HTTP read timeout per request (not per query); keep it well above the poll interval.
                request_timeout=max(60.0, float(self.args.timeout) + 60.0) if self.args.timeout > 0 else 3600.0,
            )
        return self.conn

    def preflight(self) -> list[str]:
        cursor = self._connect().cursor()
        cursor.execute(f"SHOW TABLES FROM {self.args.catalog}.{self.args.schema_name}")
        tables = {row[0] for row in cursor.fetchall()}
        return [t for t in TPCH_TABLES if t not in tables]

    def run(self, sql: str) -> dict:
        record: dict = {"status": "ok"}
        cursor = None
        timed_out = threading.Event()
        timer = None
        start = time.monotonic()
        try:
            cursor = self._connect().cursor()

            def on_timeout():
                timed_out.set()
                try:
                    cursor.cancel()
                except Exception:
                    pass

            if self.args.timeout > 0:
                # Client-side guard in case the server-side limit is not enforced.
                timer = threading.Timer(self.args.timeout + 30, on_timeout)
                timer.daemon = True
                timer.start()
            cursor.execute(sql)
            rows = cursor.fetchall()
            description = cursor.description or []
            record.update(
                rows=rows,
                columns=[d[0] for d in description],
                types=[str(d[1]) if d[1] is not None else "unknown" for d in description],
            )
        except Exception as exc:
            record.update(_classify_presto_error(exc, timed_out.is_set()))
            if record.get("error_category") == "connection":
                self.conn = None  # reconnect for the next query
        finally:
            if timer:
                timer.cancel()
        record["wall_time_s"] = round(time.monotonic() - start, 3)
        try:
            stats = cursor.stats if cursor is not None else {}
            record["presto_query_id"] = getattr(getattr(cursor, "_query", None), "query_id", None)
            if stats.get("elapsedTimeMillis") is not None:
                record["elapsed_ms"] = stats.get("elapsedTimeMillis")
        except Exception:
            pass
        return record


def _classify_presto_error(exc: Exception, client_timeout: bool) -> dict:
    error_name = getattr(exc, "error_name", None)
    error_type = getattr(exc, "error_type", None)
    message = getattr(exc, "message", None) or str(exc)
    if not error_name and exc.args and isinstance(exc.args[0], dict):
        payload = exc.args[0]
        error_name = payload.get("errorName")
        error_type = payload.get("errorType")
        message = payload.get("message", message)
    exc_name = type(exc).__name__
    if client_timeout or error_name in ("EXCEEDED_TIME_LIMIT", "ABANDONED_QUERY"):
        status, category = "timeout", "timeout"
    elif exc_name in CONNECTION_ERROR_NAMES or "Connection" in exc_name or "Max retries exceeded" in message:
        status, category = "error", "connection"
    else:
        status, category = "error", "query"
    return {
        "status": status,
        "error_category": category,
        "error_name": error_name or exc_name,
        "error_type": error_type,
        "error_message": str(message)[:4000],
    }


# ---------------------------------------------------------------------------
# velox-cudf reasons from the worker log
# ---------------------------------------------------------------------------

# What the worker logs before CUDF_REPLACEMENT_FAILED, e.g.
#   Validation failed at function: canBeEvaluatedByCudf, reason: Operation is not supported in
#   cuDF execution: switch(...)
#   Validation failed at function: canRunOnGPU, reason: Operation is not supported in cuDF
#   execution: avg does not support input type DECIMAL(15, 2) on cuDF, PlanNode id: 25
#   Replacement Failed Operator: FilterProject[2] 2
_CUDF_REASON_RE = re.compile(
    r"Validation failed at function: (\w+), reason: (?:Operation is not supported in cuDF execution: )?"
    r"(.*?)(?:, PlanNode id: \d+)?$|Replacement Failed Operator: (\w+)",
    re.MULTILINE,
)


def extract_worker_reason(log_text: str) -> str | None:
    """The most specific velox-cudf reason in the log: an expression's, else an operator's, else
    the name of the operator that could not be replaced."""
    found: dict[str, str] = {}
    for function, reason, operator in _CUDF_REASON_RE.findall(log_text or ""):
        if not function:
            found.setdefault("replaced", f"{operator} operator cannot run on cuDF")
        elif reason.strip():
            found.setdefault("expression" if function == "canBeEvaluatedByCudf" else "operator", reason.strip())
    reason = found.get("expression") or found.get("operator") or found.get("replaced")
    return reason[:500] if reason else None


def worker_reason_category(reason: str) -> str:
    """Groups worker reasons that differ only in an expression's columns or literals."""
    if reason.startswith("switch("):
        return "velox-cudf: multi-branch CASE (switch) expression not supported"
    if re.fullmatch(r"-?\d+\.\d+", reason):
        return "velox-cudf: decimal constant expression not supported"
    function = re.match(r"(?:presto\.default\.)?(\w+)\(", reason)
    if function:
        return f"velox-cudf: {function.group(1)}() not supported"
    return f"velox-cudf: {reason}"


class WorkerLogTail:
    """Reads the worker log lines written while one query ran (queries run one at a time)."""

    def __init__(self, pattern: str | None):
        self.pattern = pattern
        self.offsets: dict[str, int] = {}

    def _files(self) -> list[str]:
        return sorted(glob.glob(self.pattern)) if self.pattern else []

    def mark(self) -> None:
        """Remember where each log ends; call before running a query."""
        self.offsets = {path: os.path.getsize(path) for path in self._files() if os.path.isfile(path)}

    def _read_new(self, limit: int = 16 << 20) -> str:
        chunks = []
        for path in self._files():
            try:
                with open(path, "rb") as fh:
                    fh.seek(self.offsets.get(path, 0))
                    chunks.append(fh.read(limit).decode("utf-8", "replace"))
            except OSError:
                pass
        return "\n".join(chunks)

    def reason(self, wait_s: float = 2.0) -> str | None:
        """The velox-cudf reason logged since mark(); waits up to wait_s for lines not yet written."""
        deadline = time.monotonic() + wait_s
        while self._files():
            reason = extract_worker_reason(self._read_new())
            if reason or time.monotonic() >= deadline:
                return reason
            time.sleep(0.2)
        return None


# ---------------------------------------------------------------------------
# DuckDB reference
# ---------------------------------------------------------------------------


class DuckDBReference:
    def __init__(self, data_dir: Path, timeout: float, threads: int | None, cache_dir: Path | None):
        import duckdb

        self.conn = duckdb.connect(":memory:")
        if threads:
            self.conn.execute(f"SET threads = {int(threads)}")
        self.timeout = timeout
        self.cache_dir = cache_dir
        # Cache entries are keyed by data set as well as SQL: most queries have the
        # same SQL at every scale factor, so a cache dir shared between SF10 and
        # SF100 runs must not return one data set's answers for the other.
        self.data_key = hashlib.sha256(str(Path(data_dir).resolve()).encode()).hexdigest()[:8]
        for table in TPCH_TABLES:
            table_dir = data_dir / table
            if table_dir.is_dir():
                pattern = str(table_dir / "**" / "*.parquet")
            elif (data_dir / f"{table}.parquet").exists():
                pattern = str(data_dir / f"{table}.parquet")
            else:
                raise FileNotFoundError(f"No Parquet data for table {table!r} under {data_dir}")
            self.conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{pattern}')")

    def run(self, query: havoc_queries.HavocQuery) -> dict:
        cache_file = None
        if self.cache_dir:
            cache_file = self.cache_dir / f"{query.query_id.lower()}.{self.data_key}.{query.sql_hash('duckdb')}.parquet"
            if cache_file.exists():
                return {"status": "ok", "frame": pd.read_parquet(cache_file), "wall_time_s": 0.0, "cached": True}
        timed_out = threading.Event()

        def on_timeout():
            timed_out.set()
            self.conn.interrupt()

        timer = threading.Timer(self.timeout, on_timeout) if self.timeout > 0 else None
        start = time.monotonic()
        try:
            if timer:
                timer.daemon = True
                timer.start()
            frame = self.conn.execute(query.sql["duckdb"]).df()
            record = {"status": "ok", "frame": frame}
            if cache_file:
                result_utils.write_frame(frame, cache_file)
        except Exception as exc:
            record = {
                "status": "timeout" if timed_out.is_set() else "error",
                "error_name": type(exc).__name__,
                "error_message": str(exc)[:4000],
            }
        finally:
            if timer:
                timer.cancel()
        record["wall_time_s"] = round(time.monotonic() - start, 3)
        return record


# ---------------------------------------------------------------------------
# Known issues
# ---------------------------------------------------------------------------


def load_known_issues(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    import yaml

    data = yaml.safe_load(path.read_text()) or {}
    entries = data.get("known_issues", data) if isinstance(data, dict) else data
    if not isinstance(entries, list):
        raise ValueError(f"{path}: expected a list under 'known_issues'")
    for entry in entries:
        if "query" not in entry:
            raise ValueError(f"{path}: known issue without 'query': {entry}")
        check = entry.get("check", "any")
        if check != "any" and check not in CHECKS:
            raise ValueError(f"{path}: invalid check {check!r} (expected one of {CHECKS} or 'any')")
    return entries


def _issue_applies(entry: dict, query_id: str, engine: str, scale_factor: float) -> bool:
    """Query, engine and scale factor filters of a known issue. ``query`` and ``engines`` are
    fnmatch globs, so e.g. ``gpu*`` covers both ``gpu`` and ``gpu-no-fallback``."""
    queries = entry["query"] if isinstance(entry["query"], list) else [entry["query"]]
    engines, scale_factors = entry.get("engines"), entry.get("scale_factors")
    return (
        any(fnmatch.fnmatchcase(query_id.upper(), str(q).upper()) for q in queries)
        and (not engines or any(fnmatch.fnmatchcase(engine, str(e)) for e in engines))
        and (not scale_factors or any(float(sf) == float(scale_factor) for sf in scale_factors))
    )


def match_known_issue(
    issues: list[dict], query_id: str, check: str, message: str, engine: str, scale_factor: float
) -> dict | None:
    for entry in issues:
        if (
            _issue_applies(entry, query_id, engine, scale_factor)
            and entry.get("check", "any") in ("any", check)
            and (not entry.get("match") or re.search(entry["match"], message or "", re.IGNORECASE | re.DOTALL))
        ):
            return entry
    return None


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def infer_scale_factor(schema_name: str) -> float | None:
    match = re.search(r"sf_?(\d+(?:[._]\d+)?)", schema_name, re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1).replace("_", "."))


def write_sql_files(queries: dict[str, havoc_queries.HavocQuery], output_dir: Path, dialects: list[str]) -> None:
    for dialect in dialects:
        sql_dir = output_dir / "queries" / dialect
        sql_dir.mkdir(parents=True, exist_ok=True)
        for qid, query in queries.items():
            (sql_dir / f"{qid}.sql").write_text(query.sql[dialect].rstrip() + ";\n")
    manifest = {
        qid: {
            "description": q.description,
            "base": q.base_id,
            **{f"sql_sha256_{d}": q.sql_hash(d) for d in dialects},
        }
        for qid, q in queries.items()
    }
    (output_dir / "queries" / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def run(args) -> int:
    output_dir = Path(args.output_dir).resolve()
    if args.tag:
        output_dir = output_dir / args.tag
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "query_results"
    duckdb_dir = output_dir / "duckdb_results"

    scale_factor = args.scale_factor or infer_scale_factor(args.schema_name)
    if scale_factor is None:
        print(f"[tpc-havoc] Error: cannot infer the scale factor from schema {args.schema_name!r}; pass --scale-factor")
        return 2

    dialects = ["presto"] + (["duckdb"] if args.duckdb_data_dir else [])
    print(f"[tpc-havoc] Generating queries via BenchBox (scale factor {scale_factor:g}, dialects {dialects})")
    all_queries = havoc_queries.generate_queries(scale_factor, dialects)
    try:
        selected_ids = havoc_queries.select_query_ids(list(all_queries), args.queries)
    except ValueError as exc:
        print(f"[tpc-havoc] Error: {exc}")
        return 2
    queries = {qid: all_queries[qid] for qid in selected_ids}
    write_sql_files(queries, output_dir, dialects)
    print(f"[tpc-havoc] {len(queries)} queries selected; SQL written to {output_dir / 'queries'}")

    known_issues = load_known_issues(Path(args.known_issues) if args.known_issues else None)

    reference_queries, reference_results_dir = {}, None
    if args.reference_dir:
        reference_dir = Path(args.reference_dir)
        summary_file = reference_dir / "summary.json"
        if not summary_file.exists():
            print(f"[tpc-havoc] Error: --reference-dir has no summary.json: {reference_dir}")
            return 2
        reference_queries = {q["query_id"]: q for q in json.loads(summary_file.read_text()).get("queries", [])}
        reference_results_dir = reference_dir / "query_results"

    duck = None
    if args.duckdb_data_dir:
        cache_dir = Path(args.duckdb_cache_dir) if args.duckdb_cache_dir else None
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
        duck = DuckDBReference(Path(args.duckdb_data_dir), args.duckdb_timeout, args.duckdb_threads, cache_dir)

    presto = PrestoRunner(args)
    worker_log = WorkerLogTail(args.worker_log)
    if not args.skip_preflight:
        try:
            missing = presto.preflight()
        except Exception as exc:
            # SHOW TABLES runs on a worker; a GPU worker without CPU fallback
            # (cudf.allow_cpu_fallback=false) cannot execute it. Missing tables
            # then surface as per-query errors instead.
            print(f"[tpc-havoc] Warning: cannot list tables in {args.catalog}.{args.schema_name}: {exc}")
            missing = []
        if missing:
            print(f"[tpc-havoc] Error: missing TPC-H tables in {args.catalog}.{args.schema_name}: {missing}")
            return 2

    started_at = datetime.now(timezone.utc)
    deadline = time.monotonic() + args.max_total_time if args.max_total_time > 0 else None
    normalized: dict[str, pd.DataFrame] = {}
    column_types: dict[str, list[str]] = {}
    records: list[dict] = []

    for index, (qid, query) in enumerate(queries.items(), 1):
        record: dict = {
            "query_id": qid,
            "base_id": query.base_id,
            "variant": query.variant,
            "description": query.description,
            "sql_sha256": query.sql_hash("presto"),
            "checks": {},
        }
        records.append(record)
        if deadline and time.monotonic() > deadline:
            record.update(status="skipped", error_message="--max-total-time exceeded")
            _classify_failures(record, known_issues, args.engine, scale_factor)
            continue

        worker_log.mark()
        result = presto.run(query.sql["presto"])
        if CUDF_REPLACEMENT_FAILED in result.get("error_message", ""):
            reason = worker_log.reason()
            if reason:
                result.update(worker_reason=reason, worker_reason_category=worker_reason_category(reason))
        rows = result.pop("rows", None)
        record.update(result)
        if result["status"] == "ok":
            record["row_count"] = len(rows)
            raw = result_utils.presto_rows_to_frame(rows, result["columns"], result["types"])
            result_utils.write_frame(raw, results_dir / f"{qid.lower()}.parquet")
            normalized[qid] = result_utils.normalize_frame(raw, result["types"])
            column_types[qid] = result["types"]
            record["checks"]["execution"] = {"status": "passed"}
        else:
            record["checks"]["execution"] = {
                "status": "failed",
                "message": f"{record['status'].upper()}: {record.get('error_name')}: {record.get('error_message')}",
            }

        # 1. Variant vs base from this run.
        if not query.is_base:
            record["checks"]["base"] = _check_against(
                normalized.get(qid),
                normalized.get(query.base_id),
                queries[query.base_id].sql["presto"],
                missing_msg=f"base query {query.base_id} has no result",
                comparator=functools.partial(result_utils.compare_variant_to_base, variant_sql=query.sql["presto"]),
                types=result_utils.merge_types(column_types.get(qid), column_types.get(query.base_id)),
            )

        # 2. DuckDB reference.
        if duck is not None:
            duck_result = duck.run(query)
            record["duckdb"] = {
                "status": duck_result["status"],
                "wall_time_s": duck_result.get("wall_time_s"),
                "cached": duck_result.get("cached", False),
                **({"error": duck_result.get("error_message")} if duck_result["status"] != "ok" else {}),
            }
            duck_frame = None
            if duck_result["status"] == "ok":
                duck_frame = result_utils.normalize_frame(duck_result["frame"])
                record["duckdb"]["row_count"] = len(duck_frame)
                if not duck_result.get("cached"):
                    result_utils.write_frame(duck_result["frame"], duckdb_dir / f"{qid.lower()}.parquet")
            record["checks"]["duckdb"] = _check_against(
                normalized.get(qid),
                duck_frame,
                query.sql["presto"],
                missing_msg=f"DuckDB reference {duck_result['status']}: {duck_result.get('error_message', '')[:300]}",
                types=result_utils.merge_types(column_types.get(qid)),
            )

        # 3. Earlier run (e.g. CPU reference for a GPU run).
        if reference_results_dir is not None:
            ref_record = reference_queries.get(qid)
            ref_frame = None
            missing_msg = f"{qid} not in reference run"
            if ref_record is not None:
                if ref_record.get("sql_sha256") != record["sql_sha256"]:
                    record["reference_sql_differs"] = True
                ref_file = reference_results_dir / f"{qid.lower()}.parquet"
                if ref_record.get("status") == "ok" and ref_file.exists():
                    ref_frame = result_utils.normalize_frame(pd.read_parquet(ref_file), ref_record.get("types"))
                else:
                    missing_msg = f"reference run status {ref_record.get('status')}"
            record["checks"]["reference"] = _check_against(
                normalized.get(qid),
                ref_frame,
                query.sql["presto"],
                missing_msg=missing_msg,
                types=result_utils.merge_types(column_types.get(qid), (ref_record or {}).get("types")),
            )

        _classify_failures(record, known_issues, args.engine, scale_factor)
        _print_progress(index, len(queries), record)

    summary = _build_summary(args, records, scale_factor, started_at, output_dir, dialects)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    title = " ".join(filter(None, ["TPC-Havoc", args.tag, f"({args.engine}, SF{scale_factor:g})"]))
    run_for_summary = {**summary, "label": args.engine, "dir": output_dir}
    (output_dir / "summary.md").write_text(nightly_summary.render_markdown([run_for_summary], title))

    totals = summary["totals"]
    print(
        f"[tpc-havoc] Done: {totals['queries']} queries, {totals['ok']} ok, {totals['error']} error, "
        f"{totals['timeout']} timeout; {totals['failed_queries']} with failures "
        f"({totals['new_failures']} new, {totals['known_failures']} known); "
        f"{len(summary['unexpected_passes'])} known issues passed. Output: {output_dir}"
    )
    if args.fail_on_any and totals["failed_queries"]:
        return 1
    if totals["new_failures"]:
        return 1
    if args.fail_on_unexpected_pass and summary["unexpected_passes"]:
        return 1
    return 0


def _check_against(actual, expected, sql, missing_msg, comparator=result_utils.compare, types=None) -> dict:
    if actual is None:
        return {"status": "not-validated", "message": "query has no result"}
    if expected is None:
        return {"status": "not-validated", "message": missing_msg}
    passed, message = comparator(actual, expected, sql, types)
    return {"status": "passed"} if passed else {"status": "failed", "message": message}


def _classify_failures(record: dict, known_issues: list[dict], engine: str, scale_factor: float) -> None:
    record["failures"] = []
    for check, outcome in record["checks"].items():
        if outcome["status"] != "failed":
            continue
        issue = match_known_issue(
            known_issues, record["query_id"], check, outcome.get("message", ""), engine, scale_factor
        )
        outcome["known_issue"] = issue.get("reason", True) if issue else None
        if issue and issue.get("repro"):
            outcome["known_issue_repro"] = issue["repro"]
        if issue and issue.get("issue"):
            outcome["known_issue_link"] = issue["issue"]
        record["failures"].append({"check": check, "known": issue is not None})
    record["has_new_failure"] = any(not f["known"] for f in record["failures"])
    # Known issues that did not reproduce (stale entries).
    record["unexpected_passes"] = []
    for entry in known_issues:
        if not _issue_applies(entry, record["query_id"], engine, scale_factor):
            continue
        checks = CHECKS if entry.get("check", "any") == "any" else (entry["check"],)
        ran = [record["checks"][c] for c in checks if c in record["checks"]]
        if ran and all(c["status"] == "passed" for c in ran):
            record["unexpected_passes"].append(entry.get("reason") or entry["query"])


def _print_progress(index: int, total: int, record: dict) -> None:
    checks = " ".join(
        f"{name}={outcome['status']}" for name, outcome in record["checks"].items() if name != "execution"
    )
    tag = "NEW-FAIL" if record["has_new_failure"] else ("known-fail" if record["failures"] else "")
    detail = ""
    if record["status"] != "ok":
        detail = f" [{record.get('error_name')}] {str(record.get('error_message', ''))[:160]}"
        if record.get("worker_reason"):
            detail += f" | worker: {record['worker_reason'][:160]}"
    print(
        f"[{index:3d}/{total}] {record['query_id']:<8} {record['status']:<7} {record.get('wall_time_s', 0):8.2f}s "
        f"rows={record.get('row_count', '-')!s:<6} {checks} {tag}{detail}",
        flush=True,
    )


def _build_summary(args, records, scale_factor, started_at, output_dir, dialects) -> dict:
    statuses = Counter(r["status"] for r in records)
    check_totals = {
        check: {
            status: sum(1 for r in records if r["checks"].get(check, {}).get("status") == status)
            for status in ("passed", "failed", "not-validated")
        }
        for check in CHECKS
        if any(check in r["checks"] for r in records)
    }
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in records:
        for f in r["failures"]:
            groups[(f["check"], nightly_summary.failure_label(r, f["check"]))].append(r["query_id"])
    failure_groups = [
        {"check": check, "label": label, "count": len(qids), "queries": qids}
        for (check, label), qids in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    ]
    return {
        "benchmark": "tpc-havoc",
        "tag": args.tag,
        "engine": args.engine,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "connection": {
            "hostname": args.hostname,
            "port": args.port,
            "catalog": args.catalog,
            "schema": args.schema_name,
            "user": args.user,
        },
        "scale_factor": scale_factor,
        "timeout_s": args.timeout,
        "dialects": dialects,
        "benchbox": havoc_queries.benchbox_version(),
        "reference": {
            "duckdb_data_dir": args.duckdb_data_dir,
            "reference_dir": args.reference_dir,
        },
        "known_issues_file": args.known_issues,
        "output_dir": str(output_dir),
        "totals": {
            "queries": len(records),
            "ok": statuses["ok"],
            "error": statuses["error"],
            "timeout": statuses["timeout"],
            "skipped": statuses["skipped"],
            "total_wall_time_s": round(sum(r.get("wall_time_s", 0) or 0 for r in records), 3),
            "failed_queries": sum(1 for r in records if r["failures"]),
            "new_failures": sum(1 for r in records if r["has_new_failure"]),
            "known_failures": sum(1 for r in records if r["failures"] and not r["has_new_failure"]),
            "checks": check_totals,
        },
        "unexpected_passes": [
            {"query_id": r["query_id"], "known_issue": p} for r in records for p in r["unexpected_passes"]
        ],
        "failure_groups": failure_groups,
        "queries": records,
    }


def _parse_session(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"session property must be KEY=VALUE, got {value!r}")
    key, val = value.split("=", 1)
    return key.strip(), val.strip()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    conn = parser.add_argument_group("connection")
    conn.add_argument("-H", "--hostname", default=os.environ.get("HOST_NAME", "localhost"))
    conn.add_argument("-p", "--port", type=int, default=int(os.environ.get("PORT", "8080")))
    conn.add_argument("-u", "--user", default="tpc_havoc")
    conn.add_argument("-c", "--catalog", default="hive")
    conn.add_argument("-s", "--schema-name", required=True)
    conn.add_argument(
        "--session",
        type=_parse_session,
        action="append",
        metavar="KEY=VALUE",
        help="Presto session property (repeatable)",
    )

    sel = parser.add_argument_group("query selection")
    sel.add_argument("-q", "--queries", help="Comma separated: N or QN (base + all variants), QN_VM (single variant)")
    sel.add_argument(
        "--scale-factor",
        type=float,
        help="Scale factor of the data (affects Q11's threshold). Inferred from the schema name if omitted",
    )

    exe = parser.add_argument_group("execution")
    exe.add_argument("--timeout", type=float, default=600, help="Per-query timeout in seconds (0 = none)")
    exe.add_argument(
        "--max-total-time",
        type=float,
        default=0,
        help="Stop issuing new queries after this many seconds (0 = no limit); rest marked skipped",
    )
    exe.add_argument("--skip-preflight", action="store_true", help="Don't check that the TPC-H tables exist")
    exe.add_argument(
        "--worker-log",
        help="Glob of the worker log file(s), e.g. presto/scripts/presto_logs/worker_*.log. A query failing with "
        f'"{CUDF_REPLACEMENT_FAILED}" gets the velox-cudf reason logged while it ran',
    )

    ref = parser.add_argument_group("validation")
    ref.add_argument(
        "--duckdb-data-dir",
        help="Also validate every query against DuckDB on this Parquet data (one sub-directory per table)",
    )
    ref.add_argument("--duckdb-timeout", type=float, default=1800)
    ref.add_argument("--duckdb-threads", type=int)
    ref.add_argument(
        "--duckdb-cache-dir", help="Cache DuckDB reference results here, keyed by query id, data dir and SQL hash"
    )
    ref.add_argument("--reference-dir", help="Output directory of an earlier run to compare against (e.g. CPU run)")

    out = parser.add_argument_group("output / CI")
    out.add_argument("-o", "--output-dir", default="tpc_havoc_output")
    out.add_argument("-t", "--tag", help="Sub-directory of --output-dir for this run")
    out.add_argument(
        "-e",
        "--engine",
        default=os.environ.get("TPC_HAVOC_ENGINE", "unknown"),
        help="Engine label used to match known issues (e.g. cpu, gpu, gpu-no-fallback, java)",
    )
    out.add_argument(
        "--known-issues", default=str(DEFAULT_KNOWN_ISSUES), help="YAML file of expected failures ('' to disable)"
    )
    out.add_argument("--fail-on-any", action="store_true", help="Exit 1 on any failure, including known issues")
    out.add_argument(
        "--fail-on-unexpected-pass", action="store_true", help="Exit 1 if a known issue no longer reproduces"
    )
    out.add_argument(
        "--list-queries", action="store_true", help="Generate/print the selected query ids and SQL; no run"
    )

    args = parser.parse_args(argv)
    if args.tag and not re.fullmatch(r"[A-Za-z0-9_.-]+", args.tag):
        parser.error("--tag must contain only alphanumeric, '_', '.', '-' characters")
    if args.duckdb_data_dir and not Path(args.duckdb_data_dir).is_dir():
        parser.error(f"--duckdb-data-dir not found: {args.duckdb_data_dir}")
    if args.reference_dir and not (Path(args.reference_dir) / "summary.json").is_file():
        parser.error(f"--reference-dir is not the output directory of an earlier run: {args.reference_dir}")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.list_queries:
        sf = args.scale_factor or infer_scale_factor(args.schema_name) or 1.0
        queries = havoc_queries.generate_queries(sf, ["presto"])
        for qid in havoc_queries.select_query_ids(list(queries), args.queries):
            print(f"-- {qid}: {queries[qid].description}\n{queries[qid].sql['presto']};\n")
        return 0
    try:
        return run(args)
    except KeyboardInterrupt:
        return 130
    except Exception:
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
