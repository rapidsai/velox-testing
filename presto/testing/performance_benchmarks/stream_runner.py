# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Multi-stream TPC throughput runner.

Each stream owns a dedicated Presto connection/cursor and executes the full
query suite sequentially. Suite order is seeded and reshuffled per suite
repeat. Query execution retries until success (no backoff); wall time of
failed attempts is included in the measured latency. Validation runs in an
untimed region after a successful execution.
"""

from __future__ import annotations

import hashlib
import json
import logging
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import prestodb

from common.testing.result_comparison import ValidationStatus, validate_query_result

MAX_EXECUTION_ATTEMPTS = 10
MAX_RECORDED_ERRORS = 3
ERROR_SNIPPET_CHARS = 100


def derive_stream_seed(run_seed: int, stream_id: int) -> int:
    """Stable 32-bit seed derived from the main run seed and stream id."""
    digest = hashlib.sha256(f"{run_seed}:{stream_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _snippet(text: str, n: int = ERROR_SNIPPET_CHARS) -> str:
    text = text.replace("\n", "\\n")
    if len(text) <= 2 * n:
        return text
    return f"{text[:n]}...{text[-n:]}"


def _format_error(exc: BaseException) -> dict[str, str]:
    message = str(exc)
    error_type = getattr(exc, "error_type", None)
    error_name = getattr(exc, "error_name", None)
    if error_type or error_name:
        message = f"{error_type or '?'}: {error_name or '?'}: {message}"
    return {
        "type": type(exc).__name__,
        "message": message,
        "first_100": message[:ERROR_SNIPPET_CHARS],
        "last_100": message[-ERROR_SNIPPET_CHARS:],
        "snippet": _snippet(message),
    }


@dataclass
class AttemptRecord:
    attempt: int
    elapsed_ms: float
    ok: bool
    error: dict[str, str] | None = None


@dataclass
class QueryExecution:
    query_id: str
    suite_repeat: int
    repetition: int
    total_elapsed_ms: float
    status: str  # ok | execution_fail | validation_fail
    attempts: list[AttemptRecord] = field(default_factory=list)
    validation_status: ValidationStatus | None = None
    validation_message: str | None = None


@dataclass
class StreamResult:
    stream_id: int
    seed: int
    executions: list[QueryExecution] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    wall_ms: float = 0.0


class StreamLogger:
    """One log file per stream/cursor."""

    def __init__(self, path: Path, stream_id: int, seed: int):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._logger = logging.getLogger(f"throughput.stream.{stream_id}")
        self._logger.setLevel(logging.INFO)
        self._logger.handlers.clear()
        self._logger.propagate = False
        handler = logging.FileHandler(self.path, mode="w")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        self._logger.addHandler(handler)
        self._logger.info("stream_id=%s seed=%s", stream_id, seed)

    def info(self, msg: str, *args: Any) -> None:
        self._logger.info(msg, *args)

    def error(self, msg: str, *args: Any) -> None:
        self._logger.error(msg, *args)

    def close(self) -> None:
        for handler in list(self._logger.handlers):
            handler.close()
            self._logger.removeHandler(handler)


def _shuffled_query_order(query_ids: list[str], seed: int, suite_repeat: int) -> list[str]:
    import random

    order = list(query_ids)
    rng = random.Random(seed + suite_repeat * 1_000_003)
    rng.shuffle(order)
    return order


def _execute_with_retries(
    cursor: Any,
    sql: str,
    query_id: str,
    benchmark_type: str,
    stream_logger: StreamLogger,
    recorded_errors: list[dict[str, str]],
) -> tuple[Any | None, list[AttemptRecord], float, str]:
    """Run query until success or MAX_EXECUTION_ATTEMPTS.

    Returns (cursor_after_success_or_None, attempts, total_elapsed_ms, status).
    total_elapsed_ms includes failed attempts (no backoff sleeps).
    """
    attempts: list[AttemptRecord] = []
    total_elapsed_ms = 0.0
    tagged_sql = f"--{benchmark_type}_{query_id}_throughput--\n{sql}"

    for attempt in range(1, MAX_EXECUTION_ATTEMPTS + 1):
        t0 = time.perf_counter()
        try:
            result_cursor = cursor.execute(tagged_sql)
            # Prefer server-reported elapsed time when available; fall back to client wall.
            server_ms = None
            stats = getattr(result_cursor, "stats", None) or getattr(cursor, "stats", None)
            if isinstance(stats, dict):
                server_ms = stats.get("elapsedTimeMillis")
            client_ms = (time.perf_counter() - t0) * 1000.0
            elapsed_ms = float(server_ms) if server_ms is not None else client_ms
            total_elapsed_ms += elapsed_ms
            attempts.append(AttemptRecord(attempt=attempt, elapsed_ms=elapsed_ms, ok=True))
            stream_logger.info(
                "%s attempt=%s ok elapsed_ms=%.2f total_ms=%.2f",
                query_id,
                attempt,
                elapsed_ms,
                total_elapsed_ms,
            )
            return result_cursor, attempts, total_elapsed_ms, "ok"
        except Exception as exc:  # noqa: BLE001 — record and retry any client/server failure
            client_ms = (time.perf_counter() - t0) * 1000.0
            total_elapsed_ms += client_ms
            err = _format_error(exc)
            attempts.append(AttemptRecord(attempt=attempt, elapsed_ms=client_ms, ok=False, error=err))
            if len(recorded_errors) < MAX_RECORDED_ERRORS:
                recorded_errors.append(err)
            stream_logger.error(
                "%s attempt=%s fail elapsed_ms=%.2f error=%s",
                query_id,
                attempt,
                client_ms,
                err["snippet"],
            )

    return None, attempts, total_elapsed_ms, "execution_fail"


def _maybe_validate(
    df: pd.DataFrame,
    query_id: str,
    query_sql: str,
    reference_dir: Path | None,
) -> tuple[ValidationStatus, str | None]:
    if reference_dir is None:
        return "not-validated", "no reference results directory"
    q_num = int(query_id.lstrip("Qq"))
    expected_file = next(
        (
            reference_dir / name
            for name in (f"q{q_num:02d}.parquet", f"q{q_num}.parquet", f"{q_num:02d}.parquet")
            if (reference_dir / name).exists()
        ),
        None,
    )
    if expected_file is None:
        return "not-validated", f"expected parquet not found for {query_id}"
    expected = pd.read_parquet(expected_file)
    return validate_query_result(query_id.lower(), df, expected, query_sql)


def run_stream(
    stream_id: int,
    run_seed: int,
    hostname: str,
    port: int,
    user: str,
    schema: str,
    benchmark_type: str,
    queries: dict[str, str],
    query_ids: list[str],
    suite_repeats: int,
    repetitions_per_query: int,
    output_dir: Path,
    reference_results_dir: Path | None,
    save_results: bool,
    save_lock: threading.Lock,
    saved_queries: set[str],
) -> StreamResult:
    seed = derive_stream_seed(run_seed, stream_id)
    log_path = output_dir / "stream_logs" / f"stream_{stream_id}.log"
    stream_logger = StreamLogger(log_path, stream_id, seed)
    result = StreamResult(stream_id=stream_id, seed=seed)
    results_dir = output_dir / "query_results"

    wall_t0 = time.perf_counter()
    conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema)
    cursor = conn.cursor()
    try:
        for suite_repeat in range(suite_repeats):
            order = _shuffled_query_order(query_ids, seed, suite_repeat)
            stream_logger.info("suite_repeat=%s order=%s", suite_repeat, ",".join(order))
            for query_id in order:
                sql = queries[query_id]
                for repetition in range(repetitions_per_query):
                    result_cursor, attempts, total_ms, status = _execute_with_retries(
                        cursor=cursor,
                        sql=sql,
                        query_id=query_id,
                        benchmark_type=benchmark_type,
                        stream_logger=stream_logger,
                        recorded_errors=result.errors,
                    )
                    execution = QueryExecution(
                        query_id=query_id,
                        suite_repeat=suite_repeat,
                        repetition=repetition,
                        total_elapsed_ms=total_ms,
                        status=status,
                        attempts=attempts,
                    )
                    if status != "ok" or result_cursor is None:
                        result.executions.append(execution)
                        continue

                    # Untimed region: fetch + optional validation / result capture.
                    rows = result_cursor.fetchall()
                    columns = [desc[0] for desc in result_cursor.description]
                    df = pd.DataFrame(rows, columns=columns)

                    if save_results:
                        with save_lock:
                            if query_id not in saved_queries:
                                results_dir.mkdir(parents=True, exist_ok=True)
                                parquet_path = results_dir / f"{query_id.lower()}.parquet"
                                if not parquet_path.exists():
                                    df.to_parquet(parquet_path, index=False)
                                saved_queries.add(query_id)

                    val_status, val_message = _maybe_validate(
                        df, query_id, sql, reference_results_dir
                    )
                    execution.validation_status = val_status
                    execution.validation_message = val_message
                    if val_status == "failed":
                        execution.status = "validation_fail"
                        stream_logger.error(
                            "%s validation_fail message=%s", query_id, _snippet(val_message or "")
                        )
                    else:
                        stream_logger.info(
                            "%s validation=%s message=%s",
                            query_id,
                            val_status,
                            _snippet(val_message or ""),
                        )

                    result.executions.append(execution)
    finally:
        try:
            cursor.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        result.wall_ms = (time.perf_counter() - wall_t0) * 1000.0
        stream_logger.info("stream wall_ms=%.2f executions=%s", result.wall_ms, len(result.executions))
        stream_logger.close()

    # Persist structured stream summary beside the text log.
    summary_path = output_dir / "stream_logs" / f"stream_{stream_id}.json"
    summary_path.write_text(
        json.dumps(
            {
                "stream_id": stream_id,
                "seed": seed,
                "wall_ms": result.wall_ms,
                "errors": result.errors,
                "executions": [
                    {
                        "query_id": e.query_id,
                        "suite_repeat": e.suite_repeat,
                        "repetition": e.repetition,
                        "total_elapsed_ms": e.total_elapsed_ms,
                        "status": e.status,
                        "validation_status": e.validation_status,
                        "validation_message": e.validation_message,
                        "attempts": [
                            {
                                "attempt": a.attempt,
                                "elapsed_ms": a.elapsed_ms,
                                "ok": a.ok,
                                "error": a.error,
                            }
                            for a in e.attempts
                        ],
                    }
                    for e in result.executions
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return result


def run_throughput(
    *,
    num_streams: int,
    run_seed: int,
    hostname: str,
    port: int,
    user: str,
    schema: str,
    benchmark_type: str,
    queries: dict[str, str],
    query_ids: list[str],
    suite_repeats: int,
    repetitions_per_query: int,
    output_dir: Path,
    reference_results_dir: Path | None,
    save_results: bool = True,
) -> dict[str, Any]:
    """Launch concurrent streams and aggregate throughput metrics."""
    output_dir.mkdir(parents=True, exist_ok=True)
    overall_t0 = time.perf_counter()
    stream_results: list[StreamResult] = []
    save_lock = threading.Lock()
    saved_queries: set[str] = set()

    with ThreadPoolExecutor(max_workers=num_streams) as pool:
        futures = {
            pool.submit(
                run_stream,
                stream_id=stream_id,
                run_seed=run_seed,
                hostname=hostname,
                port=port,
                user=user,
                schema=schema,
                benchmark_type=benchmark_type,
                queries=queries,
                query_ids=query_ids,
                suite_repeats=suite_repeats,
                repetitions_per_query=repetitions_per_query,
                output_dir=output_dir,
                reference_results_dir=reference_results_dir,
                save_results=save_results,
                save_lock=save_lock,
                saved_queries=saved_queries,
            ): stream_id
            for stream_id in range(num_streams)
        }
        for future in as_completed(futures):
            stream_results.append(future.result())

    overall_wall_ms = (time.perf_counter() - overall_t0) * 1000.0
    stream_results.sort(key=lambda s: s.stream_id)
    return aggregate_results(
        stream_results=stream_results,
        query_ids=query_ids,
        overall_wall_ms=overall_wall_ms,
        num_streams=num_streams,
        suite_repeats=suite_repeats,
        repetitions_per_query=repetitions_per_query,
        run_seed=run_seed,
    )


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    # Nearest-rank style for p99 with small samples.
    idx = min(len(ordered) - 1, max(0, int(round(p * (len(ordered) - 1)))))
    return ordered[idx]


def aggregate_results(
    *,
    stream_results: list[StreamResult],
    query_ids: list[str],
    overall_wall_ms: float,
    num_streams: int,
    suite_repeats: int,
    repetitions_per_query: int,
    run_seed: int,
) -> dict[str, Any]:
    raw_times: dict[str, list[float]] = {qid: [] for qid in query_ids}
    stream_times: dict[str, dict[str, list[float]]] = {}
    failed_queries: dict[str, list[dict[str, Any]]] = {}
    validation: dict[str, dict[str, Any]] = {}

    completed = 0
    for stream in stream_results:
        sid = str(stream.stream_id)
        stream_times[sid] = {qid: [] for qid in query_ids}
        for execution in stream.executions:
            qid = execution.query_id
            if execution.status == "ok":
                raw_times.setdefault(qid, []).append(execution.total_elapsed_ms)
                stream_times[sid].setdefault(qid, []).append(execution.total_elapsed_ms)
                completed += 1
            else:
                failed_queries.setdefault(qid, []).append(
                    {
                        "stream_id": stream.stream_id,
                        "suite_repeat": execution.suite_repeat,
                        "repetition": execution.repetition,
                        "status": execution.status,
                        "validation_status": execution.validation_status,
                        "validation_message": execution.validation_message,
                        "total_elapsed_ms": execution.total_elapsed_ms,
                    }
                )
            if execution.validation_status and execution.validation_status != "not-validated":
                validation[qid] = {
                    "status": execution.validation_status,
                    "message": execution.validation_message,
                }

    p99 = {qid: _percentile(times, 0.99) for qid, times in raw_times.items()}
    p50 = {qid: statistics.median(times) if times else None for qid, times in raw_times.items()}
    avg = {qid: statistics.mean(times) if times else None for qid, times in raw_times.items()}

    queries_per_sec = (completed / (overall_wall_ms / 1000.0)) if overall_wall_ms > 0 else 0.0

    return {
        "raw_times_ms": raw_times,
        "stream_times_ms": stream_times,
        "failed_queries": failed_queries,
        "validation": validation,
        "agg_times_ms": {
            "avg": avg,
            "median": p50,
            "p99": p99,
        },
        "throughput": {
            "total_wall_ms": overall_wall_ms,
            "queries_completed": completed,
            "queries_per_sec": queries_per_sec,
            "stream_wall_ms": {str(s.stream_id): s.wall_ms for s in stream_results},
            "stream_seeds": {str(s.stream_id): s.seed for s in stream_results},
        },
        "errors_by_stream": {str(s.stream_id): s.errors for s in stream_results},
        "config": {
            "concurrency_streams": num_streams,
            "suite_repeats": suite_repeats,
            "repetitions_per_query": repetitions_per_query,
            "run_seed": run_seed,
        },
    }
