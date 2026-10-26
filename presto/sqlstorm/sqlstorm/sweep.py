# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Runs SQLStorm StackOverflow queries against a running Presto cluster.

Each query is converted from PostgreSQL to Presto SQL on the fly, then either
EXPLAINed (--mode explain) or executed (--mode run). One CSV row is appended per
query; queries already in the output file are skipped, so an interrupted sweep can
be resumed by rerunning the same command.
"""

import argparse
import csv
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import prestodb
import requests

from sqlstorm.convert import convert

FIELDS = ["query", "engine", "mode", "status", "error_name", "message", "elapsed_s"]


def query_files(sqlstorm_dir, offset, limit):
    queries_dir = os.path.join(sqlstorm_dir, "v1.0", "stackoverflow", "queries")
    names = sorted((f for f in os.listdir(queries_dir) if f.endswith(".sql")), key=lambda f: int(f.split(".")[0]))
    names = names[offset:]
    if limit is not None:
        names = names[:limit]
    return [(name.removesuffix(".sql"), os.path.join(queries_dir, name)) for name in names]


def already_done(output_path):
    if not os.path.exists(output_path):
        return set()
    with open(output_path, newline="") as f:
        return {row["query"] for row in csv.DictReader(f)}


def explain_failures(explain_path):
    """Maps query -> reason for queries that did not pass EXPLAIN in an earlier sweep."""
    if not explain_path:
        return {}
    with open(explain_path, newline="") as f:
        return {
            row["query"]: f"{row['status']}: {row['error_name']} {row['message']}".strip()
            for row in csv.DictReader(f)
            if row["status"] != "ok"
        }


class Cluster:
    def __init__(self, host, port, schema, timeout_s, restart_command, worker_check_command):
        self.host = host
        self.port = port
        self.schema = schema
        self.timeout_s = timeout_s
        self.restart_command = restart_command
        self.worker_check_command = worker_check_command
        self.restart_lock = threading.Lock()

    def connect(self):
        return prestodb.dbapi.connect(
            host=self.host,
            port=self.port,
            user="sqlstorm",
            catalog="hive",
            schema=self.schema,
            session_properties={"query_max_execution_time": f"{self.timeout_s}s", "offset_clause_enabled": "true"},
            request_timeout=self.timeout_s + 60,
        )

    def active_workers(self):
        try:
            return requests.get(f"http://{self.host}:{self.port}/v1/cluster", timeout=5).json()["activeWorkers"]
        except (requests.RequestException, ValueError, KeyError):
            return 0

    def wait_for_active_workers(self, stable_polls=3, max_wait_s=240):
        stable = 0
        deadline = time.monotonic() + max_wait_s
        while time.monotonic() < deadline:
            stable = stable + 1 if self.active_workers() > 0 else 0
            if stable >= stable_polls:
                return
            time.sleep(1)
        raise RuntimeError("No active workers registered with the coordinator")

    def workers_alive(self):
        if self.worker_check_command:
            return subprocess.run(self.worker_check_command, shell=True, capture_output=True).returncode == 0
        return self.active_workers() > 0

    def ensure_workers(self):
        """Restarts the cluster if its workers are gone. Returns True if a restart happened."""
        if self.workers_alive() or not self.restart_command:
            return False
        with self.restart_lock:
            if self.workers_alive():
                return False
            print("Workers are down; restarting", flush=True)
            subprocess.run(self.restart_command, shell=True, check=True, capture_output=True)
            self.wait_for_active_workers()
            return True


class WorkerCrashed(Exception):
    pass


def execute(cluster, sql, mode):
    """Runs the query to completion. With a worker check command, the worker is polled
    while the query runs so a crash is reported immediately instead of after the
    coordinator's timeout."""
    cursor = cluster.connect().cursor()
    statement = f"EXPLAIN {sql}" if mode == "explain" else sql

    def drain():
        cursor.execute(statement)
        while cursor.fetchmany(10000):
            pass

    if not cluster.worker_check_command:
        return drain()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(drain)
        while True:
            try:
                return future.result(timeout=0.5)
            except TimeoutError:
                if not cluster.workers_alive():
                    try:
                        cursor.cancel()
                    except Exception:
                        pass
                    raise WorkerCrashed("worker process died while running the query")


def run_one(cluster, name, path, engine, mode, skipped):
    row = {"query": name, "engine": engine, "mode": mode, "elapsed_s": ""}
    if name in skipped:
        return row | {"status": "not_converted", "error_name": "", "message": skipped[name][:500]}
    with open(path) as f:
        conversion = convert(f.read())
    if not conversion.ok:
        return row | {"status": "not_converted", "error_name": "", "message": conversion.reason}

    for attempt in range(3):
        cluster.ensure_workers()
        start = time.monotonic()
        user_error = False
        try:
            execute(cluster, conversion.sql, mode)
            status, error_name, message = "ok", "", ""
        except WorkerCrashed as e:
            status, error_name, message = "crash", "WORKER_CRASHED", str(e)
        except prestodb.exceptions.PrestoQueryError as e:
            error_name = e.error_name or ""
            user_error = e.error_type == "USER_ERROR"
            status = "timeout" if error_name == "EXCEEDED_TIME_LIMIT" else "error"
            message = e.message
        except Exception as e:
            status, error_name, message = "error", type(e).__name__, str(e)
        elapsed = time.monotonic() - start
        # The coordinator had not yet re-registered a restarted worker; not the query's fault.
        if error_name != "GENERIC_INSUFFICIENT_RESOURCES":
            break
        cluster.wait_for_active_workers()

    if status != "ok" and not user_error and cluster.ensure_workers():
        status, error_name = "crash", "WORKER_CRASHED"
    message = " ".join((message or "").split())[:500]
    return row | {"status": status, "error_name": error_name, "message": message, "elapsed_s": f"{elapsed:.3f}"}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlstorm-dir", required=True, help="Checkout of github.com/SQL-Storm/SQLStorm")
    parser.add_argument("--engine", required=True, help="Label written to the results, e.g. java, cpu, gpu")
    parser.add_argument("--mode", choices=["explain", "run"], default="run")
    parser.add_argument("--output", required=True, help="Results CSV; appended to and used for resuming")
    parser.add_argument("--offset", type=int, default=0, help="Skip this many queries (in numeric filename order)")
    parser.add_argument("--limit", type=int, default=None, help="Number of queries to consider")
    parser.add_argument("--timeout", type=int, default=10, help="Per-query execution time limit in seconds")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--schema", default="stackoverflow_dba")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--restart-command",
        default=None,
        help="Shell command that restarts the cluster when a query leaves it without active workers",
    )
    parser.add_argument(
        "--converted-from",
        default=None,
        help="Results CSV of an EXPLAIN sweep; queries that failed it are recorded as not_converted without running",
    )
    parser.add_argument(
        "--worker-check-command",
        default=None,
        help="Shell command that exits 0 while the worker is alive (default: ask the coordinator)",
    )
    args = parser.parse_args()

    cluster = Cluster(
        args.host, args.port, args.schema, args.timeout, args.restart_command, args.worker_check_command
    )
    skipped = explain_failures(args.converted_from)
    done = already_done(args.output)
    todo = [(name, path) for name, path in query_files(args.sqlstorm_dir, args.offset, args.limit) if name not in done]
    print(f"{args.engine}/{args.mode}: {len(todo)} queries to run, {len(done)} already in {args.output}", flush=True)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    new_file = not os.path.exists(args.output)
    counts = {}
    with open(args.output, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            results = pool.map(lambda q: run_one(cluster, *q, args.engine, args.mode, skipped), todo)
            for i, row in enumerate(results, 1):
                writer.writerow(row)
                f.flush()
                counts[row["status"]] = counts.get(row["status"], 0) + 1
                if i % 100 == 0 or i == len(todo):
                    print(f"{i}/{len(todo)} {counts}", flush=True)


if __name__ == "__main__":
    main()
