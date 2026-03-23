# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
Gather run configuration from execution context. Engine (presto-java / presto-velox-cpu / presto-velox-gpu)
is determined from the coordinator's cluster-tag (via /v1/cluster). GPU name is
read from worker log files (LOGS_DIR env var). Scale factor and n_workers come from
schema and Presto /v1/node respectively.
"""

import json
import os
import re
from pathlib import Path

import prestodb

from ..common import test_utils
from .presto_api import get_cluster_tag, get_nodes

# Enabled by run_benchmark.sh --verbose (sets PRESTO_BENCHMARK_DEBUG=1)
_DEBUG = os.environ.get("PRESTO_BENCHMARK_DEBUG", "") == "1" or os.environ.get("DEBUG", "") == "1"


def _debug(msg: str) -> None:
    if _DEBUG:
        print(f"[run_context] {msg}")


def _get_node_count(hostname: str, port: int) -> int | None:
    """Return number of nodes in the Presto /v1/node list (workers only; coordinator not listed)."""
    nodes = get_nodes(hostname, port)
    if nodes is None:
        return None
    n = len(nodes)
    _debug(f"get_node_count: {n} node(s)")
    return n


_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _get_schema_info(hostname: str, port: int, user: str, schema_name: str) -> dict:
    """
    Resolve schema metadata from the schema's data source (metadata.json next to table data).
    Uses same logic as test_utils.get_scale_factor but without pytest request.

    Returns a dict with:
        scale_factor: int | float | None
        data_dir: str | None  — parent directory of the table data (e.g. /data/sf100)
    """
    result: dict = {"scale_factor": None, "data_dir": None}
    if not _SAFE_IDENTIFIER_RE.match(schema_name):
        _debug(f"schema_name {schema_name!r} contains unsafe characters, skipping schema info lookup")
        return result
    conn = None
    try:
        conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema_name)
        cursor = conn.cursor()
        tables = cursor.execute(f"SHOW TABLES IN {schema_name}").fetchall()
        if not tables:
            return result
        table = tables[0][0]
        location = test_utils.get_table_external_location(schema_name, table, cursor)
        data_dir = Path(location).parent.resolve()
        result["data_dir"] = str(data_dir)
        meta_path = data_dir / "metadata.json"
        if meta_path.is_file():
            with open(meta_path) as f:
                data = json.load(f)
            result["scale_factor"] = data.get("scale_factor")
        return result
    except Exception as e:
        _debug(f"schema info lookup failed: {e}")
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _find_worker_log(worker_id: int = 0) -> Path | None:
    """Locate the worker log file for *worker_id*.

    Tries, in order:
    1. ``LOGS_DIR/worker_<id>_<RUN_TIMESTAMP>.log`` (exact match when RUN_TIMESTAMP is set)
    2. The newest ``LOGS_DIR/worker_<id>_*.log`` by mtime (glob fallback)
    3. ``LOGS_DIR/worker_<id>.log`` (legacy non-timestamped name)
    """
    logs_dir = os.environ.get("LOGS_DIR")
    if not logs_dir:
        return None
    logs_path = Path(logs_dir)

    ts = os.environ.get("RUN_TIMESTAMP")
    if ts:
        exact = logs_path / f"worker_{worker_id}_{ts}.log"
        if exact.is_file():
            return exact

    candidates = sorted(logs_path.glob(f"worker_{worker_id}_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    legacy = logs_path / f"worker_{worker_id}.log"
    if legacy.is_file():
        return legacy
    return None


def _get_gpu_name_from_worker_logs() -> str | None:
    """Read GPU model name from worker log files.

    Both Docker and SLURM workers write a 'GPU Name: <model>' line
    to LOGS_DIR/worker_<id>_<timestamp>.log before starting the server.
    All workers are assumed to have the same GPU; only worker_0 is read.
    Returns None when LOGS_DIR is unset or no matching line is found.
    """
    log_file = _find_worker_log(0)
    if log_file is None:
        return None
    try:
        with open(log_file) as f:
            for line in f:
                if line.startswith("GPU Name:"):
                    return line.split(":", 1)[1].strip()
    except Exception as e:
        _debug(f"failed to read GPU name from worker logs: {e}")
    return None


_CLUSTER_TAG_TO_ENGINE = {
    "native-gpu": "presto-velox-gpu",
    "native-cpu": "presto-velox-cpu",
    "java": "presto-java",
}


def _get_engine(hostname: str, port: int) -> str:
    """Determine worker engine type from the coordinator's cluster-tag.

    Queries /v1/cluster for the tag set in the coordinator's config
    (e.g. 'native-gpu', 'native-cpu', 'java') and maps it to our
    internal engine name.  Raises RuntimeError if the tag is missing or
    unrecognised.
    """
    tag = get_cluster_tag(hostname, port)
    if tag is None:
        raise RuntimeError(
            "Could not determine worker engine: cluster-tag is not set on the "
            "coordinator.  Ensure the coordinator's config.properties includes "
            "cluster-tag=native-gpu, cluster-tag=native-cpu, or cluster-tag=java."
        )
    engine = _CLUSTER_TAG_TO_ENGINE.get(tag)
    if engine is None:
        raise RuntimeError(
            f"Unrecognised cluster-tag '{tag}'.  Expected one of: {', '.join(sorted(_CLUSTER_TAG_TO_ENGINE))}."
        )
    _debug(f"cluster-tag={tag!r} -> engine={engine!r}")
    return engine


def _get_gpu_name() -> str | None:
    """Return GPU model name from worker log files.

    Worker containers (Docker and SLURM) run nvidia-smi and write the
    output to LOGS_DIR/worker_<id>_<timestamp>.log.  Returns None when
    LOGS_DIR is unset or no GPU info is found in the logs.
    """
    gpu_name = _get_gpu_name_from_worker_logs()
    _debug(f"gpu_name: {gpu_name!r}")
    return gpu_name


def _get_num_drivers() -> int | None:
    """Parse task.max-drivers-per-task from worker log files.

    The Presto native server logs its configuration at startup.
    Returns None when LOGS_DIR is unset or the property is not found.
    """
    log_file = _find_worker_log(0)
    if log_file is None:
        _debug("num_drivers: worker_0 log not found")
        return None
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if "task.max-drivers-per-task" in line:
                    parts = line.split("task.max-drivers-per-task=", 1)
                    if len(parts) == 2:
                        value = parts[1].split()[0].rstrip(",")
                        try:
                            return int(value)
                        except ValueError:
                            pass
    except Exception as e:
        _debug(f"failed to parse num_drivers from worker logs: {e}")
    _debug(f"num_drivers: task.max-drivers-per-task not found in {log_file}")
    return None


def gather_run_context(
    hostname: str,
    port: int,
    user: str,
    schema_name: str,
) -> dict:
    """
    Build run-config dict from context. Engine is determined from the
    coordinator's cluster-tag (via /v1/cluster). Scale factor is read
    from the metadata file next to the schema's table data.
    """
    ctx = {}
    schema_info = _get_schema_info(hostname, port, user, schema_name)
    sf = schema_info["scale_factor"]
    if sf is not None:
        ctx["scale_factor"] = int(sf) if isinstance(sf, float) and sf == int(sf) else sf
    if schema_info["data_dir"] is not None:
        ctx["data_dir"] = schema_info["data_dir"]

    n_workers = _get_node_count(hostname, port)
    engine = _get_engine(hostname, port)

    if n_workers is not None:
        ctx["worker_count"] = n_workers
        ctx["kind"] = "single-node" if n_workers == 1 else f"{n_workers}-node"

    ctx["engine"] = engine
    if engine == "presto-velox-gpu":
        ctx["gpu_count"] = n_workers if n_workers is not None else 0
        gpu_name = _get_gpu_name()
        if gpu_name is not None:
            ctx["gpu_name"] = gpu_name
    elif engine == "presto-velox-cpu":
        ctx["gpu_count"] = 0
        ctx["gpu_name"] = "NA"
    elif engine == "presto-java":
        ctx["gpu_count"] = 0
        ctx["gpu_name"] = "NA"

    num_drivers = _get_num_drivers()
    if num_drivers is not None:
        ctx["num_drivers"] = num_drivers

    # Always 1 for single-run invocations; reserved for future multi-execution support.
    ctx["execution_number"] = 1

    return ctx
