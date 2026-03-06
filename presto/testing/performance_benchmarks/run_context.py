# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
Gather run configuration from execution context. Engine (presto-java / presto-velox-cpu / presto-velox-gpu)
is determined from the coordinator's cluster-tag (via /v1/cluster). GPU name is
parsed from nvidia-smi output in worker log files (LOGS env var). Scale factor and
n_workers come from schema and Presto /v1/node respectively.
"""

import json
import os
import re
from pathlib import Path

import prestodb

from ..common import test_utils
from .presto_api import get_cluster_tag, get_nodes

# Set PRESTO_BENCHMARK_DEBUG=1 or DEBUG=1 to print engine-detection debug logs
_DEBUG = os.environ.get("PRESTO_BENCHMARK_DEBUG") or os.environ.get("DEBUG")


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


def _get_scale_factor_from_schema(hostname: str, port: int, user: str, schema_name: str) -> int | float | None:
    """
    Resolve scale factor from the schema's data source (metadata.json next to table data).
    Uses same logic as test_utils.get_scale_factor but without pytest request.
    """
    conn = None
    try:
        conn = prestodb.dbapi.connect(host=hostname, port=port, user=user, catalog="hive", schema=schema_name)
        cursor = conn.cursor()
        tables = cursor.execute(f"SHOW TABLES IN {schema_name}").fetchall()
        if not tables:
            return None
        table = tables[0][0]
        location = test_utils.get_table_external_location(schema_name, table, cursor)
        # metadata.json is typically in parent of table data dir
        meta_path = Path(location).parent / "metadata.json"
        if not meta_path.is_file():
            meta_path = (Path(location) / ".." / "metadata.json").resolve()
        if not meta_path.is_file():
            return None
        with open(meta_path) as f:
            data = json.load(f)
        return data.get("scale_factor")
    except Exception as e:
        _debug(f"scale factor lookup failed: {e}")
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _parse_gpu_name_from_text(line: str) -> str | None:
    """Parse a single line of nvidia-smi -L output; return GPU model name or None."""
    line = line.strip()
    if not line:
        return None
    match = re.search(r"GPU \d+:\s*(.+?)(?:\s*\(UUID)", line)
    if match:
        return match.group(1).strip()
    if line.startswith("GPU "):
        return line.split(":", 1)[-1].strip()
    return None


def _get_gpu_name_from_worker_logs() -> str | None:
    """Parse GPU model from nvidia-smi output in worker log files.

    Both Docker and SLURM workers write nvidia-smi -L output to
    LOGS/worker_<id>.log before starting the server.  All workers are
    assumed to have the same GPU model; only worker_0.log is read.
    Returns None when LOGS is unset or no matching line is found.
    """
    logs_dir = os.environ.get("LOGS")
    if not logs_dir:
        return None
    log_file = Path(logs_dir) / "worker_0.log"
    if not log_file.is_file():
        return None
    try:
        with open(log_file) as f:
            for line in f:
                gpu_name = _parse_gpu_name_from_text(line)
                if gpu_name:
                    return gpu_name
    except Exception:
        pass
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

    Worker containers (Docker and SLURM) run nvidia-smi -L and write the
    output to LOGS/worker_<id>.log.  Returns None when LOGS is unset or
    no GPU info is found in the logs.
    """
    gpu_name = _get_gpu_name_from_worker_logs()
    _debug(f"gpu_name: {gpu_name!r}")
    return gpu_name


_ENGINE_TO_VARIANT = {
    "presto-velox-gpu": "gpu",
    "presto-velox-cpu": "cpu",
    "presto-java": "java",
}


def _get_num_drivers(engine: str) -> int | None:
    """Read task.max-drivers-per-task from the generated worker config_native.properties.

    Falls back to None when the generated config directory does not exist
    (e.g. pre-configured cluster or SLURM without local config generation).
    """
    variant = _ENGINE_TO_VARIANT.get(engine)
    if variant is None:
        return None

    config_file = (
        Path(__file__).resolve().parent
        / ".."
        / ".."
        / "docker"
        / "config"
        / "generated"
        / variant
        / "etc_worker"
        / "config_native.properties"
    )
    if not config_file.is_file():
        _debug(f"num_drivers: config not found at {config_file}")
        return None

    for line in config_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("task.max-drivers-per-task="):
            try:
                return int(line.split("=", 1)[1])
            except (ValueError, IndexError):
                pass
    _debug(f"num_drivers: task.max-drivers-per-task not found in {config_file}")
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
    sf = _get_scale_factor_from_schema(hostname, port, user, schema_name)
    if sf is not None:
        ctx["scale_factor"] = int(sf) if isinstance(sf, float) and sf == int(sf) else sf

    n_workers = _get_node_count(hostname, port)
    engine = _get_engine(hostname, port)

    if n_workers is not None:
        ctx["n_workers"] = n_workers
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

    num_drivers = _get_num_drivers(engine)
    if num_drivers is not None:
        ctx["num_drivers"] = num_drivers

    ctx["execution_number"] = 1

    return ctx
