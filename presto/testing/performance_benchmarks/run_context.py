# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
Gather run configuration from execution context. Engine (java / velox-cpu / velox-gpu)
is inferred only from: (1) Docker running images with expected names and tag =
username, or (2) SLURM nvidia-smi in LOGS/worker_0.log. Scale factor, n_workers,
gpu_name, etc. come from schema, Presto /v1/node (node count), and nvidia-smi/env.
"""

import getpass
import json
import os
import re
import subprocess
from pathlib import Path

import prestodb
import requests

from ..common import test_utils

# Set PRESTO_BENCHMARK_DEBUG=1 or DEBUG=1 to print engine-detection debug logs
_DEBUG = os.environ.get("PRESTO_BENCHMARK_DEBUG") or os.environ.get("DEBUG")


def _debug(msg: str) -> None:
    if _DEBUG:
        print(f"[run_context] {msg}")


def _fetch_json(url: str, timeout: int = 10):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        _debug(f"GET {url} failed: {e!r}")
        return None


def get_node_count(hostname: str, port: int) -> int | None:
    """Return number of nodes in the Presto /v1/node list (workers only; coordinator not listed)."""
    url = f"http://{hostname}:{port}/v1/node"
    raw = _fetch_json(url)
    if raw is None:
        return None
    if not isinstance(raw, list):
        _debug(f"get_node_count: {url} returned type {type(raw).__name__}, expected list: {raw!r}")
        return None
    n = len(raw)
    _debug(f"get_node_count: {url} -> {n} node(s). First node sample: {raw[0] if raw else 'N/A'}")
    return n


def get_scale_factor_from_schema(hostname: str, port: int, user: str, schema_name: str) -> int | float | None:
    """
    Resolve scale factor from the schema's data source (metadata.json next to table data).
    Uses same logic as test_utils.get_scale_factor but without pytest request.
    """
    conn = None
    try:
        conn = prestodb.dbapi.connect(
            host=hostname, port=port, user=user, catalog="hive", schema=schema_name
        )
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
    except Exception:
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


def get_gpu_name_from_slurm_logs() -> str | None:
    """
    When running under SLURM, workers run nvidia-smi -L and write to LOGS/worker_<id>.log.
    All workers are assumed the same GPU; read worker_0.log only. LOGS env must be set.
    Returns None if not in SLURM, LOGS unset, or no matching line found.
    """
    if not os.environ.get("SLURM_JOB_ID"):
        return None
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


def get_engine_from_slurm() -> str | None:
    """
    Infer engine when running under SLURM from nvidia-smi -L output in LOGS/worker_0.log.
    If that log contains GPU lines (from nvidia-smi -L), return 'velox-gpu'; otherwise
    'velox-cpu'. Returns None if not in SLURM (SLURM_JOB_ID and LOGS unset) or LOGS
    not available. Does not use the Presto API for engine type.
    """
    if not os.environ.get("SLURM_JOB_ID"):
        return None
    if not os.environ.get("LOGS"):
        return None
    gpu_name = get_gpu_name_from_slurm_logs()
    if gpu_name is not None:
        _debug(f"SLURM: nvidia-smi in logs -> velox-gpu ({gpu_name!r})")
        return "velox-gpu"
    # SLURM but no GPU in logs -> CPU native
    _debug("SLURM: no nvidia-smi in logs -> velox-cpu")
    return "velox-cpu"


def get_gpu_name() -> str | None:
    """
    Return GPU model name. Under SLURM, read from LOGS/worker_<id>.log if LOGS is set;
    otherwise run nvidia-smi -L on the current host (e.g. Docker host).
    """
    gpu_from_logs = get_gpu_name_from_slurm_logs()
    if gpu_from_logs is not None:
        return gpu_from_logs
    try:
        result = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        first_line = result.stdout.strip().split("\n")[0]
        return _parse_gpu_name_from_text(first_line)
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return None


def get_worker_image() -> str | None:
    """Return worker image name from env (set by cluster/container setup)."""
    return os.environ.get("WORKER_IMAGE")


def _current_username() -> str:
    """Return the username of the user running the process (for Docker image tag matching)."""
    return os.environ.get("USER") or os.environ.get("USERNAME") or getpass.getuser() or ""


def get_engine_from_docker_containers(hostname: str, port: int) -> str | None:
    """
    Infer engine from running Docker containers whose image has an expected name
    (presto-native-worker-gpu, presto-native-worker-cpu, presto-java-worker) and
    a tag equal to the username of the user running the benchmarks. Returns
    'velox-gpu', 'velox-cpu', 'java', or None.
    """
    username = _current_username()
    if not username:
        _debug("docker: could not determine username, skip Docker engine detection")
        return None
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Image}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        images = [s.strip() for s in result.stdout.strip().splitlines() if s.strip()]
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        _debug("docker ps failed or not available")
        return None
    has_gpu = has_cpu = has_java = False
    for image in images:
        parts = image.rsplit(":", 1)
        name = parts[0] if parts else ""
        tag = parts[1] if len(parts) == 2 else ""
        if tag != username:
            continue
        if "presto-native-worker-gpu" in name:
            has_gpu = True
        if "presto-native-worker-cpu" in name:
            has_cpu = True
        if "presto-java-worker" in name:
            has_java = True
    if has_gpu or has_cpu or has_java:
        _debug(f"docker (image tag={username!r}): gpu={has_gpu}, cpu={has_cpu}, java={has_java}")
    if has_gpu:
        return "velox-gpu"
    if has_cpu:
        return "velox-cpu"
    if has_java:
        return "java"
    return None


def gather_run_context(
    hostname: str,
    port: int,
    user: str,
    schema_name: str,
    scale_factor_override: str | int | None = None,
) -> dict:
    """
    Build run-config dict from context. Engine is taken only from Docker (running
    images presto-native-worker-gpu/cpu or presto-java-worker with tag = username)
    or SLURM (nvidia-smi in LOGS/worker_0.log). scale_factor_override takes
    precedence over schema-derived scale factor.
    """
    ctx = {}
    # Scale factor: CLI override first, then from schema data source
    if scale_factor_override is not None:
        try:
            ctx["scale_factor"] = int(scale_factor_override)
        except (TypeError, ValueError):
            ctx["scale_factor"] = scale_factor_override
    else:
        sf = get_scale_factor_from_schema(hostname, port, user, schema_name)
        if sf is not None:
            ctx["scale_factor"] = int(sf) if isinstance(sf, float) and sf == int(sf) else sf

    n_workers = get_node_count(hostname, port)
    # Engine only from Docker (container names) or SLURM (nvidia-smi in LOGS). No API fallback.
    engine_from_docker = get_engine_from_docker_containers(hostname, port)
    engine_from_slurm = get_engine_from_slurm() if engine_from_docker is None else None
    engine = engine_from_docker or engine_from_slurm
    if engine_from_docker is not None:
        _debug(f"using engine from Docker containers: {engine_from_docker}")
    elif engine_from_slurm is not None:
        _debug(f"using engine from SLURM (nvidia-smi in logs): {engine_from_slurm}")

    if n_workers is not None:
        ctx["n_workers"] = n_workers
        ctx["kind"] = "single-node" if n_workers == 1 else f"{n_workers}-node"

    if engine == "velox-cpu":
        ctx["gpu_count"] = 0
        ctx["gpu_name"] = "NA"
        ctx["engine"] = "velox-cpu"
    elif engine == "velox-gpu":
        ctx["gpu_count"] = n_workers if n_workers is not None else 0
        gpu_name = get_gpu_name()
        if gpu_name is not None:
            ctx["gpu_name"] = gpu_name
        ctx["engine"] = "velox-gpu"
    elif engine == "java":
        ctx["gpu_count"] = 0
        ctx["engine"] = "java"
    else:
        raise RuntimeError(
            "Could not determine worker engine. Run in Docker (worker images "
            "presto-native-worker-gpu, presto-native-worker-cpu, or presto-java-worker with "
            "tag equal to your username) or on SLURM with LOGS set and nvidia-smi -L in LOGS/worker_0.log."
        )

    worker_image = get_worker_image()
    if worker_image is not None:
        ctx["worker_image"] = worker_image

    return ctx
