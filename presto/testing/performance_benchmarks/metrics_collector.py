# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Metrics collector for Presto queries.

Collects detailed metrics from Presto REST API endpoints after each query
and stores them as a combined JSON file.
"""

import json
import math
import requests
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def collect_metrics(query_id: str, query_name: str, hostname: str, port: int, output_dir: str) -> None:
    """
    Collect metrics from Presto REST API endpoints for a given query.

    Args:
        query_id: The Presto query ID
        query_name: Name to use in the output filename
        hostname: Presto coordinator hostname
        port: Presto coordinator port
        output_dir: Base directory to store metrics
    """
    base_url = f"http://{hostname}:{port}"
    output_path = Path(output_dir) / "metrics"
    output_path.mkdir(parents=True, exist_ok=True)

    combined = {}

    # Collect node information
    nodes = _fetch_json(f"{base_url}/v1/node")
    if nodes:
        combined["nodes"] = nodes

    # Collect query details and extract task information
    query_info = _fetch_json(f"{base_url}/v1/query/{query_id}")
    if query_info:
        combined["query"] = query_info
        tasks, metrics = _collect_worker_data(query_info)
        if tasks:
            combined["tasks"] = tasks
        if metrics:
            combined["metrics"] = metrics

    # Write combined JSON file
    if combined:
        combined = _sanitize_json_values(combined)
        combined_path = output_path / f"{query_name}_{query_id}.presto_metrics.json"
        with open(combined_path, "w") as f:
            json.dump(combined, f, indent=2)


def _fetch_json(url: str, timeout: int = 30) -> Any | None:
    """Fetch JSON from a URL, returning None on error."""
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Warning: Failed to fetch {url}: {e}")
        return None


def _fetch_text(url: str, timeout: int = 30) -> str | None:
    """Fetch text from a URL, returning None on error."""
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Warning: Failed to fetch {url}: {e}")
        return None


def _collect_worker_data(query_info: dict) -> tuple[dict, dict]:
    """Collect task details and metrics from each worker."""
    # Group tasks by worker
    tasks_by_worker: dict[str, list] = {}
    stack = [query_info.get("outputStage")]
    while stack:
        stage_info = stack.pop()
        if stage_info is None:
            continue
        latest_attempt = stage_info.get("latestAttemptExecutionInfo", {})
        for task in latest_attempt.get("tasks", []):
            task_id = task.get("taskId")
            task_self = task.get("taskStatus", {}).get("self")
            if task_id and task_self:
                parsed = urlparse(task_self)
                worker_uri = f"{parsed.scheme}://{parsed.netloc}"
                tasks_by_worker.setdefault(worker_uri, []).append(task_id)
        stack.extend(stage_info.get("subStages", []))

    all_worker_tasks = {}
    all_worker_metrics = {}

    for worker_uri, task_ids in tasks_by_worker.items():
        worker_id = _worker_id_from_uri(worker_uri)

        # Collect detailed task info from worker
        worker_tasks = []
        for task_id in task_ids:
            task_data = _fetch_json(f"{worker_uri}/v1/task/{task_id}")
            if task_data:
                worker_tasks.append(task_data)

        if worker_tasks:
            all_worker_tasks[worker_id] = worker_tasks

        # Collect worker metrics
        metrics = _fetch_worker_metrics(worker_uri)
        if metrics:
            all_worker_metrics[worker_id] = metrics

    return all_worker_tasks, all_worker_metrics


def _fetch_worker_metrics(worker_uri: str) -> dict | None:
    """Fetch metrics from a worker node and convert to nested object structure."""
    text = _fetch_text(f"{worker_uri}/v1/info/metrics")
    if text is None:
        return None
    metrics = _parse_prometheus_metrics(text)
    return _metrics_to_nested_object(metrics)


def _metrics_to_nested_object(metrics: list) -> dict:
    """Convert a list of (metric_name, value) tuples to a nested object structure.

    Metric names use underscores as separators. The first component (e.g., 'presto_cpp', 'velox')
    becomes the top-level key, and the remainder becomes the nested key.

    Example:
        presto_cpp_num_http_request -> {"presto_cpp": {"num_http_request": 0}}
        velox_driver_yield_count -> {"velox": {"driver_yield_count": 2}}
    """
    result: dict[str, dict] = {}

    for metric_name, value in metrics:
        # Determine the prefix (presto_cpp or velox or other)
        if metric_name.startswith("presto_cpp_"):
            prefix = "presto_cpp"
            key = metric_name[len("presto_cpp_"):]
        elif metric_name.startswith("velox_"):
            prefix = "velox"
            key = metric_name[len("velox_"):]
        else:
            # For other metrics, use the first underscore-separated component
            parts = metric_name.split("_", 1)
            if len(parts) == 2:
                prefix, key = parts
            else:
                prefix = metric_name
                key = "value"

        result.setdefault(prefix, {})[key] = value

    return result


def _parse_prometheus_metrics(text: str) -> list[tuple[str, float | str]]:
    """Parse Prometheus text format into a list of (metric_name, value) tuples."""
    metrics = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            # Strip labels from metric name (e.g., "metric{label=value}" -> "metric")
            metric_name = parts[0].split("{")[0]
            try:
                value: float | str = float(parts[1])
            except ValueError:
                value = parts[1]
            metrics.append((metric_name, value))
    return metrics


def _worker_id_from_uri(uri: str) -> str:
    """Extract a filesystem-safe worker ID from a URI."""
    return urlparse(uri).netloc.replace(":", "_").replace(".", "_")


def _sanitize_json_values(obj: Any) -> Any:
    """Recursively replace NaN/Inf float values with None for JSON compatibility."""
    if isinstance(obj, dict):
        return {k: _sanitize_json_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_json_values(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj
