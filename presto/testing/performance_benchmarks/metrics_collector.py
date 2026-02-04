# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
Metrics collector for Presto queries.

Collects detailed metrics from Presto REST API endpoints after each query
and stores them as JSON files.
"""

import json
import requests
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
import math
import re
from pathlib import Path

def collect_metrics(query_id: str, query_name: str, hostname: str, port: int, output_dir: str) -> None:
    """
    Collect metrics from Presto REST API endpoints for a given query.

    Args:
        query_id: The Presto query ID
        hostname: Presto coordinator hostname
        port: Presto coordinator port
        output_dir: Base directory to store metrics
    """
    base_url = f"http://{hostname}:{port}"
    output_path = Path(output_dir) / "metrics" / query_id
    output_path.mkdir(parents=True, exist_ok=True)

    # Collect node information
    _fetch_and_save(f"{base_url}/v1/node", output_path / "nodes.json")

    # Collect query details and extract stage/task information
    query_info = _fetch_json(f"{base_url}/v1/query/{query_id}")
    if query_info:
        _save_json(query_info, output_path / "query.json")
        _collect_stages(query_info, output_path)
        _collect_worker_data(query_info, output_path)
        _combined_json(output_path, query_name)


def _fetch_json(url: str, timeout: int = 30) -> dict | None:
    """Fetch JSON from a URL, returning None on error."""
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Warning: Failed to fetch {url}: {e}")
        return None


def _fetch_and_save(url: str, output_path: Path) -> None:
    """Fetch JSON from URL and save to file."""
    data = _fetch_json(url)
    if data:
        _save_json(data, output_path)


def _save_json(data, output_path: Path) -> None:
    """Save data as JSON."""
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)


def _collect_stages(query_info: dict, output_path: Path) -> None:
    """Extract and save stage information from query info."""
    stages = []
    stack = [query_info.get("outputStage")]
    while stack:
        stage_info = stack.pop()
        if stage_info is None:
            continue
        latest_attempt = stage_info.get("latestAttemptExecutionInfo", {})
        stages.append({
            "stageId": stage_info.get("stageId"),
            "state": latest_attempt.get("state"),
            "stats": latest_attempt.get("stats"),
        })
        stack.extend(stage_info.get("subStages", []))

    if stages:
        _save_json(stages, output_path / "stages.json")


def _collect_worker_data(query_info: dict, output_path: Path) -> None:
    """Collect task details and metrics from each worker."""
    # Group tasks by worker
    tasks_by_worker = defaultdict(list)
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
                tasks_by_worker[worker_uri].append(task_id)
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

    if all_worker_tasks:
        _save_json(all_worker_tasks, output_path / "tasks.json")
    if all_worker_metrics:
        _save_json(all_worker_metrics, output_path / "metrics.json")


def _fetch_worker_metrics(worker_uri: str) -> list | None:
    """Fetch metrics from a worker node, supporting JSON or Prometheus text format."""
    try:
        response = requests.get(f"{worker_uri}/v1/info/metrics", timeout=30)
        response.raise_for_status()
        try:
            data = response.json()
            if isinstance(data, (dict, list)):
                return data
        except (json.JSONDecodeError, ValueError):
            pass
        return _parse_prometheus_metrics(response.text)
    except Exception as e:
        print(f"Warning: Failed to fetch {worker_uri}/v1/info/metrics: {e}")
        return None


def _parse_prometheus_metrics(text: str) -> list:
    """Parse Prometheus text format into a list of metric objects."""
    metrics = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            try:
                value = float(parts[1])
            except ValueError:
                value = parts[1]
            metrics.append({"metric": parts[0], "value": value})
    return metrics or [{"raw": text}]


def _worker_id_from_uri(uri: str) -> str:
    """Extract a filesystem-safe worker ID from a URI."""
    return urlparse(uri).netloc.replace(":", "_").replace(".", "_")


def _parse_metric_name(metric_with_labels: str) -> str:
    """Extract base metric name, stripping Prometheus-style labels like {cluster="x"}."""
    match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)', metric_with_labels)
    return match.group(1) if match else metric_with_labels


def _load_worker_metrics(metrics_path: Path) -> dict:
    """Load metrics.json and convert to flat dict keyed by metric name."""
    with open(metrics_path) as f:
        data = json.load(f)

    # metrics.json has {"worker_ip": [{metric: "name{labels}", value: float}, ...]}
    # We need to flatten to {"metric_name": value, ...}
    result = {}
    for worker_key, metric_list in data.items():
        for item in metric_list:
            if 'metric' not in item:
                continue
            metric_name = _parse_metric_name(item['metric'])
            value = item['value']
            # Replace NaN/Inf with null
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                value = None
            # Convert float to int if it's a whole number
            elif isinstance(value, float) and value.is_integer():
                value = int(value)
            result[metric_name] = value
    return result


def _load_query_info(query_path: Path) -> dict:
    """Load query.json as-is."""
    with open(query_path) as f:
        return json.load(f)


def _load_tasks_info(tasks_path: Path) -> list:
    """Load tasks.json and extract the tasks list."""
    with open(tasks_path) as f:
        data = json.load(f)

    # tasks.json has {"worker_ip": [task_objects]}
    # Return the first (or combined) list of tasks
    tasks = []
    for worker_key, task_list in data.items():
        tasks.extend(task_list)
    return tasks


def _combined_json(output_path: Path, query_name: str) -> dict:
    """Combine the three JSON files into one structure."""
    metrics_path = output_path / "metrics.json"
    query_path = output_path / "query.json"
    tasks_path = output_path / "tasks.json"

    combined = {}

    if metrics_path.exists():
        combined["worker_metrics"] = _load_worker_metrics(metrics_path)
    if query_path.exists():
        combined["query_info"] = _load_query_info(query_path)
    if tasks_path.exists():
        combined["tasks_info"] = _load_tasks_info(tasks_path)
    # Write combined JSON if any component is present

    combined_path = output_path.parent / (query_name + "_" + output_path.name + ".presto_metrics.json")
    if combined:
        with open(combined_path, "w") as f:
            json.dump(combined, f, indent=2, allow_nan=False)
    return combined
