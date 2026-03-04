# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""
Metrics collector for Presto queries.

Collects detailed metrics from Presto REST API endpoints after each query
and stores them as a combined JSON file.

Standalone usage:
    # Convert pre-downloaded query JSON files:
    python metrics_collector.py convert /path/to/input_dir /path/to/output_dir

    # Interactively collect from a live Presto coordinator:
    python metrics_collector.py collect http://host:port /path/to/output_dir
"""

import argparse
import json
import math
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .presto_api import fetch_json, fetch_text, get_nodes


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
    nodes = get_nodes(hostname, port)
    if nodes:
        combined["nodes"] = nodes

    # Collect query details and extract task information
    query_info = fetch_json(f"{base_url}/v1/query/{query_id}")
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
            task_data = fetch_json(f"{worker_uri}/v1/task/{task_id}")
            if task_data:
                worker_tasks.append(task_data)

        if worker_tasks:
            all_worker_tasks[worker_id] = worker_tasks

        # Collect worker metrics
        metrics = _fetch_worker_metrics(worker_uri)
        if metrics:
            all_worker_metrics[worker_id] = metrics

    return all_worker_tasks, all_worker_metrics


def _extract_embedded_tasks(query_info: dict) -> dict:
    """Extract task data already embedded in the query's stage tree, grouped by worker.

    Mirrors the structure produced by _collect_worker_data but without HTTP fetches,
    using the task objects present in each stage's latestAttemptExecutionInfo.
    """
    tasks_by_worker: dict[str, list] = {}
    stack = [query_info.get("outputStage")]
    while stack:
        stage_info = stack.pop()
        if stage_info is None:
            continue
        latest_attempt = stage_info.get("latestAttemptExecutionInfo", {})
        for task in latest_attempt.get("tasks", []):
            task_self = task.get("taskStatus", {}).get("self")
            if task_self:
                parsed = urlparse(task_self)
                worker_uri = f"{parsed.scheme}://{parsed.netloc}"
                worker_id = _worker_id_from_uri(worker_uri)
                tasks_by_worker.setdefault(worker_id, []).append(task)
        stack.extend(stage_info.get("subStages", []))
    return tasks_by_worker


def _fetch_worker_metrics(worker_uri: str) -> dict | None:
    """Fetch metrics from a worker node and convert to nested object structure."""
    text = fetch_text(f"{worker_uri}/v1/info/metrics")
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
            key = metric_name[len("presto_cpp_") :]
        elif metric_name.startswith("velox_"):
            prefix = "velox"
            key = metric_name[len("velox_") :]
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


def _build_query_preview(query_info: dict) -> str:
    """Format a BasicQueryInfo dict into a multi-line preview string for the TUI."""
    qid = query_info.get("queryId", "?")
    state = query_info.get("state", "?")
    session = query_info.get("session", {})
    user = session.get("user", "?")
    catalog = session.get("catalog", "")
    schema = session.get("schema", "")
    catalog_schema = f"{catalog}.{schema}" if catalog and schema else catalog or schema or "?"

    stats = query_info.get("queryStats", {})
    elapsed = stats.get("elapsedTime", "?")
    cpu_time = stats.get("totalCpuTime", "?")
    peak_mem = stats.get("peakUserMemoryReservation", "?")
    completed_splits = stats.get("completedSplits", "?")
    total_splits = stats.get("totalSplits", "?")

    sql = query_info.get("query", "")

    error_section = ""
    error_type = query_info.get("errorType")
    if error_type:
        error_code = query_info.get("errorCode", {}).get("name", "?")
        error_section = f"\nError:     {error_type} ({error_code})"

    return (
        f"Query ID:  {qid}\n"
        f"State:     {state}\n"
        f"User:      {user}\n"
        f"Catalog:   {catalog_schema}\n"
        f"Elapsed:   {elapsed}\n"
        f"CPU Time:  {cpu_time}\n"
        f"Peak Mem:  {peak_mem}\n"
        f"Splits:    {completed_splits}/{total_splits}"
        f"{error_section}\n"
        f"\nSQL:\n{sql}"
    )


def _format_menu_entry(query_info: dict) -> str:
    """Format a BasicQueryInfo dict into a compact one-line menu entry.

    The query ID is appended after a | separator so simple-term-menu passes it
    to the preview callback instead of the display string.
    """
    state = query_info.get("state", "?")
    qid = query_info.get("queryId", "?")
    elapsed = query_info.get("queryStats", {}).get("elapsedTime", "?")
    sql = query_info.get("query", "").replace("\n", " ").strip()
    sql_preview = sql[:60] + "..." if len(sql) > 60 else sql

    return f"{state:<10s} {elapsed:>10s}  {qid}  {sql_preview}|{qid}"


def interactive_collect(base_url: str, output_dir: str) -> None:
    """Fetch the query list from a live Presto coordinator and present a TUI picker.

    The user navigates with arrow/j-k keys, sees a preview pane with query
    details, and presses Enter to collect full metrics for the selected query.
    """
    try:
        from simple_term_menu import TerminalMenu
    except ImportError:
        print("Error: simple-term-menu is required for interactive mode.")
        print("Install it with: pip install simple-term-menu")
        return

    print(f"Fetching query list from {base_url}/v1/query ...")
    queries = fetch_json(f"{base_url}/v1/query")
    if not queries:
        print("No queries returned from coordinator.")
        return

    queries.sort(
        key=lambda q: q.get("queryStats", {}).get("createTime", ""),
        reverse=True,
    )

    query_map: dict[str, dict] = {}
    menu_entries: list[str] = []
    for q in queries:
        entry = _format_menu_entry(q)
        menu_entries.append(entry)
        query_map[q.get("queryId", "")] = q

    def preview_callback(entry_with_data: str) -> str:
        qid = entry_with_data
        info = query_map.get(qid, {})
        return _build_query_preview(info) if info else "No details available"

    menu = TerminalMenu(
        menu_entries,
        title="Presto Queries  (/ to search, q to quit)\n",
        preview_command=preview_callback,
        preview_size=0.4,
        preview_title="Query Details",
        show_search_hint=True,
    )

    chosen = menu.show()
    if chosen is None:
        print("Cancelled.")
        return

    selected_entry = menu_entries[chosen]
    selected_qid = selected_entry.rsplit("|", 1)[-1]
    parsed = urlparse(base_url)
    hostname = parsed.hostname or "localhost"
    port = parsed.port or 8080

    print(f"\nCollecting metrics for query {selected_qid} ...")
    collect_metrics(
        query_id=selected_qid,
        query_name=selected_qid,
        hostname=hostname,
        port=port,
        output_dir=output_dir,
    )
    print("Done.")


def convert_local_files(input_dir: str, output_dir: str) -> None:
    """
    Convert pre-downloaded Presto query JSON files into the visualiser format.

    Reads all .json files from input_dir. Files containing a dict with a
    "queryId" key are treated as query data; list-typed files (e.g. node.json)
    are treated as node data. Each query file produces an output combining
    {"nodes": ..., "query": ...} with NaN/Inf sanitization.

    Args:
        input_dir: Directory containing pre-downloaded query JSON files
        output_dir: Directory to write converted metrics files
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    json_files = sorted(input_path.glob("*.json"))
    if not json_files:
        print(f"No .json files found in {input_path}")
        return

    nodes: list | None = None
    query_files: list[tuple[Path, dict]] = []

    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)
        if isinstance(data, dict) and "queryId" in data:
            query_files.append((json_file, data))
        elif isinstance(data, list):
            print(f"Using {json_file.name} as node data")
            nodes = data
        else:
            print(f"Skipping {json_file.name} (unrecognized format)")

    if not query_files:
        print("No query JSON files found (expected dicts with 'queryId' key)")
        return

    for json_file, query_info in query_files:
        print(f"Processing {json_file.name}...")
        combined: dict[str, Any] = {}
        if nodes is not None:
            combined["nodes"] = nodes
        combined["query"] = query_info
        tasks = _extract_embedded_tasks(query_info)
        if tasks:
            combined["tasks"] = tasks
        combined = _sanitize_json_values(combined)

        query_id = query_info.get("queryId", "unknown")
        out_name = f"{json_file.stem}_{query_id}.presto_metrics.json"
        out_file = output_path / out_name
        with open(out_file, "w") as f:
            json.dump(combined, f, indent=2)
        print(f"  -> {out_file}")

    print(f"Converted {len(query_files)} file(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Presto metrics collector and converter.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert_parser = subparsers.add_parser(
        "convert", help="Convert pre-downloaded query JSON files into visualiser format."
    )
    convert_parser.add_argument("input_dir", help="Directory containing query JSON files")
    convert_parser.add_argument("output_dir", help="Directory to write converted metrics files")

    collect_parser = subparsers.add_parser(
        "collect", help="Interactively collect metrics from a live Presto coordinator."
    )
    collect_parser.add_argument("presto_url", help="Presto coordinator URL (e.g. http://host:8080)")
    collect_parser.add_argument("output_dir", help="Directory to write collected metrics files")

    args = parser.parse_args()
    if args.command == "convert":
        convert_local_files(args.input_dir, args.output_dir)
    elif args.command == "collect":
        interactive_collect(args.presto_url, args.output_dir)
