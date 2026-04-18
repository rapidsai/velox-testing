#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""
CLI for posting Velox benchmark results to the API.

This script operates on the parsed output of the benchmark runner. The
expected directory structure is:

    ../benchmark-root/
    ├── benchmark.json           # optional
    ├── benchmark_result.json
    ├── configs                  # optional
    │   ├── coordinator.config
    │   └── worker.config
    └── logs                     # optional
        └── slurm-4575179.out

Usage:
    python benchmark_reporting_tools/post_results.py /path/to/benchmark/dir \
        --sku-name PDX-H100 \
        --storage-configuration-name pdx-lustre-sf-100 \
        --cache-state warm

    # With optional version info
    python benchmark_reporting_tools/post_results.py /path/to/benchmark/dir \
        --sku-name PDX-H100 \
        --storage-configuration-name pdx-lustre-sf-100 \
        --cache-state warm \
        --identifier-hash abc123 \
        --version 1.0.0 \
        --commit-hash def456

Environment variables:
    BENCHMARK_API_URL: API URL
    BENCHMARK_API_KEY: API key for authentication

"""

import argparse
import asyncio
import dataclasses
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from typing import Any
import httpx

LARGE_ASSET_DIRECT_UPLOAD_THRESHOLD_BYTES = 10 * 1024 * 1024

@dataclasses.dataclass(kw_only=True)
class BenchmarkMetadata:
    kind: str
    benchmark: str
    timestamp: datetime
    execution_number: int
    n_workers: int
    node_count: int | None = None
    scale_factor: int
    gpu_count: int
    num_drivers: int
    worker_image: str | None = None
    image_digest: str | None = None
    gpu_name: str
    engine: str

    @classmethod
    def from_file(cls, file_path: Path) -> "BenchmarkMetadata":
        data = json.loads(file_path.read_text())

        # parse fields, like the timestamp
        data["timestamp"] = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

        return cls(**data)

    @classmethod
    def from_results_context(cls, context: dict) -> "BenchmarkMetadata":
        """Construct from the context dict embedded in benchmark_result.json."""
        timestamp_str = context["timestamp"]
        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        engine = context["engine"]
        is_cpu = "cpu" in engine
        return cls(
            kind=context["kind"],
            benchmark=context.get("benchmark", "tpch"),
            timestamp=timestamp,
            execution_number=context.get("execution_number", 1),
            n_workers=int(context["n_workers"]),
            node_count=int(context["node_count"]) if "node_count" in context else None,
            scale_factor=int(context["scale_factor"]),
            gpu_count=0 if is_cpu else int(context["gpu_count"]),
            gpu_name="N/A" if is_cpu else context["gpu_name"],
            num_drivers=int(context["num_drivers"]),
            worker_image=context.get("worker_image"),
            image_digest=context.get("image_digest"),
            engine=engine,
        )

    def serialize(self) -> dict:
        out = dataclasses.asdict(self)
        out["timestamp"] = out["timestamp"].isoformat()
        return out


@dataclasses.dataclass
class BenchmarkResults:
    benchmark_type: str
    raw_times_ms: dict[str, list[float | None]]
    failed_queries: dict[str, str]

    @classmethod
    def from_file(cls, file_path: Path, benchmark_name: str) -> "BenchmarkResults":
        data = json.loads(file_path.read_text())

        if benchmark_name not in data.keys():
            raise KeyError(f"Expected '{benchmark_name}' key in {file_path}, got: {sorted(data.keys())}")

        raw_times_ms = data[benchmark_name]["raw_times_ms"]
        failed_queries = data[benchmark_name]["failed_queries"]

        return cls(
            benchmark_type=benchmark_name,
            raw_times_ms=raw_times_ms,
            failed_queries=failed_queries,
        )


def parse_config_file(file_path: Path) -> dict[str, str]:
    """Parse a key=value config file, ignoring comments and blank lines.

    Args:
        file_path: Path to the config file

    Returns:
        Dictionary of configuration key-value pairs
    """
    config = {}
    for line in file_path.read_text().splitlines():
        line = line.strip()
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue
        # Parse key=value
        if "=" in line:
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config


@dataclasses.dataclass
class EngineConfig:
    coordinator: dict[str, str]
    worker: dict[str, str]

    @classmethod
    def from_dir(cls, configs_dir: Path) -> "EngineConfig":
        """Load engine configuration from a configs directory.

        Expects coordinator.config and worker.config files.
        """
        coordinator_config = parse_config_file(configs_dir / "coordinator.config")
        worker_config = parse_config_file(configs_dir / "worker.config")
        return cls(coordinator=coordinator_config, worker=worker_config)

    def serialize(self) -> dict:
        return dataclasses.asdict(self)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Post Velox benchmark results to the API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to benchmark directory containing benchmark.json and result_dir/",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("BENCHMARK_API_URL"),
        help="API URL (default: from BENCHMARK_API_URL env var)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BENCHMARK_API_KEY"),
        help="API key for authentication (default: from BENCHMARK_API_KEY env var)",
    )
    parser.add_argument(
        "--sku-name",
        required=True,
        help="Compute hardware SKU name (e.g., 'PDX-H100')",
    )
    parser.add_argument(
        "--storage-configuration-name",
        required=True,
        help="Storage configuration name",
    )
    parser.add_argument(
        "--cache-state",
        choices=["cold", "warm", "hot", "lukewarm"],
        help="Cache state for the benchmark run",
        required=True,
    )
    parser.add_argument(
        "--engine-name",
        default=None,
        help="Query engine name (optionally derived from benchmark.json 'engine' field)",
    )
    parser.add_argument(
        "--identifier-hash",
        help="Unique identifier hash for software environment (e.g. a container image digest). "
             "If omitted, the image_digest from benchmark_result.json context is used.",
        default=None,
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version string for the query engine (e.g. velox-cudf's version).",
    )
    parser.add_argument(
        "--commit-hash",
        default=None,
        help="Git commit hash for the query engine",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the submission data without posting to API",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--is-official",
        action="store_true",
        help="Mark the benchmark run as official",
    )
    parser.add_argument(
        "--upload-logs",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Upload *.log files from the benchmark directory as assets (default: True). Use --no-upload-logs to skip.",
    )
    parser.add_argument(
        "--benchmark-name",
        help="Benchmark definition name",
        required=True,
    )
    parser.add_argument(
        "--velox-branch",
        default=None,
        help="Velox branch used to build the worker image",
    )
    parser.add_argument(
        "--velox-repo",
        default=None,
        help="Velox repository used to build the worker image",
    )
    parser.add_argument(
        "--presto-branch",
        default=None,
        help="Presto branch used to build the worker image",
    )
    parser.add_argument(
        "--presto-repo",
        default=None,
        help="Presto repository used to build the worker image",
    )
    parser.add_argument(
        "--concurrency-streams",
        help="Number of concurrency streams to use for the benchmark run",
        type=int,
        default=1,
    )

    # A bunch of optional arguments for when benchmark.json is not present.
    parser.add_argument(
        "--kind",
        help="Run kind (e.g. 'single-node', 'multi-node')",
    )
    parser.add_argument(
        "--benchmark",
        help="Benchmark name (e.g. 'tpch')",
        default="tpch",
    )
    parser.add_argument(
        "--timestamp",
        help="Timestamp of the benchmark run",
        default=None,
    )
    parser.add_argument("--execution-number", help="Execution number of the benchmark run", type=int, default=1)
    parser.add_argument(
        "--n-workers",
        help="Number of GPU workers in the benchmark run",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--node-count",
        help="Number of cluster nodes in the benchmark run",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--scale-factor",
        help="Scale factor of the benchmark run",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--gpu-count",
        help="Number of GPUs in the benchmark run",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--gpu-name",
        help="GPU name (e.g. 'H100')",
        default=None,
    )
    parser.add_argument(
        "--num-drivers",
        help="Number of drivers in the benchmark run",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--worker-image",
        help="Worker image (e.g. 'velox/worker:latest')",
        default=None,
    )

    return parser.parse_args()


def normalize_api_url(url: str) -> str:
    """Normalize a user-provided API URL to a base URL.

    Handles various formats:
    - https://example.nvidia.com
    - https://example.nvidia.com/
    - https://example.nvidia.com/api/benchmark
    - https://example.nvidia.com/api/benchmark/

    Returns a normalized base URL (scheme + netloc) without trailing slash.
    """
    parsed = urlparse(url)
    # Reconstruct URL with only scheme and netloc (removes path, query, fragment)
    normalized = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    # Remove trailing slash if present
    return normalized.rstrip("/")


def build_submission_payload(
    benchmark_metadata: BenchmarkMetadata,
    benchmark_results: BenchmarkResults,
    engine_config: EngineConfig | None,
    sku_name: str,
    storage_configuration_name: str,
    benchmark_definition_name: str,
    cache_state: str,
    engine_name: str | None,
    identifier_hash: str,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    asset_ids: list[int] | None = None,
    concurrency_streams: int = 1,
    velox_branch: str | None = None,
    velox_repo: str | None = None,
    presto_branch: str | None = None,
    presto_repo: str | None = None,
    validation_results: dict | None = None,
) -> dict:
    """Build a BenchmarkSubmission payload from parsed dataclasses.

    Args:
        benchmark_metadata: Parsed benchmark.json as BenchmarkMetadata
        benchmark_results: Parsed benchmark_result.json as BenchmarkResults
        engine_config: Parsed config files as EngineConfig, optional
        sku_name: Hardware SKU name
        storage_configuration_name: Storage configuration name
        cache_state: Cache state (cold/warm/hot)
        engine_name: Override for engine name (or None to use metadata)
        identifier_hash: Explicit identifier hash
        version: Explicit version (or None for placeholder)
        commit_hash: Explicit commit hash (or None for placeholder)
        is_official: Whether this is an official benchmark run
    """
    # Use engine from metadata if not overridden
    engine = engine_name or benchmark_metadata.engine

    # Use placeholders for version info if not provided
    if version is None:
        version = "unknown"
    if commit_hash is None:
        commit_hash = "unknown"

    # Build query logs from results
    query_logs = []
    execution_order = 0

    raw_times = benchmark_results.raw_times_ms
    failed_queries = benchmark_results.failed_queries

    # Sort query names for consistent ordering (Q1, Q2, ..., Q22)
    query_names = sorted(raw_times.keys(), key=lambda x: int(x[1:]))

    per_query_validation = (validation_results or {}).get("queries", {})

    for query_name in query_names:
        times = raw_times[query_name]
        if times is None:
            times = []
        is_failed = query_name in failed_queries

        # Look up validation result for this query (keys are lowercase e.g. "q1")
        vkey = "q" + query_name.lstrip("Q").lower()
        vdata = per_query_validation.get(vkey)
        validation_result = (
            {
                "status": "expected-failure" if vdata["status"] == "xfail" else vdata["status"],
                "message": vdata.get("message"),
            }
            if vdata
            else {"status": "not-validated"}
        )

        # Each execution becomes a separate query log entry
        for exec_idx, runtime_ms in enumerate(times):
            if is_failed:
                runtime_ms = None
            else:
                assert runtime_ms is not None, "Expected runtime_ms to be not None for non-failed queries"
                runtime_ms = float(runtime_ms)
            query_logs.append(
                {
                    "query_name": query_name.lstrip("Q"),
                    "execution_order": execution_order,
                    "runtime_ms": runtime_ms,
                    "status": "error" if is_failed else "success",
                    "extra_info": {
                        "execution_number": exec_idx + 1,
                    },
                    "validation_result": validation_result,
                }
            )
            execution_order += 1

    # Handle failed queries that may not have times
    for query_name, error_info in failed_queries.items():
        if query_name not in raw_times:
            query_logs.append(
                {
                    "query_name": query_name.lstrip("Q"),
                    "execution_order": execution_order,
                    "runtime_ms": None,
                    "status": "error",
                    "extra_info": {
                        "error": str(error_info),
                    },
                }
            )
            execution_order += 1

    # Build extra info from metadata
    extra_info = {
        "kind": benchmark_metadata.kind,
        "gpu_count": benchmark_metadata.gpu_count,
        "gpu_name": benchmark_metadata.gpu_name,
        "num_drivers": benchmark_metadata.num_drivers,
        "worker_image": benchmark_metadata.worker_image,
        "execution_number": benchmark_metadata.execution_number,
    }

    return {
        "sku_name": sku_name,
        "storage_configuration_name": storage_configuration_name,
        "benchmark_definition_name": benchmark_definition_name,
        "cache_state": cache_state,
        "query_engine": {
            "engine_name": engine,
            "identifier_hash": identifier_hash,
            "version": version,
            "commit_hash": commit_hash,
        },
        "run_at": benchmark_metadata.timestamp.isoformat(),
        "node_count": benchmark_metadata.node_count,
        "gpu_count": benchmark_metadata.gpu_count,
        "query_logs": query_logs,
        "concurrency_streams": concurrency_streams,
        "engine_config": {
            **(engine_config.serialize() if engine_config else {}),
            "velox_branch": velox_branch,
            "velox_repo": velox_repo,
            "presto_branch": presto_branch,
            "presto_repo": presto_repo,
        },
        "extra_info": extra_info,
        "is_official": is_official,
        "asset_ids": asset_ids,
        "validation_status": "expected-failure" if (validation_results or {}).get("overall_status") == "xfail" else (validation_results or {}).get("overall_status", "not-validated"),
    }


def build_http_client(api_url: str, api_key: str, timeout: float) -> httpx.AsyncClient:
    base_url = normalize_api_url(api_url)
    transport = httpx.AsyncHTTPTransport(retries=3)
    return httpx.AsyncClient(
        base_url=base_url,
        transport=transport,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
    )

async def _s3_presigned_put(
    upload_url: str,
    required_headers: dict[str, Any],
    content: bytes,
    timeout: float,
) -> tuple[int, str]:
    headers = {str(k): str(v) for k, v in required_headers.items()}
    async with httpx.AsyncClient(timeout=timeout) as s3_client:
        response = await s3_client.put(upload_url, headers=headers, content=content)
    return response.status_code, response.text


async def _upload_asset_presigned(
    client: httpx.AsyncClient,
    content: bytes,
    filename: str,
    title: str,
    media_type: str,
    timeout: float,
) -> int:
    url_resp = await client.post(
        "/api/assets/upload-url/",
        json={"original_filename": filename, "media_type": media_type},
    )
    if url_resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to get upload URL: {url_resp.status_code} {url_resp.text}")

    presign = url_resp.json()
    upload_url = presign["upload_url"]
    s3_key = presign["s3_key"]
    required_headers = presign.get("required_headers") or {}

    put_status, put_body = await _s3_presigned_put(upload_url, required_headers, content, timeout)
    if put_status not in (200, 204):
        raise RuntimeError(f"S3 PUT failed: {put_status} {put_body}")

    complete = await client.post(
        "/api/assets/complete-upload/",
        json={"s3_key": s3_key, "title": title, "media_type": media_type},
    )
    if complete.status_code != 201:
        raise RuntimeError(f"Complete upload failed: {complete.status_code} {complete.text}")
    return complete.json()["asset_id"]


async def upload_log_files(
    benchmark_dir: Path,
    api_url: str,
    api_key: str,
    timeout: float,
    max_concurrency: int = 5,
) -> list[int]:
    """Upload all *.log files from benchmark_dir as assets, in parallel.

    Args:
        benchmark_dir: Directory to glob for *.log files
        api_url: Base API URL
        api_key: API bearer token
        timeout: Request timeout in seconds
        max_concurrency: Maximum number of concurrent uploads

    Returns:
        List of asset IDs from the uploaded files
    """
    log_files = sorted(benchmark_dir.glob("*.log"))
    log_files.extend(sorted(benchmark_dir.glob("*.nsys-rep")))
    metrics_dir = benchmark_dir / "metrics"
    if metrics_dir.is_dir():
        log_files.extend(sorted(metrics_dir.glob("*.json")))
    if not log_files:
        return []

    print(f"  Uploading {len(log_files)} log file(s) (max {max_concurrency} concurrent)...", file=sys.stderr)
    semaphore = asyncio.Semaphore(max_concurrency)

    async with build_http_client(api_url, api_key, timeout) as client:

        async def _upload_one(log_file: Path) -> int:
            async with semaphore:
                print(f"    Uploading {log_file.name}...", file=sys.stderr)
                content = log_file.read_bytes()
                if log_file.suffix == ".json":
                    media_type = "application/json"
                elif log_file.suffix == ".nsys-rep":
                    media_type = "application/octet-stream"
                else:
                    media_type = "text/plain"

                if len(content) > LARGE_ASSET_DIRECT_UPLOAD_THRESHOLD_BYTES:
                    print(f"    Using presigned upload for {log_file.name} ({len(content) // (1024 * 1024)} MiB)...", file=sys.stderr)
                    asset_id = await _upload_asset_presigned(client, content, log_file.name, log_file.name, media_type, timeout)
                else:
                    response = await client.post(
                        "/api/assets/upload/",
                        files={"file": (log_file.name, content, media_type)},
                        data={"title": log_file.name, "media_type": media_type},
                    )
                    if response.status_code >= 400:
                        raise RuntimeError(f"Failed to upload {log_file.name}: {response.status_code} {response.text}")
                    asset_id = response.json()["asset_id"]

                print(f"    Uploaded {log_file.name} (asset_id={asset_id})", file=sys.stderr)
                return asset_id

        asset_ids = await asyncio.gather(*[_upload_one(f) for f in log_files])

    return list(asset_ids)


async def post_submission(api_url: str, api_key: str, payload: dict, timeout: float) -> tuple[int, str]:
    """Post a benchmark submission to the API.

    Returns:
        Tuple of (status_code, response_text)
    """
    async with build_http_client(api_url, api_key, timeout) as client:
        response = await client.post("/api/benchmark/", json=payload)
    return response.status_code, response.text


async def process_benchmark_dir(
    benchmark_dir: Path,
    *,
    sku_name: str,
    storage_configuration_name: str,
    cache_state: str,
    engine_name: str | None,
    identifier_hash: str,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    dry_run: bool,
    api_url: str,
    api_key: str,
    timeout: float,
    upload_logs: bool = True,
    benchmark_definition_name: str,
    # all the optional arguments for when benchmark.json is not present.
    concurrency_streams: int = 1,
    velox_branch: str | None = None,
    velox_repo: str | None = None,
    presto_branch: str | None = None,
    presto_repo: str | None = None,
    kind: str | None = None,
    benchmark: str | None = None,
    timestamp: str | None = None,
    execution_number: int = 1,
    n_workers: int | None = None,
    node_count: int | None = None,
    scale_factor: int | None = None,
    gpu_count: int | None = None,
    gpu_name: str | None = None,
    worker_image: str | None = None,
    num_drivers: int | None = None,
) -> int:
    """Process a benchmark directory and post results to API.

    Returns:
        0 on success, 1 on failure
    """
    print(f"\nProcessing: {benchmark_dir}", file=sys.stderr)

    # Load results file — also used as the primary metadata source via its context.
    result_file = benchmark_dir / "benchmark_result.json"
    try:
        result_data = json.loads(result_file.read_text())
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"  Error loading results: {e}", file=sys.stderr)
        return 1

    context = result_data.get("context", {})

    # Determine metadata source: context > benchmark.json > CLI args.
    benchmark_json_path = benchmark_dir / "benchmark.json"

    # Resolve identifier_hash: CLI arg > context image_digest > "unknown"
    resolved_identifier_hash = identifier_hash or context.get("image_digest") or "unknown"
    if resolved_identifier_hash == "unknown":
        print("  Warning: image_digest not found in benchmark_result.json context and --identifier-hash not provided; using 'unknown'", file=sys.stderr)

    if "kind" in context:
        print("  Loading metadata from benchmark_result.json context...", file=sys.stderr)
        try:
            benchmark_metadata = BenchmarkMetadata.from_results_context(context)
        except (KeyError, ValueError) as e:
            print(f"  Error loading metadata from results context: {e}", file=sys.stderr)
            return 1
    elif benchmark_json_path.exists():
        print("  Loading metadata from benchmark.json...", file=sys.stderr)
        try:
            benchmark_metadata = BenchmarkMetadata.from_file(benchmark_json_path)
        except (ValueError, json.JSONDecodeError, FileNotFoundError) as e:
            print(f"  Error loading metadata: {e}", file=sys.stderr)
            return 1
    else:
        missing_args = []
        if kind is None:
            missing_args.append("kind")
        if benchmark is None:
            missing_args.append("benchmark")
        if timestamp is None:
            missing_args.append("timestamp")
        if n_workers is None:
            missing_args.append("n_workers")
        if node_count is None:
            missing_args.append("node_count")
        if scale_factor is None:
            missing_args.append("scale_factor")
        if gpu_count is None:
            missing_args.append("gpu_count")
        if gpu_name is None:
            missing_args.append("gpu_name")
        if num_drivers is None:
            missing_args.append("num_drivers")
        if engine_name is None:
            missing_args.append("engine_name")

        if missing_args:
            print("  Error: must provide benchmark metadata when benchmark.json is not present", file=sys.stderr)
            print(f"  Error: missing arguments: {', '.join(missing_args)}", file=sys.stderr)
            return 1

        # mypy doesn't realize that kind, benchmark, etc. have been narrowed to not-None by the check above.
        benchmark_metadata = BenchmarkMetadata(
            kind=kind,  # type: ignore[arg-type]
            benchmark=benchmark,  # type: ignore[arg-type]
            timestamp=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),  # type: ignore[union-attr]
            execution_number=execution_number,
            n_workers=n_workers,  # type: ignore[arg-type]
            node_count=node_count,  # type: ignore[arg-type]
            scale_factor=scale_factor,  # type: ignore[arg-type]
            gpu_count=gpu_count,  # type: ignore[arg-type]
            gpu_name=gpu_name,  # type: ignore[arg-type]
            num_drivers=num_drivers,  # type: ignore[arg-type]
            worker_image=worker_image,
            engine=engine_name,  # type: ignore[arg-type]
        )

    try:
        results = BenchmarkResults.from_file(
            benchmark_dir / "benchmark_result.json", benchmark_name=benchmark_metadata.benchmark
        )
    except (ValueError, json.JSONDecodeError, FileNotFoundError) as e:
        print(f"  Error loading results: {e}", file=sys.stderr)
        return 1

    validation_results_path = benchmark_dir / "validation_results.json"
    if validation_results_path.exists():
        print("  Loading validation results...", file=sys.stderr)
        try:
            validation_results = json.loads(validation_results_path.read_text())
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"  Warning: could not load validation results: {e}", file=sys.stderr)
            validation_results = None
    else:
        print("  No validation results found.", file=sys.stderr)
        validation_results = None

    if (benchmark_dir / "configs").exists():
        print("  Loading engine config...", file=sys.stderr)
        engine_config = EngineConfig.from_dir(benchmark_dir / "configs")
    else:
        print("  No engine config found.", file=sys.stderr)
        engine_config = None

    # Upload log files as assets
    asset_ids = None
    if upload_logs:
        if dry_run:
            log_files = sorted(benchmark_dir.glob("*.log"))
            log_files.extend(sorted(benchmark_dir.glob("*.nsys-rep")))
            metrics_dir = benchmark_dir / "metrics"
            if metrics_dir.is_dir():
                log_files.extend(sorted(metrics_dir.glob("*.json")))
            print(
                f"  [DRY RUN] Would upload {len(log_files)} log file(s): {[f.name for f in log_files]}", file=sys.stderr
            )
        else:
            try:
                asset_ids = await upload_log_files(benchmark_dir, api_url, api_key, timeout)
            except (RuntimeError, httpx.RequestError) as e:
                print(f"  Error uploading logs: {e}", file=sys.stderr)
                return 1

    # Build submission payload
    try:
        payload = build_submission_payload(
            benchmark_metadata=benchmark_metadata,
            benchmark_results=results,
            engine_config=engine_config,
            benchmark_definition_name=benchmark_definition_name,
            sku_name=sku_name,
            storage_configuration_name=storage_configuration_name,
            cache_state=cache_state,
            engine_name=engine_name,
            identifier_hash=resolved_identifier_hash,
            version=version,
            commit_hash=commit_hash,
            is_official=is_official,
            asset_ids=asset_ids,
            concurrency_streams=concurrency_streams,
            velox_branch=velox_branch,
            velox_repo=velox_repo,
            presto_branch=presto_branch,
            presto_repo=presto_repo,
            validation_results=validation_results,
        )
    except Exception as e:
        print(f"  Error building payload: {e}", file=sys.stderr)
        return 1

    # Print summary
    print(f"  Benchmark definition: {payload['benchmark_definition_name']}", file=sys.stderr)
    print(f"  Engine: {payload['query_engine']['engine_name']}", file=sys.stderr)
    print(f"  Identifier hash: {payload['query_engine']['identifier_hash']}", file=sys.stderr)
    print(f"  Node count: {payload['node_count']}", file=sys.stderr)
    print(f"  Query logs: {len(payload['query_logs'])}", file=sys.stderr)
    print(f"  Validation status: {payload['validation_status']}", file=sys.stderr)
    xfail_queries = [
        ql["query_name"]
        for ql in payload["query_logs"]
        if ql.get("validation_result", {}).get("status") == "xfail"
    ]
    if xfail_queries:
        unique_xfail = sorted(set(xfail_queries), key=lambda x: int(x))
        print(f"  XFailed queries: {unique_xfail}", file=sys.stderr)

    if dry_run:
        print("\n  [DRY RUN] Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    # Post to API
    try:
        status_code, response_text = await post_submission(api_url, api_key, payload, timeout)
        print(f"  Status: {status_code}", file=sys.stderr)
        if status_code >= 400:
            print(f"  Response: {response_text}", file=sys.stderr)
            return 1
        else:
            print(f"  Success: {response_text}", file=sys.stderr)
            return 0
    except httpx.RequestError as e:
        print(f"  Error posting: {e}", file=sys.stderr)
        return 1


async def main() -> int:
    args = parse_args()

    # Resolve to str (parser already falls back to BENCHMARK_API_URL / BENCHMARK_API_KEY)
    api_url = args.api_url or ""
    api_key = args.api_key or ""

    if not args.dry_run:
        if not api_url:
            print(
                "Error: --api-url or BENCHMARK_API_URL environment variable required",
                file=sys.stderr,
            )
            return 1
        if not api_key:
            print(
                "Error: --api-key or BENCHMARK_API_KEY environment variable required",
                file=sys.stderr,
            )
            return 1

    # Validate input path
    benchmark_dir = Path(args.input_path)
    if not benchmark_dir.is_dir():
        print(f"Error: Input path is not a directory: {args.input_path}", file=sys.stderr)
        return 1

    result = await process_benchmark_dir(
        benchmark_dir,
        sku_name=args.sku_name,
        storage_configuration_name=args.storage_configuration_name,
        cache_state=args.cache_state,
        engine_name=args.engine_name,
        identifier_hash=args.identifier_hash,
        version=args.version,
        commit_hash=args.commit_hash,
        is_official=args.is_official,
        dry_run=args.dry_run,
        api_url=api_url,
        api_key=api_key,
        timeout=args.timeout,
        upload_logs=args.upload_logs,
        benchmark_definition_name=args.benchmark_name,
        velox_branch=args.velox_branch,
        velox_repo=args.velox_repo,
        presto_branch=args.presto_branch,
        presto_repo=args.presto_repo,
        kind=args.kind,
        benchmark=args.benchmark,
        timestamp=args.timestamp,
        execution_number=args.execution_number,
        n_workers=args.n_workers,
        node_count=args.node_count,
        scale_factor=args.scale_factor,
        gpu_count=args.gpu_count,
        gpu_name=args.gpu_name,
        worker_image=args.worker_image,
        num_drivers=args.num_drivers,
        concurrency_streams=args.concurrency_streams,
    )

    return result


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
