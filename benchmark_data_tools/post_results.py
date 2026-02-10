#!/usr/bin/env python3
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

    ../velox-testing-benchmark-viz/2026/01/28/01/
    ├── benchmark.json
    ├── configs
    │   ├── coordinator.config
    │   └── worker.config
    ├── logs
    │   └── slurm-4575179.out
    └── result_dir
        ├── benchmark_cold.json
        ├── benchmark_full.json
        ├── benchmark_result.json
        ├── benchmark_warm.json
        └── summary.csv

Usage:
    python benchmark_data_tools/post_results.py /path/to/benchmark/dir \
        --sku-name PDX-H100 \
        --storage-configuration-name pdx-lustre-sf-100 \
        --cache-state warm

    # With optional version info
    python scripts/post_results.py /path/to/benchmark/dir \
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
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
import dataclasses

import httpx


@dataclasses.dataclass
class BenchmarkMetadata:
    kind: str
    benchmark: str
    timestamp: datetime
    execution_number: int
    n_workers: int
    scale_factor: int
    gpu_count: int
    num_drivers: int
    worker_image: str
    gpu_name: str
    engine: str

    @classmethod
    def from_file(cls, file_path: Path) -> "BenchmarkMetadata":
        data = json.loads(file_path.read_text())
        
        # parse fields, like the timestamp
        data["timestamp"] = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

        return cls(**data)

    def serialize(self) -> dict:
        out = dataclasses.asdict(self)
        out["timestamp"] = out["timestamp"].isoformat()
        return out

    def get_benchmark_definition_name(self) -> str:
        """Return benchmark definition name, e.g., 'tpch-100' for TPC-H SF100."""
        return f"{self.benchmark}-{self.scale_factor}"


@dataclasses.dataclass
class BenchmarkResults:
    benchmark_type: str
    raw_times_ms: dict[str, list[float]]
    failed_queries: dict[str, str]

    @classmethod
    def from_file(cls, file_path: Path) -> "BenchmarkResults":
        data = json.loads(file_path.read_text())

        keys = list(data)
        if keys != ["tpch"]:
            raise ValueError(f"Unexpected benchmark type: {keys}")

        raw_times_ms = data["tpch"]["raw_times_ms"]
        failed_queries = data["tpch"]["failed_queries"]

        return cls(
            benchmark_type="tpch",
            raw_times_ms=raw_times_ms,
            failed_queries=failed_queries,
        )


@dataclasses.dataclass
class PreAggregatedBenchmarkResults:
    """Pre-aggregated benchmark results from benchmark_result.json.

    These files contain aggregated statistics (median, lukewarm, etc.)
    rather than raw per-iteration times.
    """

    benchmark_type: str
    lukewarm_times_ms: dict[str, float]
    median_times_ms: dict[str, float]
    failed_queries: dict[str, str]
    iterations_count: int
    benchmark_name: str
    scale_factor: int

    @classmethod
    def from_file(cls, file_path: Path) -> "PreAggregatedBenchmarkResults":
        data = json.loads(file_path.read_text())

        context = data.get("context", {})
        iterations_count = context.get("iterations_count", 0)
        schema_name = context.get("schema_name", "")

        # Derive benchmark name and scale factor from schema_name
        # e.g. "tpchsf1000" -> benchmark_name="tpch", scale_factor=1000
        match = re.match(r"^(\w+?)sf(\d+)$", schema_name)
        if match:
            benchmark_name = match.group(1)
            scale_factor = int(match.group(2))
        else:
            raise ValueError(
                f"Cannot parse schema_name '{schema_name}' into benchmark + scale factor"
            )

        # Find the benchmark type key (e.g. "tpch")
        benchmark_keys = [k for k in data if k != "context"]
        if len(benchmark_keys) != 1:
            raise ValueError(f"Expected exactly one benchmark type key, got: {benchmark_keys}")
        benchmark_type = benchmark_keys[0]

        agg_times = data[benchmark_type]["agg_times_ms"]
        lukewarm_times_ms = agg_times["lukewarm"]
        median_times_ms = agg_times["median"]
        failed_queries = data[benchmark_type].get("failed_queries", {})

        return cls(
            benchmark_type=benchmark_type,
            lukewarm_times_ms=lukewarm_times_ms,
            median_times_ms=median_times_ms,
            failed_queries=failed_queries,
            iterations_count=iterations_count,
            benchmark_name=benchmark_name,
            scale_factor=scale_factor,
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
        "--data-source",
        choices=["full", "pre-aggregated"],
        default="full",
        help="Data source type: 'full' reads benchmark_full.json (default), "
        "'pre-aggregated' reads benchmark_result.json and posts lukewarm + median",
    )
    parser.add_argument(
        "--cache-state",
        choices=["cold", "warm", "hot", "lukewarm"],
        default=None,
        help="Cache state for the benchmark run (required for 'full' data source)",
    )
    parser.add_argument(
        "--engine-name",
        default=None,
        help="Query engine name (default: derived from benchmark.json 'engine' field)",
    )
    parser.add_argument(
        "--identifier-hash",
        default=None,
        help="Unique identifier hash for the query engine version",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version string for the query engine",
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
        help="Upload *.log files from the benchmark directory as assets (default: True). "
        "Use --no-upload-logs to skip.",
    )

    # Pre-aggregated metadata arguments (used when --data-source=pre-aggregated)
    pre_agg_group = parser.add_argument_group(
        "pre-aggregated metadata",
        "Metadata fields required/optional when --data-source=pre-aggregated",
    )
    pre_agg_group.add_argument(
        "--benchmark-definition-name",
        default=None,
        help="Override benchmark definition name (default: derived from schema_name, e.g. 'tpch-1000')",
    )
    pre_agg_group.add_argument(
        "--n-workers",
        type=int,
        default=None,
        help="Number of worker nodes (required for pre-aggregated)",
    )
    pre_agg_group.add_argument(
        "--timestamp",
        default=None,
        help="Benchmark run timestamp in ISO 8601 format (default: current time)",
    )
    pre_agg_group.add_argument(
        "--gpu-count",
        type=int,
        default=0,
        help="Number of GPUs (default: 0)",
    )
    pre_agg_group.add_argument(
        "--num-drivers",
        type=int,
        default=0,
        help="Number of drivers (default: 0)",
    )
    pre_agg_group.add_argument(
        "--gpu-name",
        default="unknown",
        help="GPU name (default: 'unknown')",
    )
    pre_agg_group.add_argument(
        "--worker-image",
        default="unknown",
        help="Worker image (default: 'unknown')",
    )
    pre_agg_group.add_argument(
        "--kind",
        default="unknown",
        help="Benchmark kind (default: 'unknown')",
    )
    pre_agg_group.add_argument(
        "--execution-number",
        type=int,
        default=0,
        help="Execution number (default: 0)",
    )

    return parser.parse_args()


def generate_identifier_hash(timestamp: datetime, engine: str) -> str:
    """Generate a placeholder identifier hash from timestamp and engine.

    Used when no explicit identifier hash is provided via CLI.
    """
    combined = f"{timestamp.isoformat()}:{engine}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def build_submission_payload(
    benchmark_metadata: BenchmarkMetadata,
    benchmark_results: BenchmarkResults,
    engine_config: EngineConfig,
    sku_name: str,
    storage_configuration_name: str,
    cache_state: str,
    engine_name: str | None,
    identifier_hash: str | None,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    asset_ids: list[int] | None = None,
) -> dict:
    """Build a BenchmarkSubmission payload from parsed dataclasses.

    Args:
        benchmark_metadata: Parsed benchmark.json as BenchmarkMetadata
        benchmark_results: Parsed benchmark_full.json as BenchmarkResults
        engine_config: Parsed config files as EngineConfig
        sku_name: Hardware SKU name
        storage_configuration_name: Storage configuration name
        cache_state: Cache state (cold/warm/hot)
        engine_name: Override for engine name (or None to use metadata)
        identifier_hash: Explicit identifier hash (or None for placeholder)
        version: Explicit version (or None for placeholder)
        commit_hash: Explicit commit hash (or None for placeholder)
        is_official: Whether this is an official benchmark run
    """
    # Use engine from metadata if not overridden
    engine = engine_name or benchmark_metadata.engine

    # Generate or use provided identifier hash
    if identifier_hash is None:
        identifier_hash = generate_identifier_hash(benchmark_metadata.timestamp, engine)

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

    for query_name in query_names:
        times = raw_times[query_name]
        is_failed = query_name in failed_queries

        # Each execution becomes a separate query log entry
        for exec_idx, runtime_ms in enumerate(times):
            query_logs.append(
                {
                    "query_name": query_name.lstrip("Q"),
                    "execution_order": execution_order,
                    "runtime_ms": float(runtime_ms),
                    "status": "error" if is_failed else "success",
                    "extra_info": {
                        "execution_number": exec_idx + 1,
                    },
                }
            )
            execution_order += 1

    # Handle failed queries that may not have times
    for query_name, error_info in failed_queries.items():
        if query_name not in raw_times:
            query_logs.append(
                {
                    "query_name": query_name,
                    "execution_order": execution_order,
                    "runtime_ms": 0.0,
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
        "benchmark_definition_name": benchmark_metadata.get_benchmark_definition_name(),
        "cache_state": cache_state,
        "query_engine": {
            "engine_name": engine,
            "identifier_hash": identifier_hash,
            "version": version,
            "commit_hash": commit_hash,
        },
        "run_at": benchmark_metadata.timestamp.isoformat(),
        "node_count": benchmark_metadata.n_workers,
        "query_logs": query_logs,
        "concurrency_streams": 1,
        "engine_config": engine_config.serialize(),
        "extra_info": extra_info,
        "is_official": is_official,
        "asset_ids": asset_ids,
    }


def build_pre_aggregated_payloads(
    pre_agg_results: PreAggregatedBenchmarkResults,
    metadata: BenchmarkMetadata,
    sku_name: str,
    storage_configuration_name: str,
    engine_name: str,
    identifier_hash: str | None,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    benchmark_definition_name: str | None = None,
    asset_ids: list[int] | None = None,
) -> list[dict]:
    """Build two BenchmarkSubmission payloads from pre-aggregated results.

    Returns a list of two payloads:
      1. Lukewarm (first iteration time) with cache_state="lukewarm"
      2. Median (median of subsequent runs) with cache_state="warm"
    """
    if identifier_hash is None:
        identifier_hash = generate_identifier_hash(metadata.timestamp, engine_name)
    if version is None:
        version = "unknown"
    if commit_hash is None:
        commit_hash = "unknown"

    bench_def_name = benchmark_definition_name or metadata.get_benchmark_definition_name()
    iterations_count = pre_agg_results.iterations_count
    failed_queries = pre_agg_results.failed_queries

    submissions = []
    for label, cache_state, times_ms in [
        ("lukewarm (first iteration) time", "lukewarm", pre_agg_results.lukewarm_times_ms),
        ("median of subsequent runs", "warm", pre_agg_results.median_times_ms),
    ]:
        note = f"Pre-aggregated from {iterations_count} iterations: {label}"

        # Build query logs — one entry per query
        query_logs = []
        query_names = sorted(times_ms.keys(), key=lambda x: int(x[1:]))

        for execution_order, query_name in enumerate(query_names):
            is_failed = query_name in failed_queries
            query_logs.append(
                {
                    "query_name": query_name.lstrip("Q"),
                    "execution_order": execution_order,
                    "runtime_ms": float(times_ms[query_name]),
                    "status": "error" if is_failed else "success",
                    "extra_info": {
                        "note": note,
                    },
                }
            )

        # Handle failed queries that have no times
        execution_order = len(query_logs)
        for query_name, error_info in failed_queries.items():
            if query_name not in times_ms:
                query_logs.append(
                    {
                        "query_name": query_name.lstrip("Q"),
                        "execution_order": execution_order,
                        "runtime_ms": 0.0,
                        "status": "error",
                        "extra_info": {
                            "note": note,
                            "error": str(error_info),
                        },
                    }
                )
                execution_order += 1

        extra_info = {
            "kind": metadata.kind,
            "gpu_count": metadata.gpu_count,
            "gpu_name": metadata.gpu_name,
            "num_drivers": metadata.num_drivers,
            "worker_image": metadata.worker_image,
            "execution_number": metadata.execution_number,
            "note": note,
        }

        submissions.append(
            {
                "sku_name": sku_name,
                "storage_configuration_name": storage_configuration_name,
                "benchmark_definition_name": bench_def_name,
                "cache_state": cache_state,
                "query_engine": {
                    "engine_name": engine_name,
                    "identifier_hash": identifier_hash,
                    "version": version,
                    "commit_hash": commit_hash,
                },
                "run_at": metadata.timestamp.isoformat(),
                "node_count": metadata.n_workers,
                "query_logs": query_logs,
                "concurrency_streams": 1,
                "engine_config": {},
                "extra_info": extra_info,
                "is_official": is_official,
                "asset_ids": asset_ids,
            }
        )

    return submissions


def upload_log_files(
    benchmark_dir: Path,
    api_url: str,
    api_key: str,
    timeout: float,
) -> list[int]:
    """Upload all *.log files from benchmark_dir as assets.

    Args:
        benchmark_dir: Directory to glob for *.log files
        api_url: Base API URL
        api_key: API bearer token
        timeout: Request timeout in seconds

    Returns:
        List of asset IDs from the uploaded files
    """
    log_files = sorted(benchmark_dir.glob("*.log"))
    if not log_files:
        print("  No *.log files found to upload.", file=sys.stderr)
        return []

    print(f"  Uploading {len(log_files)} log file(s)...", file=sys.stderr)
    url = f"{api_url.rstrip('/')}/api/assets/upload/"
    headers = {"Authorization": f"Bearer {api_key}"}
    asset_ids = []

    for log_file in log_files:
        print(f"    Uploading {log_file.name}...", file=sys.stderr, end="")
        with open(log_file, "rb") as f:
            response = httpx.post(
                url,
                files={"file": (log_file.name, f, "text/plain")},
                data={"title": log_file.name, "media_type": "text/plain"},
                headers=headers,
                timeout=timeout,
            )
        if response.status_code >= 400:
            print(f" FAILED ({response.status_code})", file=sys.stderr)
            raise RuntimeError(
                f"Failed to upload {log_file.name}: {response.status_code} {response.text}"
            )
        result = response.json()
        asset_id = result["asset_id"]
        asset_ids.append(asset_id)
        print(f" OK (asset_id={asset_id})", file=sys.stderr)

    return asset_ids


def post_submission(api_url: str, api_key: str, payload: dict, timeout: float) -> tuple[int, str]:
    """Post a benchmark submission to the API.

    Returns:
        Tuple of (status_code, response_text)
    """
    url = f"{api_url.rstrip('/')}/api/benchmark/"
    response = httpx.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
    )
    return response.status_code, response.text


def process_benchmark_dir(
    benchmark_dir: Path,
    *,
    sku_name: str,
    storage_configuration_name: str,
    cache_state: str,
    engine_name: str | None,
    identifier_hash: str | None,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    dry_run: bool,
    api_url: str | None,
    api_key: str | None,
    timeout: float,
    upload_logs: bool = True,
) -> int:
    """Process a benchmark directory and post results to API.

    Returns:
        0 on success, 1 on failure
    """
    print(f"\nProcessing: {benchmark_dir}", file=sys.stderr)

    # Load metadata, results, and config
    try:
        metadata = BenchmarkMetadata.from_file(benchmark_dir / "benchmark.json")
        results = BenchmarkResults.from_file(benchmark_dir / "result_dir" / "benchmark_full.json")
        engine_config = EngineConfig.from_dir(benchmark_dir / "configs")
    except (ValueError, json.JSONDecodeError, FileNotFoundError) as e:
        print(f"  Error loading files: {e}", file=sys.stderr)
        return 1

    print(f"  Timestamp: {metadata.timestamp}", file=sys.stderr)
    print(f"  Engine: {metadata.engine}", file=sys.stderr)
    print(f"  Scale factor: {metadata.scale_factor}", file=sys.stderr)

    # Upload log files as assets
    asset_ids = None
    if upload_logs:
        if dry_run:
            log_files = sorted(benchmark_dir.glob("*.log"))
            print(f"  [DRY RUN] Would upload {len(log_files)} log file(s): {[f.name for f in log_files]}", file=sys.stderr)
        else:
            try:
                asset_ids = upload_log_files(benchmark_dir, api_url, api_key, timeout)
            except (RuntimeError, httpx.RequestError) as e:
                print(f"  Error uploading logs: {e}", file=sys.stderr)
                return 1

    # Build submission payload
    try:
        payload = build_submission_payload(
            benchmark_metadata=metadata,
            benchmark_results=results,
            engine_config=engine_config,
            sku_name=sku_name,
            storage_configuration_name=storage_configuration_name,
            cache_state=cache_state,
            engine_name=engine_name,
            identifier_hash=identifier_hash,
            version=version,
            commit_hash=commit_hash,
            is_official=is_official,
            asset_ids=asset_ids,
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

    if dry_run:
        print("\n  [DRY RUN] Payload:", file=sys.stderr)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    # Post to API
    try:
        status_code, response_text = post_submission(
            api_url, api_key, payload, timeout
        )
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


def process_pre_aggregated_dir(
    benchmark_dir: Path,
    *,
    sku_name: str,
    storage_configuration_name: str,
    engine_name: str,
    n_workers: int,
    timestamp: datetime,
    identifier_hash: str | None,
    version: str | None,
    commit_hash: str | None,
    is_official: bool,
    dry_run: bool,
    api_url: str | None,
    api_key: str | None,
    timeout: float,
    gpu_count: int,
    num_drivers: int,
    gpu_name: str,
    worker_image: str,
    kind: str,
    execution_number: int,
    benchmark_definition_name: str | None = None,
    upload_logs: bool = True,
) -> int:
    """Process a pre-aggregated benchmark directory and post results to API.

    Creates two submissions: one for lukewarm times, one for median times.

    Returns:
        0 on success, 1 on failure
    """
    print(f"\nProcessing (pre-aggregated): {benchmark_dir}", file=sys.stderr)

    # Load pre-aggregated results
    try:
        pre_agg_results = PreAggregatedBenchmarkResults.from_file(
            benchmark_dir / "benchmark_result.json"
        )
    except (ValueError, json.JSONDecodeError, FileNotFoundError) as e:
        print(f"  Error loading benchmark_result.json: {e}", file=sys.stderr)
        return 1

    # Build BenchmarkMetadata from CLI args + derived values
    metadata = BenchmarkMetadata(
        kind=kind,
        benchmark=pre_agg_results.benchmark_name,
        timestamp=timestamp,
        execution_number=execution_number,
        n_workers=n_workers,
        scale_factor=pre_agg_results.scale_factor,
        gpu_count=gpu_count,
        num_drivers=num_drivers,
        worker_image=worker_image,
        gpu_name=gpu_name,
        engine=engine_name,
    )

    print(f"  Benchmark: {metadata.get_benchmark_definition_name()}", file=sys.stderr)
    print(f"  Engine: {engine_name}", file=sys.stderr)
    print(f"  Node count: {n_workers}", file=sys.stderr)
    print(f"  Iterations: {pre_agg_results.iterations_count}", file=sys.stderr)

    # Upload log files as assets
    asset_ids = None
    if upload_logs:
        if dry_run:
            log_files = sorted(benchmark_dir.glob("*.log"))
            print(f"  [DRY RUN] Would upload {len(log_files)} log file(s): {[f.name for f in log_files]}", file=sys.stderr)
        else:
            try:
                asset_ids = upload_log_files(benchmark_dir, api_url, api_key, timeout)
            except (RuntimeError, httpx.RequestError) as e:
                print(f"  Error uploading logs: {e}", file=sys.stderr)
                return 1

    # Build two payloads
    try:
        payloads = build_pre_aggregated_payloads(
            pre_agg_results=pre_agg_results,
            metadata=metadata,
            sku_name=sku_name,
            storage_configuration_name=storage_configuration_name,
            engine_name=engine_name,
            identifier_hash=identifier_hash,
            version=version,
            commit_hash=commit_hash,
            is_official=is_official,
            benchmark_definition_name=benchmark_definition_name,
            asset_ids=asset_ids,
        )
    except Exception as e:
        print(f"  Error building payloads: {e}", file=sys.stderr)
        return 1

    # Post (or dry-run) each payload
    for i, payload in enumerate(payloads):
        label = payload["cache_state"]
        print(f"\n  Submission {i + 1}/{len(payloads)} ({label}):", file=sys.stderr)
        print(f"    Query logs: {len(payload['query_logs'])}", file=sys.stderr)

        if dry_run:
            print(f"\n  [DRY RUN] Payload ({label}):", file=sys.stderr)
            print(json.dumps(payload, indent=2, default=str))
            continue

        try:
            status_code, response_text = post_submission(
                api_url, api_key, payload, timeout
            )
            print(f"    Status: {status_code}", file=sys.stderr)
            if status_code >= 400:
                print(f"    Response: {response_text}", file=sys.stderr)
                return 1
            else:
                print(f"    Success: {response_text}", file=sys.stderr)
        except httpx.RequestError as e:
            print(f"    Error posting: {e}", file=sys.stderr)
            return 1

    return 0


def main() -> int:
    args = parse_args()

    # Validate required arguments
    if not args.api_url and not args.dry_run:
        print(
            "Error: --api-url or BENCHMARK_API_URL environment variable required",
            file=sys.stderr,
        )
        return 1

    if not args.api_key and not args.dry_run:
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

    if args.data_source == "pre-aggregated":
        # Validate required args for pre-aggregated
        if not args.engine_name:
            print(
                "Error: --engine-name is required when --data-source=pre-aggregated",
                file=sys.stderr,
            )
            return 1
        if args.n_workers is None:
            print(
                "Error: --n-workers is required when --data-source=pre-aggregated",
                file=sys.stderr,
            )
            return 1

        # Parse or default the timestamp
        if args.timestamp:
            try:
                timestamp = datetime.fromisoformat(args.timestamp)
            except ValueError:
                print(
                    f"Error: --timestamp is not valid ISO 8601: {args.timestamp}",
                    file=sys.stderr,
                )
                return 1
        else:
            timestamp = datetime.now()

        result = process_pre_aggregated_dir(
            benchmark_dir,
            sku_name=args.sku_name,
            storage_configuration_name=args.storage_configuration_name,
            engine_name=args.engine_name,
            n_workers=args.n_workers,
            timestamp=timestamp,
            identifier_hash=args.identifier_hash,
            version=args.version,
            commit_hash=args.commit_hash,
            is_official=args.is_official,
            dry_run=args.dry_run,
            api_url=args.api_url,
            api_key=args.api_key,
            timeout=args.timeout,
            gpu_count=args.gpu_count,
            num_drivers=args.num_drivers,
            gpu_name=args.gpu_name,
            worker_image=args.worker_image,
            kind=args.kind,
            execution_number=args.execution_number,
            benchmark_definition_name=args.benchmark_definition_name,
            upload_logs=args.upload_logs,
        )
    else:
        # Full data source — cache-state is required
        if args.cache_state is None:
            print(
                "Error: --cache-state is required when --data-source=full",
                file=sys.stderr,
            )
            return 1

        result = process_benchmark_dir(
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
            api_url=args.api_url,
            api_key=args.api_key,
            timeout=args.timeout,
            upload_logs=args.upload_logs,
        )

    return result


if __name__ == "__main__":
    sys.exit(main())

