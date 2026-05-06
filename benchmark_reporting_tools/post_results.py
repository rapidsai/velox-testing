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

The script reads benchmark_result.json from the input directory.
Engine configs and worker logs are automatically loaded from the
velox-testing repo by detecting the engine variant from the
benchmark results context.  Paths can be overridden with
--config-dir and --logs-dir.

Default locations (derived from the repo root and detected variant):
    configs: presto/docker/config/generated/{variant}/
    logs:    presto/scripts/presto_logs/

Usage:
    # Auto-detect configs/logs from the repo (default):
    python benchmark_reporting_tools/post_results.py /path/to/benchmark_output \
        --sku-name PDX-H100 \
        --storage-configuration-name pdx-lustre-sf-100 \
        --benchmark-name tpch \
        --identifier-hash abc123 \
        --cache-state warm

    # Override with explicit paths:
    python benchmark_reporting_tools/post_results.py /path/to/benchmark_output \
        --config-dir /custom/path/to/configs \
        --logs-dir /custom/path/to/logs \
        --sku-name PDX-H100 \
        --storage-configuration-name pdx-lustre-sf-100 \
        --benchmark-name tpch \
        --identifier-hash abc123 \
        --cache-state warm

Environment variables:
    BENCHMARK_API_URL: API URL
    BENCHMARK_API_KEY: API key for authentication

"""

import argparse
import asyncio
import dataclasses
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx

_ENGINE_TO_VARIANT = {
    "presto-velox-gpu": "gpu",
    "presto-velox-cpu": "cpu",
    "presto-java": "java",
}


def _repo_root() -> Path:
    """Return the velox-testing repo root (parent of benchmark_reporting_tools/)."""
    return Path(__file__).resolve().parent.parent


def _default_config_dir(variant: str) -> Path | None:
    """Derive the generated config directory for a given variant."""
    d = _repo_root() / "presto" / "docker" / "config" / "generated" / variant
    return d if d.is_dir() else None


def _default_logs_dir() -> Path | None:
    """Return the presto_logs directory."""
    link = _repo_root() / "presto" / "scripts" / "presto_logs"
    if link.exists():
        return link.resolve()
    return None


@dataclasses.dataclass(kw_only=True)
class BenchmarkMetadata:
    benchmark: list[str]
    timestamp: datetime
    engine: str
    kind: str | None = None
    execution_number: int = 1
    worker_count: int | None = None
    node_count: int | None = None
    scale_factor: int | None = None
    gpu_count: int | None = None
    num_drivers: int | None = None
    gpu_name: str | None = None
    image_digest: str | None = None

    @classmethod
    def from_parsed(cls, raw: dict) -> "BenchmarkMetadata":
        """Extract metadata from the 'context' section of a parsed benchmark_result.json."""
        data = dict(raw["context"])
        data["timestamp"] = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))

        # Normalise legacy string values to a list.
        if isinstance(data.get("benchmark"), str):
            data["benchmark"] = [data["benchmark"]]

        known_fields = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known_fields}

        return cls(**filtered)

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
    def from_parsed(cls, data: dict, benchmark_name: str) -> "BenchmarkResults":
        if benchmark_name not in data:
            raise KeyError(f"Expected '{benchmark_name}' key, got: {sorted(data.keys())}")

        raw_times_ms = data[benchmark_name]["raw_times_ms"]
        failed_queries = data[benchmark_name]["failed_queries"]

        return cls(
            benchmark_type=benchmark_name,
            raw_times_ms=raw_times_ms,
            failed_queries=failed_queries,
        )


def _parse_config_file(file_path: Path) -> dict[str, str]:
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


def _find_config_file(configs_dir: Path, subdir: str, variant: str | None = None) -> Path | None:
    """Locate a config file under {configs_dir}/{subdir}/.

    When variant is known, the correct properties file is selected directly
    (config_java for 'java', config_native for 'gpu'/'cpu').
    """
    sub = configs_dir / subdir
    if not sub.is_dir():
        return None
    if variant == "java":
        candidate = sub / "config_java.properties"
    elif variant in ("gpu", "cpu"):
        candidate = sub / "config_native.properties"
    else:
        candidate = None
    if candidate and candidate.is_file():
        return candidate
    for fallback in sorted(sub.glob("config_*.properties")):
        return fallback
    return None


@dataclasses.dataclass
class EngineConfig:
    coordinator: dict[str, str]
    worker: dict[str, str]

    @classmethod
    def from_dir(cls, configs_dir: Path, variant: str | None = None) -> "EngineConfig":
        """Load engine configuration from a configs directory.

        Expects the generated layout:
          etc_coordinator/config_*.properties
          etc_worker/config_*.properties

        When variant is provided ('gpu', 'cpu', or 'java'), selects the
        matching properties file (config_native vs config_java).
        """
        coord_file = _find_config_file(configs_dir, "etc_coordinator", variant)
        worker_file = _find_config_file(configs_dir, "etc_worker", variant)
        if coord_file is None or worker_file is None:
            raise FileNotFoundError(
                f"Could not find coordinator/worker config files in {configs_dir}. "
                "Expected etc_coordinator/config_*.properties + etc_worker/config_*.properties."
            )
        coordinator_config = _parse_config_file(coord_file)
        worker_config = _parse_config_file(worker_file)
        return cls(coordinator=coordinator_config, worker=worker_config)

    def serialize(self) -> dict:
        return dataclasses.asdict(self)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Post Velox benchmark results to the API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to benchmark directory containing benchmark_result.json",
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
        help="Query engine name (overrides the 'engine' field from benchmark_result.json context)",
    )
    parser.add_argument(
        "--identifier-hash",
        default=None,
        help="Unique identifier hash for software environment (e.g. a container image digest). "
        "If omitted, the image_digest from benchmark_result.json context is used.",
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
        help="Velox branch used to build the worker image.",
    )
    parser.add_argument(
        "--velox-repo",
        default=None,
        help="Velox repository used to build the worker image.",
    )
    parser.add_argument(
        "--presto-branch",
        default=None,
        help="Presto branch used to build the worker image.",
    )
    parser.add_argument(
        "--presto-repo",
        default=None,
        help="Presto repository used to build the worker image.",
    )
    parser.add_argument(
        "--concurrency-streams",
        help="Number of concurrency streams to use for the benchmark run",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default=None,
        help="Override config directory. Default: auto-detected from the engine variant "
        "in benchmark_result.json → presto/docker/config/generated/{variant}/.",
    )
    parser.add_argument(
        "--logs-dir",
        type=str,
        default=None,
        help="Override server logs directory. Default: presto/scripts/presto_logs/.",
    )

    return parser.parse_args()


def _normalize_api_url(url: str) -> str:
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


def _build_submission_payload(
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
) -> dict:
    """Build a BenchmarkSubmission payload from parsed dataclasses.

    Args:
        benchmark_metadata: Parsed from the 'context' section of benchmark_result.json
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

    def _query_sort_key(name: str):
        stripped = name.lstrip("Qq")
        match = re.match(r"(\d+)(.*)", stripped)
        if match:
            return (int(match.group(1)), match.group(2))
        return (float("inf"), name)

    query_names = sorted(raw_times.keys(), key=_query_sort_key)

    for query_name in query_names:
        times = raw_times[query_name]
        # Allow incomplete test results to be posted
        if times is None:
            times = []
        is_failed = query_name in failed_queries

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

    # Build extra info from metadata, omitting None values
    extra_info = {
        k: v
        for k, v in {
            "kind": benchmark_metadata.kind,
            "gpu_count": benchmark_metadata.gpu_count,
            "gpu_name": benchmark_metadata.gpu_name,
            "num_drivers": benchmark_metadata.num_drivers,
            "execution_number": benchmark_metadata.execution_number,
        }.items()
        if v is not None
    }

    engine_config_payload = engine_config.serialize() if engine_config else {}
    if velox_branch or velox_repo or presto_branch or presto_repo:
        engine_config_payload = {
            **engine_config_payload,
            "velox_branch": velox_branch,
            "velox_repo": velox_repo,
            "presto_branch": presto_branch,
            "presto_repo": presto_repo,
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
        "node_count": benchmark_metadata.node_count or 1,
        "gpu_count": benchmark_metadata.gpu_count or 0,
        "query_logs": query_logs,
        "concurrency_streams": concurrency_streams,
        "engine_config": engine_config_payload,
        "extra_info": extra_info,
        "is_official": is_official,
        "asset_ids": asset_ids,
    }


def _build_http_client(api_url: str, api_key: str, timeout: float) -> httpx.AsyncClient:
    base_url = _normalize_api_url(api_url)
    transport = httpx.AsyncHTTPTransport(retries=3)
    return httpx.AsyncClient(
        base_url=base_url,
        transport=transport,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
    )


async def _upload_log_files(
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
    if not log_files:
        return []

    print(f"  Uploading {len(log_files)} log file(s) (max {max_concurrency} concurrent)...", file=sys.stderr)
    semaphore = asyncio.Semaphore(max_concurrency)

    async with _build_http_client(api_url, api_key, timeout) as client:

        async def _upload_one(log_file: Path) -> int:
            async with semaphore:
                print(f"    Uploading {log_file.name}...", file=sys.stderr)
                content = log_file.read_bytes()
                response = await client.post(
                    "/api/assets/upload/",
                    files={"file": (log_file.name, content, "text/plain")},
                    data={"title": log_file.name, "media_type": "text/plain"},
                )
                if response.status_code >= 400:
                    raise RuntimeError(f"Failed to upload {log_file.name}: {response.status_code} {response.text}")
                result = response.json()
                asset_id = result["asset_id"]
                print(f"    Uploaded {log_file.name} (asset_id={asset_id})", file=sys.stderr)
                return asset_id

        asset_ids = await asyncio.gather(*[_upload_one(f) for f in log_files])

    return list(asset_ids)


async def _post_submission(api_url: str, api_key: str, payload: dict, timeout: float) -> tuple[int, str]:
    """Post a benchmark submission to the API.

    Returns:
        Tuple of (status_code, response_text)
    """
    async with _build_http_client(api_url, api_key, timeout) as client:
        response = await client.post("/api/benchmark/", json=payload)
    return response.status_code, response.text


async def _process_benchmark_dir(
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
    api_url: str,
    api_key: str,
    timeout: float,
    upload_logs: bool = True,
    benchmark_definition_name: str,
    concurrency_streams: int = 1,
    config_dir: Path | None = None,
    logs_dir: Path | None = None,
    velox_branch: str | None = None,
    velox_repo: str | None = None,
    presto_branch: str | None = None,
    presto_repo: str | None = None,
) -> int:
    """Process a benchmark directory and post results to API.

    Returns:
        0 on success, 1 on failure
    """
    print(f"\nProcessing: {benchmark_dir}", file=sys.stderr)

    # Load metadata and results from benchmark_result.json.
    # The "context" section contains run metadata; benchmark data sits
    # under a top-level key matching the benchmark name (e.g. "tpch").

    result_file = benchmark_dir / "benchmark_result.json"

    try:
        raw = json.loads(result_file.read_text())
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"  Error reading {result_file}: {e}", file=sys.stderr)
        return 1

    try:
        benchmark_metadata = BenchmarkMetadata.from_parsed(raw)
    except (ValueError, KeyError) as e:
        print(f"  Error loading metadata: {e}", file=sys.stderr)
        return 1

    # Fall back to the container image_digest captured in the benchmark
    # results context when no explicit identifier_hash was provided on the CLI.
    if identifier_hash is None:
        identifier_hash = benchmark_metadata.image_digest
    if identifier_hash is None:
        print(
            "  Error: --identifier-hash was not provided and benchmark_result.json "
            "context has no image_digest to fall back to.",
            file=sys.stderr,
        )
        return 1

    # Resolve config directory: explicit override → auto-detect from variant
    effective_config_dir = config_dir
    variant = _ENGINE_TO_VARIANT.get(benchmark_metadata.engine)
    if effective_config_dir is None:
        if variant:
            effective_config_dir = _default_config_dir(variant)
            if effective_config_dir:
                print(f"  Auto-detected variant '{variant}' → config dir: {effective_config_dir}", file=sys.stderr)
            else:
                print(f"  Auto-detected variant '{variant}' but config dir does not exist.", file=sys.stderr)
        else:
            print(f"  Could not map engine '{benchmark_metadata.engine}' to a variant.", file=sys.stderr)

    if effective_config_dir and effective_config_dir.exists():
        print(f"  Loading engine config from {effective_config_dir}...", file=sys.stderr)
        try:
            engine_config = EngineConfig.from_dir(effective_config_dir, variant=variant)
        except FileNotFoundError as e:
            print(f"  Warning: could not load engine config: {e}", file=sys.stderr)
            engine_config = None
    else:
        if effective_config_dir:
            print(f"  Warning: config directory does not exist: {effective_config_dir}", file=sys.stderr)
        else:
            print("  Warning: no config directory found. Use --config-dir to specify one.", file=sys.stderr)
        engine_config = None

    # Resolve logs directory: explicit override → auto-detect from repo
    effective_logs_dir = logs_dir
    if effective_logs_dir is None:
        effective_logs_dir = _default_logs_dir()
        if effective_logs_dir:
            print(f"  Auto-detected logs dir: {effective_logs_dir}", file=sys.stderr)

    asset_ids = None
    if upload_logs and effective_logs_dir and effective_logs_dir.exists():
        if dry_run:
            log_files = sorted(effective_logs_dir.glob("*.log"))
            print(
                f"  [DRY RUN] Would upload {len(log_files)} log file(s) from {effective_logs_dir}: "
                f"{[f.name for f in log_files]}",
                file=sys.stderr,
            )
        else:
            try:
                asset_ids = await _upload_log_files(effective_logs_dir, api_url, api_key, timeout)
            except (RuntimeError, httpx.RequestError) as e:
                print(f"  Error uploading logs: {e}", file=sys.stderr)
                return 1
    elif upload_logs:
        print("  No logs directory found; skipping log upload.", file=sys.stderr)

    # Process each benchmark type found in the result file.
    overall_result = 0
    for bench_name in benchmark_metadata.benchmark:
        print(f"\n  Processing benchmark type: {bench_name}", file=sys.stderr)

        try:
            results = BenchmarkResults.from_parsed(raw, benchmark_name=bench_name)
        except (ValueError, KeyError) as e:
            print(f"  Error loading results for '{bench_name}': {e}", file=sys.stderr)
            overall_result = 1
            continue

        try:
            payload = _build_submission_payload(
                benchmark_metadata=benchmark_metadata,
                benchmark_results=results,
                engine_config=engine_config,
                benchmark_definition_name=benchmark_definition_name,
                sku_name=sku_name,
                storage_configuration_name=storage_configuration_name,
                cache_state=cache_state,
                engine_name=engine_name,
                identifier_hash=identifier_hash,
                version=version,
                commit_hash=commit_hash,
                is_official=is_official,
                asset_ids=asset_ids,
                concurrency_streams=concurrency_streams,
                velox_branch=velox_branch,
                velox_repo=velox_repo,
                presto_branch=presto_branch,
                presto_repo=presto_repo,
            )
        except Exception as e:
            print(f"  Error building payload for '{bench_name}': {e}", file=sys.stderr)
            overall_result = 1
            continue

        print(f"  Benchmark definition: {payload['benchmark_definition_name']}", file=sys.stderr)
        print(f"  Engine: {payload['query_engine']['engine_name']}", file=sys.stderr)
        print(f"  Identifier hash: {payload['query_engine']['identifier_hash']}", file=sys.stderr)
        print(f"  Node count: {payload['node_count']}", file=sys.stderr)
        print(f"  Query logs: {len(payload['query_logs'])}", file=sys.stderr)

        if dry_run:
            print("\n  [DRY RUN] Payload:", file=sys.stderr)
            print(json.dumps(payload, indent=2, default=str))
            continue

        try:
            status_code, response_text = await _post_submission(api_url, api_key, payload, timeout)
            print(f"  Status: {status_code}", file=sys.stderr)
            if status_code >= 400:
                print(f"  Response: {response_text}", file=sys.stderr)
                overall_result = 1
            else:
                print(f"  Success: {response_text}", file=sys.stderr)
        except httpx.RequestError as e:
            print(f"  Error posting for '{bench_name}': {e}", file=sys.stderr)
            overall_result = 1

    return overall_result


async def main() -> int:
    args = _parse_args()

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

    result = await _process_benchmark_dir(
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
        concurrency_streams=args.concurrency_streams,
        config_dir=Path(args.config_dir) if args.config_dir else None,
        logs_dir=Path(args.logs_dir) if args.logs_dir else None,
        velox_branch=args.velox_branch,
        velox_repo=args.velox_repo,
        presto_branch=args.presto_branch,
        presto_repo=args.presto_repo,
    )

    return result


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
