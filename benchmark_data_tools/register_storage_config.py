# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
# ]
# ///
"""
CLI for registering a benchmark dataset as a storage configuration in the API.

Reads metadata.json from a dataset directory, auto-detects storage system and
GDS configuration, and POSTs to /api/storage-configs/. The assigned name is
written back into metadata.json for use by post_results.py.

The storage configuration name is auto-generated as:
    {machine}-{storage_system}-{benchmark_type}-sf{scale_factor}
e.g.: pdx-lustre-tpch-sf100

Usage:
    python benchmark_data_tools/register_storage_config.py /path/to/dataset \\
        --machine pdx

    # With explicit name override:
    python benchmark_data_tools/register_storage_config.py /path/to/dataset \\
        --machine pdx --name pdx-lustre-tpch-sf100

    # Dry run (no API calls):
    python benchmark_data_tools/register_storage_config.py /path/to/dataset \\
        --machine pdx --dry-run

Environment variables:
    BENCHMARK_API_URL: API base URL
    BENCHMARK_API_KEY: API key for authentication
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx

_FSTYPE_TO_STORAGE_SYSTEM = {
    "lustre": "lustre",
    "nfs": "nfs",
    "nfs4": "nfs",
}
_LOCAL_FSTYPES = frozenset(("ext4", "xfs", "btrfs", "ext3", "ext2", "tmpfs", "overlay"))


def _detect_storage_system(path: Path) -> str | None:
    """Detect the storage system type for the filesystem containing path."""
    try:
        result = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE", str(path.resolve())],
            capture_output=True,
            text=True,
        )
        fstype = result.stdout.strip().lower()
    except FileNotFoundError:
        return None

    if not fstype:
        return None

    if fstype in _FSTYPE_TO_STORAGE_SYSTEM:
        return _FSTYPE_TO_STORAGE_SYSTEM[fstype]

    if fstype in _LOCAL_FSTYPES:
        return _detect_local_device_type(path)

    return None


def _detect_local_device_type(path: Path) -> str:
    """Distinguish NVMe from other local storage by inspecting the backing block device."""
    try:
        df_result = subprocess.run(
            ["df", "--output=source", str(path.resolve())],
            capture_output=True,
            text=True,
        )
        lines = df_result.stdout.strip().splitlines()
        if len(lines) < 2:
            return "local"
        device = lines[1].strip()

        if not device.startswith("/dev/"):
            return "local"

        device_name = device.removeprefix("/dev/")
        # Strip partition suffix: nvme0n1p1 → nvme0n1, sda1 → sda
        base_device = re.sub(r"p?\d+$", "", device_name)

        tran_result = subprocess.run(
            ["lsblk", "-d", "-n", "-o", "TRAN", f"/dev/{base_device}"],
            capture_output=True,
            text=True,
        )
        if tran_result.stdout.strip().lower() == "nvme":
            return "nvme"
    except (FileNotFoundError, Exception):
        pass

    return "local"


def _detect_gds_enabled() -> bool:
    """Return True if the nvidia-fs kernel module (GPU Direct Storage) is loaded."""
    try:
        result = subprocess.run(["lsmod"], capture_output=True, text=True)
        return "nvidia_fs" in result.stdout
    except FileNotFoundError:
        return False


def _derive_compression(metadata: dict) -> str:
    """Derive the primary compression from codec_definitions; default to 'snappy'."""
    codec_defs = metadata.get("codec_definitions")
    if not codec_defs:
        return "snappy"

    compressions = {
        table["compression"].lower()
        for table in codec_defs.get("tables", [])
        if table.get("compression")
    }
    if len(compressions) == 1:
        return compressions.pop()

    return "snappy"


def _generate_name(machine: str, storage_system: str, metadata: dict) -> str:
    """Auto-generate a storage configuration name from dataset attributes."""
    benchmark_type = metadata.get("benchmark_type", "unknown")
    sf_int = int(metadata.get("scale_factor", 0))
    return f"{machine}-{storage_system}-{benchmark_type}-sf{sf_int}"


def _build_labels(machine: str, metadata: dict, extra_labels: list[str]) -> list[str]:
    benchmark_type = metadata.get("benchmark_type", "unknown")
    sf_int = int(metadata.get("scale_factor", 0))
    labels = [benchmark_type, machine, str(sf_int)]
    labels.extend(extra_labels)
    return labels


def _build_payload(
    name: str,
    machine: str,
    storage_system: str,
    compression: str,
    region: str,
    is_gds_enabled: bool,
    path: str,
    extra_labels: list[str],
    metadata: dict,
) -> dict:
    parquet_version = metadata.get("parquet_version", 2)
    return {
        "storage_configuration_name": name,
        "storage_system": storage_system,
        "file_format": f"parquet-v{parquet_version}",
        "compression": compression,
        "region": region,
        "is_gds_enabled": is_gds_enabled,
        "labels": _build_labels(machine, metadata, extra_labels),
        "path": path,
        "table_metadata": metadata,
    }


def _normalize_api_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")).rstrip("/")


def _build_client(api_url: str, api_key: str, timeout: float) -> httpx.Client:
    return httpx.Client(
        base_url=_normalize_api_url(api_url),
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
        transport=httpx.HTTPTransport(retries=3),
    )


def _check_name_exists(client: httpx.Client, name: str) -> bool:
    """Return True if the storage config name already exists in the database."""
    response = client.get(f"/api/storage-configs/{name}/")
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    raise RuntimeError(f"Unexpected response checking name: {response.status_code} {response.text}")


def _post_storage_config(client: httpx.Client, payload: dict) -> tuple[int, str]:
    response = client.post("/api/storage-configs/", json=payload)
    return response.status_code, response.text


def _write_name_to_metadata(metadata_path: Path, name: str) -> None:
    """Add storage_configuration_name to an existing metadata.json."""
    metadata = json.loads(metadata_path.read_text())
    metadata["storage_configuration_name"] = name
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")


def register_storage_config(
    data_dir: Path,
    machine: str,
    name: str | None = None,
    storage_system: str | None = None,
    compression: str | None = None,
    region: str = "n/a",
    is_gds_enabled: bool | None = None,
    extra_labels: list[str] | None = None,
    path_override: str | None = None,
    api_url: str | None = None,
    api_key: str | None = None,
    dry_run: bool = False,
    timeout: float = 30.0,
) -> int:
    """Register a dataset directory as a storage configuration.

    Reads metadata.json from data_dir, resolves all fields (auto-detecting
    storage_system and is_gds_enabled when not provided), performs a pre-flight
    uniqueness check, POSTs to /api/storage-configs/, and writes the resulting
    storage_configuration_name back into metadata.json.

    Returns 0 on success, 1 on failure.
    """
    metadata_path = data_dir / "metadata.json"
    if not metadata_path.exists():
        print(f"Error: no metadata.json found in {data_dir}", file=sys.stderr)
        return 1

    metadata = json.loads(metadata_path.read_text())

    # Resolve storage system
    if storage_system is None:
        print("  Auto-detecting storage system...", file=sys.stderr)
        storage_system = _detect_storage_system(data_dir)
        if storage_system is None:
            print(
                "Error: could not auto-detect storage system. "
                "Use --storage-system to specify one explicitly.",
                file=sys.stderr,
            )
            return 1
        print(f"  Detected storage system: {storage_system}", file=sys.stderr)

    # Resolve GDS
    if is_gds_enabled is None:
        is_gds_enabled = _detect_gds_enabled()
        print(f"  Detected GDS enabled: {is_gds_enabled}", file=sys.stderr)

    # Resolve compression
    if compression is None:
        compression = _derive_compression(metadata)
        print(f"  Derived compression: {compression}", file=sys.stderr)

    # Resolve name
    if name is None:
        name = _generate_name(machine, storage_system, metadata)
        print(f"  Auto-generated name: {name}", file=sys.stderr)

    # Resolve path
    dataset_path = path_override or metadata.get("data_dir_path") or str(data_dir.resolve())

    payload = _build_payload(
        name=name,
        machine=machine,
        storage_system=storage_system,
        compression=compression,
        region=region,
        is_gds_enabled=is_gds_enabled,
        path=dataset_path,
        extra_labels=extra_labels or [],
        metadata=metadata,
    )

    if dry_run:
        print("\n[DRY RUN] Would POST to /api/storage-configs/:", file=sys.stderr)
        print(json.dumps(payload, indent=2))
        print(
            f"\n[DRY RUN] Would write storage_configuration_name='{name}' to {metadata_path}",
            file=sys.stderr,
        )
        return 0

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

    try:
        with _build_client(api_url, api_key, timeout) as client:
            print(f"  Checking if '{name}' already exists...", file=sys.stderr)
            try:
                if _check_name_exists(client, name):
                    print(
                        f"Error: storage configuration '{name}' already exists in the database.\n"
                        "Use --name to specify a different name.",
                        file=sys.stderr,
                    )
                    return 1
            except RuntimeError as e:
                print(f"Error checking name existence: {e}", file=sys.stderr)
                return 1

            print(f"  Registering storage configuration '{name}'...", file=sys.stderr)
            status_code, response_text = _post_storage_config(client, payload)
    except httpx.RequestError as e:
        print(f"Error communicating with API: {e}", file=sys.stderr)
        return 1

    if status_code >= 400:
        print(f"Error: API returned {status_code}: {response_text}", file=sys.stderr)
        return 1

    print(f"  Registered '{name}' successfully.", file=sys.stderr)
    _write_name_to_metadata(metadata_path, name)
    print(f"  Wrote storage_configuration_name to {metadata_path}", file=sys.stderr)

    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register a benchmark dataset as a storage configuration in the API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Path to the dataset directory containing metadata.json",
    )
    parser.add_argument(
        "--machine",
        required=True,
        help="Machine or cluster where the dataset is accessible (e.g. 'pdx', 'umb-219-B200'). "
        "Used as a label and in the auto-generated name.",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Storage configuration name. Auto-generated as "
        "{machine}-{storage_system}-{benchmark_type}-sf{scale_factor} if omitted.",
    )
    parser.add_argument(
        "--storage-system",
        default=None,
        help="Storage system type (e.g. 'nvme', 'lustre', 'nfs'). Auto-detected if omitted.",
    )
    parser.add_argument(
        "--compression",
        default=None,
        help="Primary compression codec (e.g. 'snappy', 'zstd'). "
        "Derived from codec_definitions in metadata.json if omitted; defaults to 'snappy'.",
    )
    parser.add_argument(
        "--region",
        default="n/a",
        help="Storage region (default: 'n/a' for on-premises).",
    )
    parser.add_argument(
        "--is-gds-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Whether GPU Direct Storage is enabled. Auto-detected from lsmod if omitted.",
    )
    parser.add_argument(
        "--label",
        dest="labels",
        action="append",
        default=[],
        metavar="LABEL",
        help="Additional label to attach to the storage config. Can be specified multiple times.",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Filesystem path to the dataset root. Defaults to data_dir_path from metadata.json.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("BENCHMARK_API_URL"),
        help="API base URL (default: from BENCHMARK_API_URL env var)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BENCHMARK_API_KEY"),
        help="API key for authentication (default: from BENCHMARK_API_KEY env var)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the payload without posting to the API.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        register_storage_config(
            data_dir=args.data_dir,
            machine=args.machine,
            name=args.name,
            storage_system=args.storage_system,
            compression=args.compression,
            region=args.region,
            is_gds_enabled=args.is_gds_enabled,
            extra_labels=args.labels,
            path_override=args.path,
            api_url=args.api_url,
            api_key=args.api_key,
            dry_run=args.dry_run,
            timeout=args.timeout,
        )
    )
