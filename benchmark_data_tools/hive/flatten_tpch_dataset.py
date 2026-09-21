#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Copy a partitioned TPC-H dataset into an unpartitioned, byte-identical layout."""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Sequence

import pyarrow.fs as pafs

try:
    from .publish_output_files import DestinationLocation
except ImportError:
    from publish_output_files import DestinationLocation

PARTITION_COLUMNS = {
    "orders": "o_ordermonth",
    "lineitem": "l_shipmonth",
}
FLAT_TABLES = ("customer", "part", "partsupp", "supplier", "nation", "region")
TABLES = tuple(PARTITION_COLUMNS) + FLAT_TABLES
MANIFEST_NAME = "_flatten_manifest.json"


@dataclass(frozen=True)
class CopyEntry:
    table: str
    source: str
    destination: str
    physical_bytes: int


def relative_path(root: str, path: str) -> PurePosixPath:
    root_path = PurePosixPath(root.rstrip("/"))
    try:
        return PurePosixPath(path).relative_to(root_path)
    except ValueError as error:
        raise ValueError(f"Path is outside the dataset root: {path}") from error


def flattened_path(relative: PurePosixPath) -> PurePosixPath:
    if not relative.parts:
        raise ValueError("Dataset object has an empty relative path")
    table = relative.parts[0]
    if table not in TABLES:
        raise ValueError(f"Unknown TPC-H table directory: {table}")
    if relative.suffix != ".parquet":
        raise ValueError(f"Unexpected non-Parquet dataset object: {relative}")

    if table in PARTITION_COLUMNS:
        if len(relative.parts) != 3:
            raise ValueError(f"Expected table/partition/file for {relative}")
        partition, filename = relative.parts[1:]
        expected = f"{PARTITION_COLUMNS[table]}="
        if not partition.startswith(expected) or not partition.removeprefix(expected):
            raise ValueError(f"Invalid {table} partition directory: {partition}")
        return PurePosixPath(table, f"{partition}__{filename}")

    if len(relative.parts) != 2:
        raise ValueError(f"Expected table/file for {relative}")
    return relative


def filesystem(location: DestinationLocation, region: str | None) -> pafs.FileSystem:
    if location.scheme == "file":
        return pafs.LocalFileSystem()
    bucket = location.root.split("/", 1)[0]
    return pafs.S3FileSystem(region=region or pafs.resolve_s3_region(bucket))


def list_files(fs: pafs.FileSystem, root: str) -> list[pafs.FileInfo]:
    selector = pafs.FileSelector(root.rstrip("/"), recursive=True, allow_not_found=True)
    return sorted(
        (item for item in fs.get_file_info(selector) if item.type == pafs.FileType.File),
        key=lambda item: item.path,
    )


def build_plan(source_root: str, source_files: Sequence[pafs.FileInfo], destination_root: str) -> list[CopyEntry]:
    plan = []
    destinations = set()
    for source in source_files:
        relative = relative_path(source_root, source.path)
        destination = PurePosixPath(destination_root.rstrip("/"), flattened_path(relative)).as_posix()
        if destination in destinations:
            raise ValueError(f"Multiple source objects map to {destination}")
        destinations.add(destination)
        plan.append(CopyEntry(relative.parts[0], source.path, destination, source.size))
    if not plan:
        raise ValueError(f"No dataset objects found below {source_root}")
    return plan


def validate_locations(source: DestinationLocation, destination: DestinationLocation) -> None:
    if source.scheme != destination.scheme:
        raise ValueError("Source and destination must use the same filesystem")
    if source.scheme == "s3":
        source_bucket = source.root.split("/", 1)[0]
        destination_bucket = destination.root.split("/", 1)[0]
        if source_bucket != destination_bucket:
            raise ValueError("S3 server-side copying requires source and destination in the same bucket")
    source_root = source.root.rstrip("/")
    destination_root = destination.root.rstrip("/")
    if source_root == destination_root:
        raise ValueError("Source and destination must differ")
    if source_root.startswith(destination_root + "/") or destination_root.startswith(source_root + "/"):
        raise ValueError("Source and destination prefixes must not contain one another")


def summarize(plan: Sequence[CopyEntry]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for entry in plan:
        values = summary.setdefault(entry.table, {"objects": 0, "physical_bytes": 0})
        values["objects"] += 1
        values["physical_bytes"] += entry.physical_bytes
    return summary


def destination_state(
    fs: pafs.FileSystem,
    destination_root: str,
    plan: Sequence[CopyEntry],
    resume: bool,
) -> tuple[list[CopyEntry], int]:
    manifest_path = PurePosixPath(destination_root, MANIFEST_NAME).as_posix()
    existing = {item.path: item for item in list_files(fs, destination_root) if item.path != manifest_path}
    expected = {entry.destination: entry for entry in plan}
    unexpected = set(existing) - set(expected)
    if unexpected:
        sample = ", ".join(sorted(unexpected)[:3])
        raise FileExistsError(f"Destination contains unexpected objects: {sample}")
    if existing and not resume:
        raise FileExistsError("Destination is not empty; use --resume to validate and continue")

    pending = []
    for path, entry in expected.items():
        current = existing.get(path)
        if current is None:
            pending.append(entry)
        elif current.size != entry.physical_bytes:
            raise RuntimeError(f"Destination size differs for {path}: {current.size} != {entry.physical_bytes}")
    return pending, len(plan) - len(pending)


def copy_plan(fs: pafs.FileSystem, entries: Sequence[CopyEntry], workers: int) -> None:
    if not entries:
        return
    if isinstance(fs, pafs.LocalFileSystem):
        for parent in {str(PurePosixPath(entry.destination).parent) for entry in entries}:
            fs.create_dir(parent, recursive=True)
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fs.copy_file, entry.source, entry.destination): entry for entry in entries}
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % 100 == 0 or completed == len(entries):
                print(f"Copied {completed}/{len(entries)} objects", flush=True)


def validate_destination(fs: pafs.FileSystem, destination_root: str, plan: Sequence[CopyEntry]) -> None:
    manifest_path = PurePosixPath(destination_root, MANIFEST_NAME).as_posix()
    actual = {item.path: item.size for item in list_files(fs, destination_root) if item.path != manifest_path}
    expected = {entry.destination: entry.physical_bytes for entry in plan}
    if actual != expected:
        missing = set(expected) - set(actual)
        unexpected = set(actual) - set(expected)
        mismatched = {path for path in set(actual) & set(expected) if actual[path] != expected[path]}
        raise RuntimeError(
            "Destination validation failed: "
            f"missing={len(missing)}, unexpected={len(unexpected)}, size_mismatches={len(mismatched)}"
        )


def write_manifest(
    fs: pafs.FileSystem,
    destination_root: str,
    source: str,
    destination: str,
    plan: Sequence[CopyEntry],
) -> None:
    manifest = {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "destination": destination,
        "summary": summarize(plan),
        "entries": [asdict(entry) for entry in plan],
    }
    path = PurePosixPath(destination_root, MANIFEST_NAME).as_posix()
    with fs.open_output_stream(path) as stream:
        stream.write((json.dumps(manifest, indent=2) + "\n").encode())


def run(args: argparse.Namespace) -> dict[str, dict[str, int]]:
    source = DestinationLocation.parse(args.source)
    destination = DestinationLocation.parse(args.destination)
    validate_locations(source, destination)
    fs = filesystem(source, args.s3_region)
    plan = build_plan(source.root, list_files(fs, source.root), destination.root)
    summary = summarize(plan)
    print(json.dumps(summary, indent=2), flush=True)
    pending, completed = destination_state(fs, destination.root, plan, args.resume)
    print(f"Plan: total={len(plan)}, existing={completed}, pending={len(pending)}", flush=True)
    if args.dry_run:
        return summary

    copy_plan(fs, pending, args.workers)
    validate_destination(fs, destination.root, plan)
    write_manifest(fs, destination.root, args.source, args.destination, plan)
    print(f"Validated {len(plan)} byte-identical objects", flush=True)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source filesystem path, file URI, or S3 URI")
    parser.add_argument("--destination", required=True, help="Destination filesystem path, file URI, or S3 URI")
    parser.add_argument("--s3-region")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be positive")
    return args


def main() -> None:
    try:
        run(parse_args())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
