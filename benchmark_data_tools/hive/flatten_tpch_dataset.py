#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Copy a partitioned TPC-H dataset into an unpartitioned, byte-identical layout."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Protocol, Sequence
from urllib.parse import quote, urlparse

PARTITION_COLUMNS = {
    "orders": "o_ordermonth",
    "lineitem": "l_shipmonth",
}
FLAT_TABLES = ("customer", "part", "partsupp", "supplier", "nation", "region")
TABLES = tuple(PARTITION_COLUMNS) + FLAT_TABLES
MANIFEST_NAME = "_flatten_manifest.json"


@dataclass(frozen=True)
class Location:
    scheme: str
    root: str

    @classmethod
    def parse(cls, value: str | Path) -> "Location":
        text = str(value)
        parsed = urlparse(text)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError(f"S3 location has no bucket: {text}")
            prefix = parsed.path.strip("/")
            return cls("s3", parsed.netloc + (f"/{prefix}" if prefix else ""))
        if parsed.scheme and parsed.scheme != "file":
            raise ValueError(f"Unsupported location scheme: {parsed.scheme}")
        local = Path(parsed.path if parsed.scheme == "file" else text).expanduser().resolve()
        return cls("file", str(local))


@dataclass(frozen=True)
class ObjectInfo:
    path: str
    size: int


class Storage(Protocol):
    def list_files(self, root: str) -> list[ObjectInfo]: ...

    def copy_file(self, source: str, destination: str) -> None: ...

    def write_bytes(self, path: str, contents: bytes) -> None: ...


class LocalStorage:
    def list_files(self, root: str) -> list[ObjectInfo]:
        path = Path(root)
        if not path.exists():
            return []
        return [ObjectInfo(str(item), item.stat().st_size) for item in sorted(path.rglob("*")) if item.is_file()]

    def copy_file(self, source: str, destination: str) -> None:
        output = Path(destination)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp")
        temporary.unlink(missing_ok=True)
        shutil.copy2(source, temporary)
        os.replace(temporary, output)

    def write_bytes(self, path: str, contents: bytes) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp")
        temporary.write_bytes(contents)
        os.replace(temporary, output)


class AwsCliS3Storage:
    def __init__(self, region: str | None):
        self.region = region

    def command(self, *args: str, input_bytes: bytes | None = None) -> bytes:
        command = ["aws"]
        if self.region:
            command.extend(["--region", self.region])
        command.extend(args)
        return subprocess.run(command, input=input_bytes, check=True, capture_output=True).stdout

    def list_files(self, root: str) -> list[ObjectInfo]:
        bucket, prefix = root.split("/", 1)
        output = self.command(
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix.rstrip("/") + "/",
            "--output",
            "json",
        )
        document = json.loads(output)
        return sorted(
            (ObjectInfo(item["Key"], item["Size"]) for item in document.get("Contents", [])),
            key=lambda item: item.path,
        )

    def copy_file(self, source: str, destination: str) -> None:
        source_bucket, source_key = source.split("/", 1)
        destination_bucket, destination_key = destination.split("/", 1)
        self.command(
            "s3api",
            "copy-object",
            "--bucket",
            destination_bucket,
            "--copy-source",
            quote(f"{source_bucket}/{source_key}", safe="/"),
            "--key",
            destination_key,
            "--metadata-directive",
            "COPY",
        )

    def write_bytes(self, path: str, contents: bytes) -> None:
        self.command("s3", "cp", "-", f"s3://{path}", "--only-show-errors", input_bytes=contents)


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


def filesystem(location: Location, region: str | None) -> Storage:
    if location.scheme == "file":
        return LocalStorage()
    return AwsCliS3Storage(region)


def build_plan(source_root: str, source_files: Sequence[ObjectInfo], destination_root: str) -> list[CopyEntry]:
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


def validate_locations(source: Location, destination: Location) -> None:
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
    storage: Storage,
    destination_root: str,
    plan: Sequence[CopyEntry],
    resume: bool,
) -> tuple[list[CopyEntry], int]:
    manifest_path = PurePosixPath(destination_root, MANIFEST_NAME).as_posix()
    existing = {item.path: item for item in storage.list_files(destination_root) if item.path != manifest_path}
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


def write_progress(path: Path | None, **values: object) -> None:
    if path is None:
        return
    document = {"updated_at": datetime.now(timezone.utc).isoformat(), **values}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n")
    os.replace(temporary, path)


def copy_plan(
    storage: Storage,
    entries: Sequence[CopyEntry],
    workers: int,
    progress_path: Path | None,
    completed_files: int,
    completed_bytes: int,
    total_files: int,
    total_bytes: int,
) -> None:
    if not entries:
        return
    lock = threading.Lock()
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(storage.copy_file, entry.source, entry.destination): entry for entry in entries}
        for future in as_completed(futures):
            future.result()
            entry = futures[future]
            with lock:
                completed_files += 1
                completed_bytes += entry.physical_bytes
                elapsed = max(time.monotonic() - started, 0.001)
                rate = (completed_bytes / elapsed) if completed_bytes else 0
                eta = ((total_bytes - completed_bytes) / rate) if rate else None
                write_progress(
                    progress_path,
                    status="copying",
                    current_file=entry.destination,
                    files_processed=completed_files,
                    total_files=total_files,
                    bytes_processed=completed_bytes,
                    total_bytes=total_bytes,
                    gigabytes_processed=completed_bytes / 1_000_000_000,
                    throughput_bytes_per_second=rate,
                    eta_seconds=eta,
                )
                if completed_files % 100 == 0 or completed_files == total_files:
                    print(f"Copied {completed_files}/{total_files} objects", flush=True)


def validate_destination(storage: Storage, destination_root: str, plan: Sequence[CopyEntry]) -> None:
    manifest_path = PurePosixPath(destination_root, MANIFEST_NAME).as_posix()
    actual = {item.path: item.size for item in storage.list_files(destination_root) if item.path != manifest_path}
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
    storage: Storage,
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
    storage.write_bytes(path, (json.dumps(manifest, indent=2) + "\n").encode())


def run(args: argparse.Namespace) -> dict[str, dict[str, int]]:
    source = Location.parse(args.source)
    destination = Location.parse(args.destination)
    validate_locations(source, destination)
    storage = filesystem(source, args.s3_region)
    progress_path = Path(args.progress).expanduser().resolve() if args.progress else None
    write_progress(progress_path, status="listing_source", files_processed=0, bytes_processed=0)
    plan = build_plan(source.root, storage.list_files(source.root), destination.root)
    summary = summarize(plan)
    print(json.dumps(summary, indent=2), flush=True)
    pending, completed = destination_state(storage, destination.root, plan, args.resume)
    completed_bytes = sum(entry.physical_bytes for entry in plan if entry not in pending)
    total_bytes = sum(entry.physical_bytes for entry in plan)
    print(f"Plan: total={len(plan)}, existing={completed}, pending={len(pending)}", flush=True)
    if args.dry_run:
        write_progress(
            progress_path,
            status="dry_run_complete",
            files_processed=completed,
            total_files=len(plan),
            bytes_processed=completed_bytes,
            total_bytes=total_bytes,
        )
        return summary

    copy_plan(
        storage,
        pending,
        args.workers,
        progress_path,
        completed,
        completed_bytes,
        len(plan),
        total_bytes,
    )
    write_progress(progress_path, status="validating", files_processed=len(plan), bytes_processed=total_bytes)
    validate_destination(storage, destination.root, plan)
    write_manifest(storage, destination.root, args.source, args.destination, plan)
    write_progress(
        progress_path,
        status="complete",
        current_file=None,
        files_processed=len(plan),
        total_files=len(plan),
        bytes_processed=total_bytes,
        total_bytes=total_bytes,
        gigabytes_processed=total_bytes / 1_000_000_000,
        eta_seconds=0,
    )
    print(f"Validated {len(plan)} byte-identical objects", flush=True)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source filesystem path, file URI, or S3 URI")
    parser.add_argument("--destination", required=True, help="Destination filesystem path, file URI, or S3 URI")
    parser.add_argument("--s3-region")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--progress", help="Write atomic JSON progress updates to this local path")
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
