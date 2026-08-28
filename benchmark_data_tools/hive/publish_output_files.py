#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Publish generated files to a filesystem path or S3 prefix."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlparse

import pyarrow.fs as pafs

MIB = 1024 * 1024


@dataclass(frozen=True)
class DestinationLocation:
    scheme: str
    root: str

    @classmethod
    def parse(cls, value: str | Path) -> DestinationLocation:
        text = str(value)
        parsed = urlparse(text)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError(f"S3 destination has no bucket: {text}")
            prefix = parsed.path.strip("/")
            return cls("s3", parsed.netloc + (f"/{prefix}" if prefix else ""))
        if parsed.scheme and parsed.scheme != "file":
            raise ValueError(f"Unsupported destination scheme: {parsed.scheme}")
        local = Path(parsed.path if parsed.scheme == "file" else text).expanduser().resolve()
        return cls("file", str(local))

    def join(self, relative: str | PurePosixPath) -> str:
        clean = PurePosixPath(relative)
        if clean.is_absolute() or ".." in clean.parts:
            raise ValueError(f"Destination path must be relative: {relative}")
        if self.scheme == "s3":
            return f"s3://{self.root.rstrip('/')}/{clean.as_posix()}"
        return str(Path(self.root).joinpath(*clean.parts))


class OutputDestination(Protocol):
    location: DestinationLocation

    def prepare(self, overwrite: bool) -> None: ...

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]: ...


class FileSystemDestination:
    """A local or network-mounted filesystem destination."""

    def __init__(self, location: DestinationLocation):
        if location.scheme != "file":
            raise ValueError("FileSystemDestination requires a file URI")
        self.location = location
        self.root = Path(location.root)

    def prepare(self, overwrite: bool) -> None:
        if not self.root.exists():
            return
        if not overwrite:
            raise FileExistsError(f"Destination exists; use --overwrite: {self.root}")
        shutil.rmtree(self.root)

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]:
        output = Path(self.location.join(relative))
        if output.exists():
            raise FileExistsError(f"Refusing to overwrite output: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp")
        temporary.unlink(missing_ok=True)
        shutil.copy2(local_path, temporary)
        os.replace(temporary, output)
        return {"path": relative, "physical_bytes": output.stat().st_size}


class S3Destination:
    def __init__(self, location: DestinationLocation, region: str | None = None):
        if location.scheme != "s3":
            raise ValueError("S3Destination requires an s3 URI")
        self.location = location
        self.region = region

    @staticmethod
    def _path(uri: str) -> str:
        parsed = urlparse(uri)
        return f"{parsed.netloc}/{parsed.path.lstrip('/')}"

    def _filesystem(self, uri: str) -> pafs.S3FileSystem:
        bucket = urlparse(uri).netloc
        region = self.region or pafs.resolve_s3_region(bucket)
        return pafs.S3FileSystem(region=region)

    def _files(self) -> list[pafs.FileInfo]:
        root = f"s3://{self.location.root.rstrip('/')}/"
        filesystem = self._filesystem(root)
        selector = pafs.FileSelector(self._path(root).rstrip("/"), recursive=True, allow_not_found=True)
        return [info for info in filesystem.get_file_info(selector) if info.type == pafs.FileType.File]

    def prepare(self, overwrite: bool) -> None:
        files = self._files()
        if not files:
            return
        root = f"s3://{self.location.root.rstrip('/')}/"
        if not overwrite:
            raise FileExistsError(f"S3 destination is not empty; use --overwrite: {root}")
        filesystem = self._filesystem(root)
        for info in files:
            filesystem.delete_file(info.path)

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]:
        target_uri = self.location.join(relative)
        filesystem = self._filesystem(target_uri)
        target_path = self._path(target_uri)
        if filesystem.get_file_info(target_path).type != pafs.FileType.NotFound:
            raise FileExistsError(f"Refusing to overwrite S3 object: {target_uri}")
        with local_path.open("rb") as source, filesystem.open_output_stream(target_path) as output:
            shutil.copyfileobj(source, output, length=8 * MIB)
        expected_size = local_path.stat().st_size
        actual_size = filesystem.get_file_info(target_path).size
        if actual_size != expected_size:
            raise RuntimeError(f"S3 upload size differs for {target_uri}: {actual_size} != {expected_size}")
        return {"path": relative, "physical_bytes": actual_size}


def make_destination(location: DestinationLocation, s3_region: str | None = None) -> OutputDestination:
    if location.scheme == "file":
        return FileSystemDestination(location)
    return S3Destination(location, region=s3_region)
