#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Rewrite TPC-H into size-calibrated, key-sorted Parquet.

The algorithm is deliberately scale-factor independent:

1. Read one source table in bounded pylibcudf chunks, sort every chunk by the
   complete clustering key, and write local sorted runs.
2. Plan broad date windows from run footer statistics.
3. Predicate-read every run for one window, enforce the GPU budget, globally
   sort once, and slice month-local output candidates.
4. Rewrite local candidates until normal files are within the configured size
   tolerance, learning rows per MiB from each accepted file.
5. Split large non-date tables with the same feedback loop and explicit
   primary-key sorting, while leaving only tiny tables unchanged.
6. Publish each validated file through the same destination interface for
   local or S3.

PyArrow is used only for Parquet metadata, schemas, and small scalar/boundary
results. It is never used to filter or route source rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Protocol, Sequence
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

MIB = 1024 * 1024
MANIFEST_VERSION = 2
PARTITION_TABLES = ("orders", "lineitem")
FLAT_SPLIT_SPECS = {
    "customer": ("c_custkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "supplier": ("s_suppkey",),
}
FLAT_SPLIT_TABLES = tuple(FLAT_SPLIT_SPECS)
COPY_TABLES = ("nation", "region")
ALL_TABLES = (*PARTITION_TABLES, *FLAT_SPLIT_TABLES, *COPY_TABLES)
GPU_WORKING_SET_MULTIPLIER = 3
GPU_PLANNING_SAFETY_PERCENT = 25
DEFAULT_FILE_SIZE_TOLERANCE = 0.05
DEFAULT_MAX_SIZE_RETRIES = 6


@dataclass(frozen=True)
class TableSpec:
    date_column: str
    partition_column: str
    sort_columns: tuple[str, ...]


TABLE_SPECS = {
    "orders": TableSpec("o_orderdate", "o_ordermonth", ("o_orderdate", "o_orderkey")),
    "lineitem": TableSpec(
        "l_shipdate",
        "l_shipmonth",
        ("l_shipdate", "l_orderkey", "l_linenumber"),
    ),
}


@dataclass(frozen=True)
class DateEstimate:
    day: date
    rows: int
    uncompressed_bytes: int


@dataclass(frozen=True)
class OutputRange:
    range_id: str
    partition_value: str
    minimum_date: str
    maximum_date: str
    estimated_rows: int
    estimated_compressed_bytes: int
    estimated_uncompressed_bytes: int
    size_exception: str | None = None


@dataclass(frozen=True)
class WorkWindow:
    window_id: str
    minimum_date: str
    maximum_date: str
    range_ids: tuple[str, ...]
    estimated_rows: int
    estimated_uncompressed_bytes: int


@dataclass
class SizeCalibration:
    target_bytes: int
    accepted_rows: int = 0
    accepted_bytes: int = 0
    seed_rows_per_byte: float | None = None

    @property
    def rows_per_byte(self) -> float:
        if self.accepted_bytes:
            return self.accepted_rows / self.accepted_bytes
        if self.seed_rows_per_byte and self.seed_rows_per_byte > 0:
            return self.seed_rows_per_byte
        raise ValueError("Size calibration has no observations")

    @property
    def rows_per_mib(self) -> float:
        return self.rows_per_byte * MIB

    def predicted_rows(self) -> int:
        return max(1, round(self.target_bytes * self.rows_per_byte))

    def adjusted_rows(self, candidate_rows: int, candidate_bytes: int) -> int:
        if candidate_rows <= 0 or candidate_bytes <= 0:
            raise ValueError("Candidate rows and bytes must be positive")
        return max(1, round(candidate_rows * self.target_bytes / candidate_bytes))

    def accept(self, rows: int, physical_bytes: int) -> None:
        if rows <= 0 or physical_bytes <= 0:
            raise ValueError("Accepted rows and bytes must be positive")
        self.accepted_rows += rows
        self.accepted_bytes += physical_bytes


@dataclass(frozen=True)
class DestinationURI:
    scheme: str
    root: str

    @classmethod
    def parse(cls, value: str | Path) -> DestinationURI:
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


class Destination(Protocol):
    uri: DestinationURI

    def prepare(self, resume: bool, overwrite: bool) -> None: ...

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]: ...

    def validate(self, relative: str, expected: dict[str, Any]) -> bool: ...


class LocalDestination:
    def __init__(self, uri: DestinationURI):
        if uri.scheme != "file":
            raise ValueError("LocalDestination requires a file URI")
        self.uri = uri
        self.root = Path(uri.root)

    def prepare(self, resume: bool, overwrite: bool) -> None:
        if resume:
            return
        if self.root.exists():
            if not overwrite:
                raise FileExistsError(f"Destination exists; use --resume or --overwrite: {self.root}")
            shutil.rmtree(self.root)

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]:
        output = Path(self.uri.join(relative))
        if output.exists():
            raise FileExistsError(f"Refusing to overwrite unmanifested output: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp")
        if temporary.exists():
            temporary.unlink()
        shutil.copy2(local_path, temporary)
        os.replace(temporary, output)
        return {"path": relative, "physical_bytes": output.stat().st_size}

    def validate(self, relative: str, expected: dict[str, Any]) -> bool:
        path = Path(self.uri.join(relative))
        if not path.is_file() or path.stat().st_size != expected["physical_bytes"]:
            return False
        if "footer_digest" not in expected:
            return True
        current = parquet_footer_facts(path, relative)
        return (
            current["rows"] == expected["rows"]
            and current["schema"] == expected["schema"]
            and current["footer_digest"] == expected["footer_digest"]
        )


Uploader = Callable[[Path, str], dict[str, Any] | None]
HeadObject = Callable[[str], dict[str, Any] | None]


class S3Destination:
    def __init__(
        self,
        uri: DestinationURI,
        uploader: Uploader | None = None,
        head_object: HeadObject | None = None,
        prefix_has_objects: Callable[[str], bool] | None = None,
        delete_prefix: Callable[[str], None] | None = None,
        region: str | None = None,
    ):
        if uri.scheme != "s3":
            raise ValueError("S3Destination requires an s3 URI")
        self.uri = uri
        self._region = region
        self._uploader = uploader or self._aws_upload
        self._head_object = head_object or self._aws_head
        self._prefix_has_objects = prefix_has_objects or self._aws_prefix_has_objects
        self._delete_prefix = delete_prefix or self._aws_delete_prefix

    @staticmethod
    def _s3_path(uri: str) -> str:
        parsed = urlparse(uri)
        return f"{parsed.netloc}/{parsed.path.lstrip('/')}"

    def _filesystem(self, uri: str) -> pafs.S3FileSystem:
        bucket = urlparse(uri).netloc
        region = self._region or pafs.resolve_s3_region(bucket)
        return pafs.S3FileSystem(region=region)

    def _aws_upload(self, local_path: Path, uri: str) -> dict[str, Any] | None:
        filesystem = self._filesystem(uri)
        target = S3Destination._s3_path(uri)
        with local_path.open("rb") as source, filesystem.open_output_stream(target) as output:
            shutil.copyfileobj(source, output, length=8 * MIB)
        return {"ContentLength": filesystem.get_file_info(target).size}

    def _aws_head(self, uri: str) -> dict[str, Any] | None:
        info = self._filesystem(uri).get_file_info(S3Destination._s3_path(uri))
        if info.type == pafs.FileType.NotFound:
            return None
        if info.type != pafs.FileType.File:
            return None
        return {"ContentLength": info.size}

    def _aws_prefix_has_objects(self, uri: str) -> bool:
        selector = pafs.FileSelector(
            S3Destination._s3_path(uri).rstrip("/"),
            recursive=True,
            allow_not_found=True,
        )
        return any(info.type == pafs.FileType.File for info in self._filesystem(uri).get_file_info(selector))

    def _aws_delete_prefix(self, uri: str) -> None:
        filesystem = self._filesystem(uri)
        selector = pafs.FileSelector(
            S3Destination._s3_path(uri).rstrip("/"),
            recursive=True,
            allow_not_found=True,
        )
        for info in filesystem.get_file_info(selector):
            if info.type == pafs.FileType.File:
                filesystem.delete_file(info.path)

    def prepare(self, resume: bool, overwrite: bool) -> None:
        if resume:
            return
        root = f"s3://{self.uri.root.rstrip('/')}/"
        if not self._prefix_has_objects(root):
            return
        if not overwrite:
            raise FileExistsError(f"S3 destination is not empty; use --overwrite explicitly: {root}")
        self._delete_prefix(root)

    def publish_file(self, local_path: Path, relative: str) -> dict[str, Any]:
        target = self.uri.join(relative)
        if self._head_object(target) is not None:
            raise FileExistsError(f"Refusing to overwrite unmanifested S3 object: {target}")
        metadata = self._uploader(local_path, target) or {}
        return {"path": relative, "physical_bytes": local_path.stat().st_size, "object_metadata": metadata}

    def validate(self, relative: str, expected: dict[str, Any]) -> bool:
        metadata = self._head_object(self.uri.join(relative))
        if metadata is None:
            return False
        return int(metadata.get("ContentLength", -1)) == expected["physical_bytes"]


def destination_for(
    uri: DestinationURI,
    uploader: Uploader | None = None,
    s3_region: str | None = None,
) -> Destination:
    if uri.scheme == "file":
        return LocalDestination(uri)
    return S3Destination(uri, uploader=uploader, region=s3_region)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def month_string(value: date) -> str:
    return value.strftime("%Y-%m")


def hive_partition_path(table: str, partition_column: str, partition_value: str, file_index: int) -> str:
    return (
        PurePosixPath(table)
        .joinpath(f"{partition_column}={partition_value}", f"part-{file_index:05d}.parquet")
        .as_posix()
    )


def parquet_files(table_dir: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in table_dir.rglob("*.parquet") if path.is_file()))


def schema_json(schema: pa.Schema) -> list[dict[str, Any]]:
    fields = []
    for field in schema:
        entry: dict[str, Any] = {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        if pa.types.is_decimal(field.type):
            entry.update(precision=field.type.precision, scale=field.type.scale)
        fields.append(entry)
    return fields


def metadata_digest(value: Any) -> str:
    """Hash compact metadata, never Parquet content."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def normalize_stat(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Decimal):
        return str(value)
    return value


def parquet_footer_facts(path: Path, relative: str) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    row_groups = []
    for row_group_index in range(metadata.num_row_groups):
        group = metadata.row_group(row_group_index)
        columns = []
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            statistics = column.statistics
            columns.append(
                {
                    "path": column.path_in_schema,
                    "compressed_bytes": column.total_compressed_size,
                    "uncompressed_bytes": column.total_uncompressed_size,
                    "null_count": statistics.null_count if statistics and statistics.has_null_count else None,
                    "minimum": normalize_stat(statistics.min) if statistics and statistics.has_min_max else None,
                    "maximum": normalize_stat(statistics.max) if statistics and statistics.has_min_max else None,
                }
            )
        row_groups.append({"rows": group.num_rows, "bytes": group.total_byte_size, "columns": columns})
    facts = {
        "path": relative,
        "physical_bytes": path.stat().st_size,
        "rows": metadata.num_rows,
        "row_groups": row_groups,
        "schema": schema_json(parquet_file.schema_arrow),
        "format_version": str(metadata.format_version),
    }
    facts["footer_digest"] = metadata_digest(facts)
    return facts


def source_identity(
    source: Path,
    source_manifest: Path | None = None,
    source_identity_value: str | None = None,
    tables: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Build a cheap identity from supplied metadata and top-level footer facts."""
    selected = tuple(tables or ALL_TABLES)
    files = []
    for table in sorted(selected):
        table_dir = source / table
        if not table_dir.is_dir():
            continue
        for path in parquet_files(table_dir):
            parquet_file = pq.ParquetFile(path)
            files.append(
                {
                    "path": path.relative_to(source).as_posix(),
                    "physical_bytes": path.stat().st_size,
                    "rows": parquet_file.metadata.num_rows,
                    "row_groups": parquet_file.metadata.num_row_groups,
                    "schema_digest": metadata_digest(schema_json(parquet_file.schema_arrow)),
                }
            )
    supplied_manifest = None
    if source_manifest:
        supplied_manifest = {
            "name": source_manifest.name,
            "metadata_digest": metadata_digest(json.loads(source_manifest.read_text())),
        }
    identity = {
        "identity": source_identity_value,
        "source_manifest": supplied_manifest,
        "files": files,
    }
    identity["metadata_digest"] = metadata_digest(identity)
    return identity


def inspect_schema(files: Sequence[Path]) -> pa.Schema:
    if not files:
        raise ValueError("No Parquet source files found")
    schema = pq.ParquetFile(files[0]).schema_arrow
    for path in files[1:]:
        candidate = pq.ParquetFile(path).schema_arrow
        if not candidate.equals(schema, check_metadata=True):
            raise ValueError(f"Source schema differs from {files[0]}: {path}")
    return schema


def _stat_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    if isinstance(value, int):
        return date(1970, 1, 1) + timedelta(days=value)
    raise TypeError(f"Unsupported Parquet date statistic: {value!r}")


def date_estimates_from_footers(run_facts: Sequence[dict[str, Any]], date_column: str) -> list[DateEstimate]:
    """Distribute each row group's rows/bytes across its date-stat span."""
    rows: Counter[date] = Counter()
    byte_counts: Counter[date] = Counter()
    for run in run_facts:
        for row_group in run["row_groups"]:
            column = next((item for item in row_group["columns"] if item["path"] == date_column), None)
            if not column or column["minimum"] is None or column["maximum"] is None:
                raise ValueError(f"Missing {date_column} row-group statistics in {run['path']}")
            lower = _stat_date(column["minimum"])
            upper = _stat_date(column["maximum"])
            day_count = (upper - lower).days + 1
            for offset in range(day_count):
                day = lower + timedelta(days=offset)
                rows[day] += row_group["rows"] // day_count + (offset < row_group["rows"] % day_count)
                byte_counts[day] += row_group["bytes"] // day_count + (offset < row_group["bytes"] % day_count)
    return [DateEstimate(day, rows[day], byte_counts[day]) for day in sorted(rows)]


def sorted_run_compression_ratio(run_facts: Sequence[dict[str, Any]]) -> float:
    physical_bytes = sum(run["physical_bytes"] for run in run_facts)
    uncompressed_bytes = sum(row_group["bytes"] for run in run_facts for row_group in run["row_groups"])
    if physical_bytes <= 0 or uncompressed_bytes <= 0:
        raise ValueError("Sorted-run compression calibration must be positive")
    return physical_bytes / uncompressed_bytes


def sorted_run_rows_per_byte(run_facts: Sequence[dict[str, Any]]) -> float:
    rows = sum(run["rows"] for run in run_facts)
    physical_bytes = sum(run["physical_bytes"] for run in run_facts)
    if rows <= 0 or physical_bytes <= 0:
        raise ValueError("Sorted-run row-size calibration must be positive")
    return rows / physical_bytes


def build_output_ranges(
    estimates: Sequence[DateEstimate],
    compressed_bytes_per_uncompressed_byte: float,
    target_file_bytes: int,
) -> list[OutputRange]:
    """Group whole dates into deterministic month-local target files."""
    if target_file_bytes <= 0 or compressed_bytes_per_uncompressed_byte <= 0:
        raise ValueError("Target bytes and compression estimate must be positive")
    ranges: list[OutputRange] = []
    current: list[DateEstimate] = []

    def flush() -> None:
        if not current:
            return
        uncompressed = sum(item.uncompressed_bytes for item in current)
        compressed = round(uncompressed * compressed_bytes_per_uncompressed_byte)
        ranges.append(
            OutputRange(
                range_id=f"range-{len(ranges):05d}",
                partition_value=month_string(current[0].day),
                minimum_date=current[0].day.isoformat(),
                maximum_date=current[-1].day.isoformat(),
                estimated_rows=sum(item.rows for item in current),
                estimated_compressed_bytes=compressed,
                estimated_uncompressed_bytes=uncompressed,
                size_exception="indivisible-day" if len(current) == 1 and compressed > target_file_bytes else None,
            )
        )
        current.clear()

    for estimate in sorted(estimates, key=lambda item: item.day):
        if estimate.rows <= 0 or estimate.uncompressed_bytes <= 0:
            raise ValueError(f"Invalid estimate for {estimate.day}")
        candidate_bytes = round(
            (sum(item.uncompressed_bytes for item in current) + estimate.uncompressed_bytes)
            * compressed_bytes_per_uncompressed_byte
        )
        month_changed = bool(current) and month_string(estimate.day) != month_string(current[0].day)
        if current and (month_changed or candidate_bytes > target_file_bytes):
            flush()
        current.append(estimate)
    flush()
    return ranges


def build_work_windows(ranges: Sequence[OutputRange], gpu_window_bytes: int) -> list[WorkWindow]:
    """Pack complete Hive months into broad, possibly cross-month windows."""
    if gpu_window_bytes <= 0:
        raise ValueError("gpu_window_bytes must be positive")
    windows: list[WorkWindow] = []
    current: list[OutputRange] = []
    partition_groups: list[list[OutputRange]] = []
    partition_group: list[OutputRange] = []

    for output_range in ranges:
        if partition_group and partition_group[-1].partition_value != output_range.partition_value:
            partition_groups.append(partition_group)
            partition_group = []
        partition_group.append(output_range)
    if partition_group:
        partition_groups.append(partition_group)

    def flush() -> None:
        if not current:
            return
        windows.append(
            WorkWindow(
                window_id=f"window-{len(windows):05d}",
                minimum_date=current[0].minimum_date,
                maximum_date=current[-1].maximum_date,
                range_ids=tuple(item.range_id for item in current),
                estimated_rows=sum(item.estimated_rows for item in current),
                estimated_uncompressed_bytes=sum(item.estimated_uncompressed_bytes for item in current),
            )
        )
        current.clear()

    for group in partition_groups:
        group_bytes = sum(item.estimated_uncompressed_bytes for item in group)
        if group_bytes > gpu_window_bytes:
            raise ValueError(
                f"{group[0].partition_value} partition estimate exceeds GPU window budget; "
                "increase --gpu-window-bytes"
            )
        projected = sum(item.estimated_uncompressed_bytes for item in current) + group_bytes
        if current and projected > gpu_window_bytes:
            flush()
        current.extend(group)
    flush()
    return windows


def gpu_input_budget(gpu_memory_budget_bytes: int) -> int:
    """Reserve memory for concatenate, sort output, keys, and writer scratch."""
    budget = gpu_memory_budget_bytes // GPU_WORKING_SET_MULTIPLIER
    if budget <= 0:
        raise ValueError("GPU memory budget is too small")
    return budget


def gpu_planning_budget(gpu_memory_budget_bytes: int) -> int:
    """Leave estimation headroom while retaining a hard measured-input limit."""
    return gpu_input_budget(gpu_memory_budget_bytes) * GPU_PLANNING_SAFETY_PERCENT // 100


class Manifest:
    def __init__(self, path: Path, data: dict[str, Any]):
        self.path = path
        self.data = data

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=self.path.parent, delete=False) as stream:
            json.dump(self.data, stream, indent=2, sort_keys=True)
            stream.write("\n")
            temporary = Path(stream.name)
        os.replace(temporary, self.path)

    @classmethod
    def load(cls, path: Path) -> Manifest:
        return cls(path, json.loads(path.read_text()))


def footer_entry_is_valid(path: Path, entry: dict[str, Any], relative: str) -> bool:
    if not path.is_file() or path.stat().st_size != entry["physical_bytes"]:
        return False
    current = parquet_footer_facts(path, relative)
    return (
        current["rows"] == entry["rows"]
        and current["schema"] == entry["schema"]
        and current["footer_digest"] == entry["footer_digest"]
    )


def _table_input_metadata(gpu_table: Any, schema: pa.Schema) -> Any:
    import pylibcudf as plc

    metadata = plc.io.types.TableInputMetadata(gpu_table)
    for target, field in zip(metadata.column_metadata, schema, strict=True):
        target.set_name(field.name)
        target.set_nullability(field.nullable)
        if pa.types.is_decimal(field.type):
            target.set_decimal_precision(field.type.precision)
    return metadata


def write_gpu_parquet(path: Path, gpu_table: Any, schema: pa.Schema, row_group_rows: int) -> None:
    import pylibcudf as plc

    options = (
        plc.io.parquet.ParquetWriterOptions.builder(plc.io.SinkInfo([str(path)]), gpu_table)
        .metadata(_table_input_metadata(gpu_table, schema))
        .compression(plc.io.types.CompressionType.SNAPPY)
        .dictionary_policy(plc.io.types.DictionaryPolicy.ADAPTIVE)
        .stats_level(plc.io.types.StatisticsFreq.STATISTICS_ROWGROUP)
        .write_v2_headers(True)
        .build()
    )
    options.set_row_group_size_rows(max(1, row_group_rows))
    plc.io.parquet.write_parquet(options)


def sort_gpu_table(gpu_table: Any, schema: pa.Schema, sort_columns: Sequence[str]) -> Any:
    import pylibcudf as plc

    indices = [schema.names.index(name) for name in sort_columns]
    keys = plc.Table([gpu_table.columns()[index] for index in indices])
    return plc.sorting.sort_by_key(
        gpu_table,
        keys,
        [plc.types.Order.ASCENDING] * len(indices),
        [plc.types.NullOrder.AFTER] * len(indices),
    )


def source_row_group_batches(path: Path, chunk_bytes: int) -> list[tuple[int, ...]]:
    """Plan deterministic bounded batches without reading column data."""
    metadata = pq.ParquetFile(path).metadata
    batches: list[tuple[int, ...]] = []
    current: list[int] = []
    current_bytes = 0
    for row_group_index in range(metadata.num_row_groups):
        row_group_bytes = metadata.row_group(row_group_index).total_byte_size
        if current and current_bytes + row_group_bytes > chunk_bytes:
            batches.append(tuple(current))
            current = []
            current_bytes = 0
        current.append(row_group_index)
        current_bytes += row_group_bytes
    if current:
        batches.append(tuple(current))
    return batches


def read_source_row_groups(path: Path, row_groups: Sequence[int]):
    import pylibcudf as plc

    options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([str(path)])).build()
    options.set_row_groups([list(row_groups)])
    return plc.io.parquet.read_parquet(options).tbl


def gpu_table_nbytes(table: Any) -> int:
    """Return allocated column bytes; this API needs validation against the pinned image."""
    return sum(column.device_buffer_size() for column in table.columns())


def _date_filter(schema: pa.Schema, date_column: str, lower: date, upper: date) -> Any:
    import pylibcudf as plc

    column = plc.expressions.ColumnReference(schema.names.index(date_column))
    lower_literal = plc.expressions.Literal(plc.Scalar.from_arrow(pa.scalar(lower, type=pa.date32())))
    upper_literal = plc.expressions.Literal(plc.Scalar.from_arrow(pa.scalar(upper, type=pa.date32())))
    ge = plc.expressions.Operation(plc.expressions.ASTOperator.GREATER_EQUAL, column, lower_literal)
    le = plc.expressions.Operation(plc.expressions.ASTOperator.LESS_EQUAL, column, upper_literal)
    return plc.expressions.Operation(plc.expressions.ASTOperator.LOGICAL_AND, ge, le)


def predicate_read_run(path: Path, schema: pa.Schema, date_column: str, lower: date, upper: date) -> Any:
    import pylibcudf as plc

    filter_expression = _date_filter(schema, date_column, lower, upper)
    options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([str(path)])).build()
    # The Parquet predicate prunes row groups but can return boundary rows
    # outside the requested interval. Apply the exact row predicate on GPU.
    options.set_filter(filter_expression)
    table = plc.io.parquet.read_parquet(options).tbl
    dates = table.columns()[schema.names.index(date_column)]
    bool_type = plc.DataType(plc.TypeId.BOOL8)
    lower_mask = plc.binaryop.binary_operation(
        dates,
        plc.Scalar.from_arrow(pa.scalar(lower, type=pa.date32())),
        plc.binaryop.BinaryOperator.GREATER_EQUAL,
        bool_type,
    )
    upper_mask = plc.binaryop.binary_operation(
        dates,
        plc.Scalar.from_arrow(pa.scalar(upper, type=pa.date32())),
        plc.binaryop.BinaryOperator.LESS_EQUAL,
        bool_type,
    )
    mask = plc.binaryop.binary_operation(
        lower_mask,
        upper_mask,
        plc.binaryop.BinaryOperator.LOGICAL_AND,
        bool_type,
    )
    return plc.stream_compaction.apply_boolean_mask(table, mask)


def concatenate_gpu_tables(tables: Sequence[Any]) -> Any:
    import pylibcudf as plc

    return plc.concatenate.concatenate(list(tables))


def date_boundary_index(
    table: Any,
    schema: pa.Schema,
    date_column: str,
    boundary: date,
    upper: bool,
) -> int:
    """Find a scalar date boundary on GPU and copy only the one-element index."""
    import pylibcudf as plc

    date_table = plc.Table([table.columns()[schema.names.index(date_column)]])
    needle = plc.Table([plc.Column.from_arrow(pa.array([boundary], type=pa.date32()))])
    function = plc.search.upper_bound if upper else plc.search.lower_bound
    result = function(
        date_table,
        needle,
        [plc.types.Order.ASCENDING],
        [plc.types.NullOrder.AFTER],
    )
    return int(result.to_arrow()[0].as_py())


def key_boundary_index(
    table: Any,
    schema: pa.Schema,
    key_column: str,
    index: int,
) -> int:
    """Move a prospective file cut to the start of its leading-key group."""
    import pylibcudf as plc

    column = table.columns()[schema.names.index(key_column)]
    key_table = plc.Table([column])
    needle = plc.copying.slice(key_table, [index, index + 1])[0]
    result = plc.search.lower_bound(
        key_table,
        needle,
        [plc.types.Order.ASCENDING],
        [plc.types.NullOrder.AFTER],
    )
    return int(result.to_arrow()[0].as_py())


def slice_gpu_table(table: Any, start: int, stop: int) -> Any:
    import pylibcudf as plc

    return plc.copying.slice(table, [start, stop])[0]


def gpu_date_at(table: Any, schema: pa.Schema, date_column: str, index: int) -> date:
    import pylibcudf as plc

    column = table.columns()[schema.names.index(date_column)]
    one = plc.copying.slice(plc.Table([column]), [index, index + 1])[0]
    value = one.columns()[0].to_arrow()[0].as_py()
    return _stat_date(value)


def month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def stage_table(args: Any, manifest: Manifest, table: str, schema: pa.Schema, source_files: Sequence[Path]) -> None:
    spec = TABLE_SPECS[table]
    table_manifest = manifest.data["tables"][table]
    runs = table_manifest["runs"]
    stage_runs = args.staging / table / "runs"
    stage_runs.mkdir(parents=True, exist_ok=True)
    uncompressed_per_row = table_manifest["source_statistics"]["uncompressed_bytes_per_row"]
    row_group_rows = max(1, round(args.row_group_bytes / uncompressed_per_row))
    expected_run_ids = set()

    for source_index, source_file in enumerate(source_files):
        source_relative = source_file.relative_to(args.source).as_posix()
        for batch_index, row_groups in enumerate(source_row_group_batches(source_file, args.gpu_read_chunk_bytes)):
            run_id = f"source-{source_index:05d}-batch-{batch_index:05d}"
            expected_run_ids.add(run_id)
            output = stage_runs / f"{run_id}.parquet"
            existing = runs.get(run_id)
            if existing is not None:
                source_groups = existing.get("source_row_groups", existing.get("row_groups"))
                if existing["source_path"] != source_relative or tuple(source_groups) != row_groups:
                    raise RuntimeError(f"Staging run plan changed: {run_id}")
                if not footer_entry_is_valid(output, existing, output.relative_to(args.staging).as_posix()):
                    raise RuntimeError(f"Manifest-listed staging run is missing or changed: {output}")
                continue

            chunk = read_source_row_groups(source_file, row_groups)
            sorted_chunk = sort_gpu_table(chunk, schema, spec.sort_columns)
            temporary = output.with_suffix(".parquet.tmp")
            write_gpu_parquet(temporary, sorted_chunk, schema, row_group_rows)
            os.replace(temporary, output)
            facts = parquet_footer_facts(output, output.relative_to(args.staging).as_posix())
            facts.update(
                run_id=run_id,
                source_path=source_relative,
                source_row_groups=list(row_groups),
                oversized_source_row_group=(
                    len(row_groups) == 1
                    and pq.ParquetFile(source_file).metadata.row_group(row_groups[0]).total_byte_size
                    > args.gpu_read_chunk_bytes
                ),
            )
            runs[run_id] = facts
            table_manifest["state"] = "staging"
            manifest.save()
    unexpected = sorted(set(runs) - expected_run_ids)
    if unexpected:
        raise RuntimeError(f"Manifest contains unexpected staging runs for {table}: {unexpected[:5]}")


def source_table_statistics(files: Sequence[Path]) -> dict[str, Any]:
    rows = compressed = uncompressed = 0
    for path in files:
        metadata = pq.ParquetFile(path).metadata
        rows += metadata.num_rows
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            uncompressed += group.total_byte_size
            compressed += sum(group.column(i).total_compressed_size for i in range(group.num_columns))
    if rows == 0 or uncompressed == 0:
        raise ValueError("Source table is empty")
    return {
        "rows": rows,
        "compressed_bytes": compressed,
        "uncompressed_bytes": uncompressed,
        "compressed_bytes_per_uncompressed_byte": compressed / uncompressed,
        "uncompressed_bytes_per_row": uncompressed / rows,
    }


def initialize_table_manifest(
    args: Any,
    manifest: Manifest,
    table: str,
    schema: pa.Schema,
    source_files: Sequence[Path],
) -> None:
    if table in manifest.data["tables"]:
        return
    manifest.data["tables"][table] = {
        "state": "planned",
        "schema": schema_json(schema),
        "date_column": TABLE_SPECS[table].date_column,
        "sort_columns": list(TABLE_SPECS[table].sort_columns),
        "source_files": [path.relative_to(args.source).as_posix() for path in source_files],
        "source_statistics": source_table_statistics(source_files),
        "runs": {},
        "ranges": [],
        "windows": [],
        "files": [],
    }
    manifest.save()


def plan_staged_table(args: Any, manifest: Manifest, table: str) -> tuple[list[OutputRange], list[WorkWindow]]:
    table_manifest = manifest.data["tables"][table]
    if table_manifest["ranges"]:
        ranges = [OutputRange(**item) for item in table_manifest["ranges"]]
        if not table_manifest["windows"]:
            table_manifest["windows"] = [
                asdict(item) for item in build_work_windows(ranges, gpu_planning_budget(args.gpu_window_bytes))
            ]
            manifest.save()
        return (
            ranges,
            [WorkWindow(**{**item, "range_ids": tuple(item["range_ids"])}) for item in table_manifest["windows"]],
        )
    run_facts = [
        parquet_footer_facts(args.staging / table_manifest["runs"][key]["path"], table_manifest["runs"][key]["path"])
        for key in sorted(table_manifest["runs"])
    ]
    compression_ratio = sorted_run_compression_ratio(run_facts)
    table_manifest["size_calibration"] = {
        "source": "sorted-runs",
        "rows": sum(run["rows"] for run in run_facts),
        "physical_bytes": sum(run["physical_bytes"] for run in run_facts),
        "rows_per_mib": sorted_run_rows_per_byte(run_facts) * MIB,
        "compressed_bytes_per_uncompressed_byte": compression_ratio,
    }
    estimates = date_estimates_from_footers(run_facts, TABLE_SPECS[table].date_column)
    ranges = build_output_ranges(
        estimates,
        compression_ratio,
        args.target_file_bytes,
    )
    windows = build_work_windows(ranges, gpu_planning_budget(args.gpu_window_bytes))
    table_manifest["ranges"] = [asdict(item) for item in ranges]
    table_manifest["windows"] = [asdict(item) for item in windows]
    table_manifest["state"] = "staged"
    manifest.save()
    return ranges, windows


def _row_group_endpoint_key(
    parquet_file: pq.ParquetFile,
    row_group_index: int,
    sort_columns: Sequence[str],
    first: bool,
) -> list[Any]:
    table = parquet_file.read_row_group(row_group_index, columns=list(sort_columns))
    index = 0 if first else table.num_rows - 1
    return [normalize_stat(table.column(column)[index].as_py()) for column in sort_columns]


def validate_output_file(
    path: Path,
    relative: str,
    schema: pa.Schema,
    spec: TableSpec,
    output_range: OutputRange,
    expected_rows: int,
) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
        raise RuntimeError(f"Output schema differs from source: {path}")
    if parquet_file.metadata.num_rows != expected_rows:
        raise RuntimeError(
            f"Output row count differs from sorted slice: {path}: {parquet_file.metadata.num_rows} != {expected_rows}"
        )

    date_index = schema.names.index(spec.date_column)
    date_minima = []
    date_maxima = []
    previous_key = None
    key_minimum = None
    key_maximum = None
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        statistics = parquet_file.metadata.row_group(row_group_index).column(date_index).statistics
        if statistics is None or not statistics.has_min_max:
            raise RuntimeError(f"Missing output date statistics: {path}")
        date_minima.append(_stat_date(statistics.min))
        date_maxima.append(_stat_date(statistics.max))
        first_key = _row_group_endpoint_key(parquet_file, row_group_index, spec.sort_columns, first=True)
        last_key = _row_group_endpoint_key(parquet_file, row_group_index, spec.sort_columns, first=False)
        if first_key > last_key or (previous_key is not None and first_key < previous_key):
            raise RuntimeError(f"Output row-group key ranges are not ordered: {path}")
        key_minimum = key_minimum or first_key
        key_maximum = last_key
        previous_key = last_key

    actual_minimum = min(date_minima)
    actual_maximum = max(date_maxima)
    planned_minimum = date.fromisoformat(output_range.minimum_date)
    planned_maximum = date.fromisoformat(output_range.maximum_date)
    if actual_minimum < planned_minimum or actual_maximum > planned_maximum:
        raise RuntimeError(f"Output dates escape planned range: {path}")
    if (
        month_string(actual_minimum) != output_range.partition_value
        or month_string(actual_maximum) != output_range.partition_value
    ):
        raise RuntimeError(f"Output dates disagree with Hive partition: {path}")

    facts = parquet_footer_facts(path, relative)
    facts.update(
        range_id=output_range.range_id,
        partition_value=output_range.partition_value,
        date_min=actual_minimum.isoformat(),
        date_max=actual_maximum.isoformat(),
        key_min=key_minimum,
        key_max=key_maximum,
        estimated_rows=output_range.estimated_rows,
        estimated_compressed_bytes=output_range.estimated_compressed_bytes,
        estimated_uncompressed_bytes=output_range.estimated_uncompressed_bytes,
        size_exception=output_range.size_exception,
    )
    return facts


def validate_completed_table(destination: Destination, table_manifest: dict[str, Any], table: str) -> None:
    if table_manifest.get("state") != "complete":
        raise RuntimeError(f"Table is not complete: {table}")
    for entry in table_manifest["files"]:
        if not destination.validate(entry["path"], entry):
            raise RuntimeError(f"Completed output is missing or changed: {entry['path']}")
    expected_rows = table_manifest["source_statistics"]["rows"]
    if sum(item["rows"] for item in table_manifest["files"]) != expected_rows:
        raise RuntimeError(f"Completed output row count differs from source for {table}")


def finalize_table(
    args: Any,
    destination: Destination,
    manifest: Manifest,
    table: str,
    schema: pa.Schema,
    _ranges: Sequence[OutputRange],
    windows: Sequence[WorkWindow],
) -> None:
    spec = TABLE_SPECS[table]
    table_manifest = manifest.data["tables"][table]
    runs = [args.staging / table / "runs" / f"{key}.parquet" for key in sorted(table_manifest["runs"])]
    completed_files = table_manifest["files"]
    for entry in completed_files:
        if not destination.validate(entry["path"], entry):
            raise RuntimeError(f"Manifest-listed output is missing or changed: {entry['path']}")
    partition_indices: Counter[str] = Counter()
    previous_maximum = None
    completed_rows_by_window: Counter[str] = Counter()
    calibration_data = table_manifest["size_calibration"]
    calibration = SizeCalibration(
        target_bytes=args.target_file_bytes,
        accepted_rows=calibration_data.get("accepted_rows", 0),
        accepted_bytes=calibration_data.get("accepted_bytes", 0),
        seed_rows_per_byte=calibration_data["rows_per_mib"] / MIB,
    )
    for entry in completed_files:
        if "window_id" not in entry:
            raise RuntimeError("Completed output predates measured-size generation and cannot be resumed")
        if previous_maximum is not None and entry["key_min"] <= previous_maximum:
            raise RuntimeError(f"Manifest-listed output ranges overlap at {entry['path']}")
        previous_maximum = entry["key_max"]
        partition_indices[entry["partition_value"]] += 1
        completed_rows_by_window[entry["window_id"]] += entry["rows"]
    row_group_rows = max(
        1,
        round(args.row_group_bytes / table_manifest["source_statistics"]["uncompressed_bytes_per_row"]),
    )
    table_manifest["state"] = "finalizing"
    manifest.save()
    input_budget = gpu_input_budget(args.gpu_window_bytes)

    for window_index, window in enumerate(windows):
        lower = date.fromisoformat(window.minimum_date)
        upper = date.fromisoformat(window.maximum_date)
        pieces = []
        allocated = 0
        for run in runs:
            piece = predicate_read_run(run, schema, spec.date_column, lower, upper)
            if piece.num_rows() == 0:
                continue
            piece_bytes = gpu_table_nbytes(piece)
            if allocated + piece_bytes > input_budget:
                raise RuntimeError(
                    f"{table} {window.window_id} needs at least {allocated + piece_bytes} GPU bytes; "
                    f"safe input budget is {input_budget} from total budget {args.gpu_window_bytes}"
                )
            pieces.append(piece)
            allocated += piece_bytes
        if not pieces:
            raise RuntimeError(f"No rows found for {table} {window.window_id}")
        merged = concatenate_gpu_tables(pieces)
        pieces.clear()
        del pieces
        del piece
        if gpu_table_nbytes(merged) > input_budget:
            raise RuntimeError(f"{table} {window.window_id} exceeds GPU budget before global sort")
        globally_sorted = sort_gpu_table(merged, schema, spec.sort_columns)
        del merged
        cursor = completed_rows_by_window[window.window_id]
        if cursor > globally_sorted.num_rows():
            raise RuntimeError(f"Completed rows exceed regenerated window {window.window_id}")

        while cursor < globally_sorted.num_rows():
            first_day = gpu_date_at(globally_sorted, schema, spec.date_column, cursor)
            partition_value = month_string(first_day)
            calendar_partition_end = month_end(first_day)
            partition_upper = min(calendar_partition_end, upper)
            partition_continues = (
                window_index + 1 < len(windows)
                and month_string(date.fromisoformat(windows[window_index + 1].minimum_date)) == partition_value
            )
            partition_stop = date_boundary_index(
                globally_sorted,
                schema,
                spec.date_column,
                partition_upper,
                upper=True,
            )
            remaining_rows = partition_stop - cursor
            if remaining_rows <= 0:
                raise RuntimeError(f"No remaining rows in {table} partition {partition_value}")
            predicted_remaining_bytes = remaining_rows / calibration.rows_per_byte
            candidate_rows = (
                remaining_rows
                if predicted_remaining_bytes <= args.target_file_bytes * (1 + args.file_size_tolerance)
                else min(remaining_rows, calibration.predicted_rows())
            )
            relative = hive_partition_path(
                table,
                spec.partition_column,
                partition_value,
                partition_indices[partition_value],
            )
            local_output = args.staging / table / "output" / relative
            local_output.parent.mkdir(parents=True, exist_ok=True)
            temporary = local_output.with_suffix(".parquet.tmp")
            size_exception = None
            attempts = 0
            while True:
                attempts += 1
                output_table = slice_gpu_table(globally_sorted, cursor, cursor + candidate_rows)
                temporary.unlink(missing_ok=True)
                write_gpu_parquet(temporary, output_table, schema, row_group_rows)
                physical_bytes = temporary.stat().st_size
                lower_size = args.target_file_bytes * (1 - args.file_size_tolerance)
                upper_size = args.target_file_bytes * (1 + args.file_size_tolerance)
                if lower_size <= physical_bytes <= upper_size:
                    break
                if not partition_continues and candidate_rows == remaining_rows and physical_bytes < lower_size:
                    size_exception = "partition-tail"
                    break
                if attempts >= args.max_size_retries:
                    raise RuntimeError(
                        f"{relative} remains outside size tolerance after {attempts} writes: {physical_bytes} bytes"
                    )
                adjusted_rows = min(
                    remaining_rows,
                    calibration.adjusted_rows(candidate_rows, physical_bytes),
                )
                if adjusted_rows == candidate_rows:
                    adjusted_rows += -1 if physical_bytes > upper_size else 1
                    adjusted_rows = max(1, min(remaining_rows, adjusted_rows))
                candidate_rows = adjusted_rows
                del output_table
            os.replace(temporary, local_output)
            last_day = gpu_date_at(
                globally_sorted,
                schema,
                spec.date_column,
                cursor + candidate_rows - 1,
            )
            output_range = OutputRange(
                range_id=f"file-{len(completed_files):05d}",
                partition_value=partition_value,
                minimum_date=first_day.isoformat(),
                maximum_date=last_day.isoformat(),
                estimated_rows=candidate_rows,
                estimated_compressed_bytes=round(candidate_rows / calibration.rows_per_byte),
                estimated_uncompressed_bytes=round(
                    candidate_rows * table_manifest["source_statistics"]["uncompressed_bytes_per_row"]
                ),
                size_exception=size_exception,
            )
            details = validate_output_file(
                local_output,
                relative,
                schema,
                spec,
                output_range,
                candidate_rows,
            )
            if previous_maximum is not None and details["key_min"] <= previous_maximum:
                raise RuntimeError(f"Adjacent output ranges overlap before {relative}")
            details.update(
                window_id=window.window_id,
                size_attempts=attempts,
                observed_rows_per_mib=candidate_rows * MIB / details["physical_bytes"],
                target_file_bytes=args.target_file_bytes,
                file_size_tolerance=args.file_size_tolerance,
            )
            details.update(destination.publish_file(local_output, relative))
            if not destination.validate(relative, details):
                raise RuntimeError(f"Published output failed validation: {relative}")
            table_manifest["files"].append(details)
            previous_maximum = details["key_max"]
            partition_indices[partition_value] += 1
            completed_rows_by_window[window.window_id] += candidate_rows
            calibration.accept(candidate_rows, details["physical_bytes"])
            calibration_data.update(
                accepted_rows=calibration.accepted_rows,
                accepted_bytes=calibration.accepted_bytes,
                rows_per_mib=calibration.rows_per_mib,
            )
            manifest.save()
            local_output.unlink()
            del output_table
            cursor += candidate_rows
        del globally_sorted

    expected_rows = table_manifest["source_statistics"]["rows"]
    if sum(item["rows"] for item in table_manifest["files"]) != expected_rows:
        raise RuntimeError(f"Output row count differs from source for {table}")
    table_manifest["state"] = "complete"
    manifest.save()
    shutil.rmtree(args.staging / table / "runs", ignore_errors=True)


def ordered_source_row_groups(
    source_files: Sequence[Path],
    sort_columns: Sequence[str],
) -> list[tuple[Path, int]]:
    keyed_groups = []
    for path in source_files:
        parquet_file = pq.ParquetFile(path)
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            first = _row_group_endpoint_key(
                parquet_file,
                row_group_index,
                sort_columns,
                first=True,
            )
            last = _row_group_endpoint_key(
                parquet_file,
                row_group_index,
                sort_columns,
                first=False,
            )
            if first > last:
                raise RuntimeError(f"Source row group is not ordered by {sort_columns}: {path}")
            keyed_groups.append((first, last, path, row_group_index))
    keyed_groups.sort(key=lambda item: item[0])
    previous = None
    for first, last, path, _ in keyed_groups:
        if previous is not None and first <= previous:
            raise RuntimeError(f"Source key ranges overlap before {path}")
        previous = last
    return [(path, row_group_index) for _, _, path, row_group_index in keyed_groups]


def validate_flat_output_file(
    path: Path,
    relative: str,
    schema: pa.Schema,
    sort_columns: Sequence[str],
    expected_rows: int,
) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
        raise RuntimeError(f"Flat output schema differs from source: {path}")
    if parquet_file.metadata.num_rows != expected_rows:
        raise RuntimeError(f"Flat output row count differs from source slice: {path}")
    previous = None
    key_minimum = None
    key_maximum = None
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        first = _row_group_endpoint_key(parquet_file, row_group_index, sort_columns, first=True)
        last = _row_group_endpoint_key(parquet_file, row_group_index, sort_columns, first=False)
        if first > last or (previous is not None and first <= previous):
            raise RuntimeError(f"Flat output key ranges are not strictly ordered: {path}")
        key_minimum = key_minimum or first
        key_maximum = last
        previous = last
    facts = parquet_footer_facts(path, relative)
    facts.update(key_min=key_minimum, key_max=key_maximum)
    return facts


def rewrite_flat_table(
    args: Any,
    destination: Destination,
    manifest: Manifest,
    table: str,
) -> None:
    source_files = parquet_files(args.source / table)
    schema = inspect_schema(source_files)
    sort_columns = FLAT_SPLIT_SPECS[table]
    groups = ordered_source_row_groups(source_files, sort_columns)
    flat_tables = manifest.data.setdefault("flat_tables", {})
    table_manifest = flat_tables.setdefault(
        table,
        {
            "state": "running",
            "schema": schema_json(schema),
            "sort_columns": list(sort_columns),
            "source_statistics": source_table_statistics(source_files),
            "files": [],
            "size_calibration": {},
        },
    )
    if table_manifest["state"] == "complete":
        validate_completed_table(destination, table_manifest, table)
        return
    for entry in table_manifest["files"]:
        if not destination.validate(entry["path"], entry):
            raise RuntimeError(f"Manifest-listed flat output is missing or changed: {entry['path']}")

    statistics = table_manifest["source_statistics"]
    calibration_data = table_manifest["size_calibration"]
    calibration = SizeCalibration(
        target_bytes=args.target_file_bytes,
        accepted_rows=calibration_data.get("accepted_rows", 0),
        accepted_bytes=calibration_data.get("accepted_bytes", 0),
        seed_rows_per_byte=statistics["rows"] / statistics["compressed_bytes"],
    )
    rows_to_skip = sum(entry["rows"] for entry in table_manifest["files"])
    previous_maximum = table_manifest["files"][-1]["key_max"] if table_manifest["files"] else None
    row_group_rows = max(1, round(args.row_group_bytes / statistics["uncompressed_bytes_per_row"]))
    input_budget = gpu_input_budget(args.gpu_window_bytes)
    group_index = 0
    buffer = None

    def load_one(current: Any | None) -> Any | None:
        nonlocal group_index, rows_to_skip
        while group_index < len(groups):
            path, row_group_index = groups[group_index]
            group_index += 1
            piece = read_source_row_groups(path, (row_group_index,))
            if rows_to_skip >= piece.num_rows():
                rows_to_skip -= piece.num_rows()
                continue
            if rows_to_skip:
                piece = slice_gpu_table(piece, rows_to_skip, piece.num_rows())
                rows_to_skip = 0
            combined = piece if current is None else concatenate_gpu_tables((current, piece))
            if gpu_table_nbytes(combined) > input_budget:
                raise RuntimeError(f"{table} flat-output buffer exceeds GPU input budget")
            return combined
        return current

    while group_index < len(groups) or buffer is not None:
        if buffer is None:
            buffer = load_one(None)
            if buffer is None:
                break
        predicted_rows = calibration.predicted_rows()
        while buffer.num_rows() < predicted_rows and group_index < len(groups):
            buffer = load_one(buffer)
        available_rows = buffer.num_rows()
        exhausted = group_index == len(groups)
        predicted_remaining_bytes = available_rows / calibration.rows_per_byte
        candidate_rows = (
            available_rows
            if exhausted and predicted_remaining_bytes <= args.target_file_bytes * (1 + args.file_size_tolerance)
            else min(available_rows, predicted_rows)
        )
        file_index = len(table_manifest["files"])
        relative = f"{table}/part-{file_index:05d}.parquet"
        local_output = args.staging / table / "flat-output" / relative
        local_output.parent.mkdir(parents=True, exist_ok=True)
        temporary = local_output.with_suffix(".parquet.tmp")
        attempts = 0
        size_exception = None
        while True:
            attempts += 1
            if len(sort_columns) > 1 and candidate_rows < available_rows:
                aligned_rows = key_boundary_index(
                    buffer,
                    schema,
                    sort_columns[0],
                    candidate_rows,
                )
                if aligned_rows <= 0:
                    raise RuntimeError(f"{table} leading-key group exceeds the flat-output buffer")
                candidate_rows = aligned_rows
            source_slice = slice_gpu_table(buffer, 0, candidate_rows)
            output_table = sort_gpu_table(source_slice, schema, sort_columns)
            temporary.unlink(missing_ok=True)
            write_gpu_parquet(temporary, output_table, schema, row_group_rows)
            physical_bytes = temporary.stat().st_size
            lower_size = args.target_file_bytes * (1 - args.file_size_tolerance)
            upper_size = args.target_file_bytes * (1 + args.file_size_tolerance)
            if lower_size <= physical_bytes <= upper_size:
                break
            if exhausted and candidate_rows == available_rows and physical_bytes < lower_size:
                size_exception = "table-tail"
                break
            if attempts >= args.max_size_retries:
                raise RuntimeError(
                    f"{relative} remains outside size tolerance after {attempts} writes: {physical_bytes} bytes"
                )
            adjusted_rows = calibration.adjusted_rows(candidate_rows, physical_bytes)
            while adjusted_rows > available_rows and group_index < len(groups):
                buffer = load_one(buffer)
                available_rows = buffer.num_rows()
            exhausted = group_index == len(groups)
            candidate_rows = min(available_rows, adjusted_rows)
            del source_slice
            del output_table

        os.replace(temporary, local_output)
        details = validate_flat_output_file(
            local_output,
            relative,
            schema,
            sort_columns,
            candidate_rows,
        )
        if previous_maximum is not None and details["key_min"] <= previous_maximum:
            raise RuntimeError(f"Adjacent flat outputs overlap before {relative}")
        details.update(
            file_id=f"file-{file_index:05d}",
            size_exception=size_exception,
            size_attempts=attempts,
            observed_rows_per_mib=candidate_rows * MIB / details["physical_bytes"],
            target_file_bytes=args.target_file_bytes,
            file_size_tolerance=args.file_size_tolerance,
        )
        details.update(destination.publish_file(local_output, relative))
        if not destination.validate(relative, details):
            raise RuntimeError(f"Published flat output failed validation: {relative}")
        table_manifest["files"].append(details)
        previous_maximum = details["key_max"]
        calibration.accept(candidate_rows, details["physical_bytes"])
        calibration_data.update(
            source="accepted-output-files",
            accepted_rows=calibration.accepted_rows,
            accepted_bytes=calibration.accepted_bytes,
            rows_per_mib=calibration.rows_per_mib,
        )
        manifest.save()
        local_output.unlink()
        buffer = slice_gpu_table(buffer, candidate_rows, available_rows) if candidate_rows < available_rows else None
        del source_slice
        del output_table

    if sum(entry["rows"] for entry in table_manifest["files"]) != statistics["rows"]:
        raise RuntimeError(f"Flat output row count differs from source for {table}")
    table_manifest["state"] = "complete"
    manifest.save()


def publish_copy_tables(args: Any, destination: Destination, manifest: Manifest) -> None:
    if manifest.data["copied_files"]:
        for entry in manifest.data["copied_files"]:
            if not destination.validate(entry["path"], entry):
                raise RuntimeError(f"Copied file is missing or changed: {entry['path']}")
        return
    for table in COPY_TABLES:
        source_table = args.source / table
        if not source_table.is_dir():
            raise FileNotFoundError(f"Missing source table: {source_table}")
        for source_file in sorted(path for path in source_table.rglob("*") if path.is_file()):
            relative = source_file.relative_to(args.source).as_posix()
            entry = destination.publish_file(source_file, relative)
            if not destination.validate(relative, entry):
                raise RuntimeError(f"Copied file failed validation: {relative}")
            manifest.data["copied_files"].append(entry)
            manifest.save()


def new_manifest(args: Any, identity: dict[str, Any], destination_uri: DestinationURI) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "state": "running",
        "started_at": utc_now(),
        "completed_at": None,
        "source": str(args.source),
        "destination": {"scheme": destination_uri.scheme, "root": destination_uri.root},
        "s3_region": args.s3_region,
        "source_identity": identity,
        "target_file_bytes": args.target_file_bytes,
        "file_size_tolerance": args.file_size_tolerance,
        "max_size_retries": args.max_size_retries,
        "row_group_bytes": args.row_group_bytes,
        "gpu_read_chunk_bytes": args.gpu_read_chunk_bytes,
        "gpu_window_bytes": args.gpu_window_bytes,
        "gpu_working_set_multiplier": GPU_WORKING_SET_MULTIPLIER,
        "gpu_planning_safety_percent": GPU_PLANNING_SAFETY_PERCENT,
        "rapids_image": args.rapids_image,
        "tables_requested": list(args.tables),
        "complete_dataset": set(args.tables) == set(PARTITION_TABLES),
        "copied_files": [],
        "tables": {},
        "flat_tables": {},
    }


def validate_resume(manifest: Manifest, args: Any, identity: dict[str, Any], uri: DestinationURI) -> None:
    expected = {
        "source": str(args.source),
        "destination": {"scheme": uri.scheme, "root": uri.root},
        "s3_region": args.s3_region,
        "source_identity": identity,
        "target_file_bytes": args.target_file_bytes,
        "file_size_tolerance": args.file_size_tolerance,
        "max_size_retries": args.max_size_retries,
        "row_group_bytes": args.row_group_bytes,
        "gpu_read_chunk_bytes": args.gpu_read_chunk_bytes,
        "gpu_window_bytes": args.gpu_window_bytes,
        "gpu_working_set_multiplier": GPU_WORKING_SET_MULTIPLIER,
        "gpu_planning_safety_percent": GPU_PLANNING_SAFETY_PERCENT,
        "tables_requested": list(args.tables),
    }
    for key, value in expected.items():
        if manifest.data.get(key) != value:
            raise ValueError(f"Resume configuration differs for {key}")


def prepare_manifest(args: Any, uri: DestinationURI, identity: dict[str, Any]) -> Manifest:
    manifest_path = args.manifest or args.staging / "rewrite_manifest.json"
    if args.resume:
        if not manifest_path.is_file():
            raise FileNotFoundError(f"--resume requires manifest: {manifest_path}")
        manifest = Manifest.load(manifest_path)
        validate_resume(manifest, args, identity, uri)
        return manifest
    if args.staging.exists():
        if not args.overwrite:
            raise FileExistsError(f"Staging exists; use --resume or --overwrite: {args.staging}")
        shutil.rmtree(args.staging)
    args.staging.mkdir(parents=True)
    return Manifest(manifest_path, new_manifest(args, identity, uri))


def publish_completion_files(args: Any, destination: Destination, manifest: Manifest) -> None:
    with tempfile.TemporaryDirectory(dir=args.staging) as temporary_dir:
        temporary = Path(temporary_dir)
        manifest_copy = temporary / "rewrite_manifest.json"
        manifest_copy.write_text(json.dumps(manifest.data, indent=2, sort_keys=True) + "\n")
        success = temporary / "_SUCCESS"
        success.write_bytes(b"")
        for local_path, relative in (
            (manifest_copy, "rewrite_manifest.json"),
            (success, "_SUCCESS"),
        ):
            expected = {"physical_bytes": local_path.stat().st_size}
            if destination.validate(relative, expected):
                continue
            destination.publish_file(local_path, relative)


def dry_run_plan(args: Any, identity: dict[str, Any]) -> None:
    result: dict[str, Any] = {"source_identity": identity, "tables": {}}
    for table in args.tables:
        files = parquet_files(args.source / table)
        statistics = source_table_statistics(files)
        facts = [parquet_footer_facts(path, path.relative_to(args.source).as_posix()) for path in files]
        try:
            estimates = date_estimates_from_footers(facts, TABLE_SPECS[table].date_column)
            ranges = build_output_ranges(
                estimates,
                statistics["compressed_bytes_per_uncompressed_byte"],
                args.target_file_bytes,
            )
            windows = build_work_windows(ranges, gpu_planning_budget(args.gpu_window_bytes))
            plan = {"ranges": [asdict(item) for item in ranges], "windows": [asdict(item) for item in windows]}
        except ValueError as error:
            plan = {"planning_note": f"Exact plan will be built from sorted-run statistics: {error}"}
        result["tables"][table] = {"source_statistics": statistics, **plan}
    print(json.dumps(result, indent=2, sort_keys=True))


def run(args: Any, uploader: Uploader | None = None) -> None:
    args.source = args.source.expanduser().resolve()
    args.staging = args.staging.expanduser().resolve()
    uri = DestinationURI.parse(args.destination)
    if not args.source.is_dir():
        raise FileNotFoundError(f"Source dataset does not exist: {args.source}")
    if uri.scheme == "file":
        destination_path = Path(uri.root)
        for left, right in (
            (destination_path, args.source),
            (destination_path, args.staging),
            (args.staging, args.source),
        ):
            if left == right or left in right.parents or right in left.parents:
                raise ValueError("Source, destination, and staging paths must be disjoint")
    identity = source_identity(
        args.source,
        source_manifest=args.source_manifest,
        source_identity_value=args.source_identity,
        tables=ALL_TABLES if set(args.tables) == set(PARTITION_TABLES) else args.tables,
    )
    if args.dry_run:
        dry_run_plan(args, identity)
        return

    destination = destination_for(uri, uploader=uploader, s3_region=args.s3_region)
    destination.prepare(resume=args.resume, overwrite=args.overwrite)
    manifest = prepare_manifest(args, uri, identity)
    manifest.save()
    for table in args.tables:
        source_files = parquet_files(args.source / table)
        schema = inspect_schema(source_files)
        initialize_table_manifest(args, manifest, table, schema, source_files)
        if manifest.data["tables"][table]["state"] == "complete":
            validate_completed_table(destination, manifest.data["tables"][table], table)
            continue
        stage_table(args, manifest, table, schema, source_files)
        ranges, windows = plan_staged_table(args, manifest, table)
        finalize_table(args, destination, manifest, table, schema, ranges, windows)

    complete_dataset = set(args.tables) == set(PARTITION_TABLES)
    if complete_dataset:
        for table in FLAT_SPLIT_TABLES:
            rewrite_flat_table(args, destination, manifest, table)
        publish_copy_tables(args, destination, manifest)
        if not all(manifest.data["tables"][table]["state"] == "complete" for table in PARTITION_TABLES):
            raise RuntimeError("Refusing to publish completion markers before all rewritten tables validate")
        if not all(manifest.data["flat_tables"][table]["state"] == "complete" for table in FLAT_SPLIT_TABLES):
            raise RuntimeError("Refusing to publish completion markers before flat tables validate")
        if manifest.data["state"] != "complete":
            manifest.data["state"] = "complete"
            manifest.data["completed_at"] = utc_now()
            manifest.save()
        publish_completion_files(args, destination, manifest)
    else:
        manifest.data["state"] = "partial"
        manifest.save()


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Local source dataset root")
    parser.add_argument("--destination", required=True, help="Local path, file:// URI, or s3:// URI")
    parser.add_argument(
        "--s3-region",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="S3 region; auto-resolved from the bucket when omitted",
    )
    parser.add_argument("--staging", type=Path, required=True, help="Local staging directory (prefer NVMe)")
    parser.add_argument("--target-file-bytes", type=int, default=512 * MIB)
    parser.add_argument("--file-size-tolerance", type=float, default=DEFAULT_FILE_SIZE_TOLERANCE)
    parser.add_argument("--max-size-retries", type=int, default=DEFAULT_MAX_SIZE_RETRIES)
    parser.add_argument("--row-group-bytes", type=int, default=128 * MIB)
    parser.add_argument("--gpu-read-chunk-bytes", type=int, default=2 * 1024 * MIB)
    parser.add_argument(
        "--gpu-window-bytes",
        type=int,
        default=48 * 1024 * MIB,
        help="Total GPU working-set budget; at most one third is planned input data",
    )
    parser.add_argument("--tables", nargs="+", choices=PARTITION_TABLES, default=list(PARTITION_TABLES))
    parser.add_argument("--resume", action="store_true", help="Resume manifest-verified runs and output files")
    parser.add_argument("--overwrite", action="store_true", help="Replace staging and local destination")
    parser.add_argument("--dry-run", action="store_true", help="Inspect metadata and print an estimated plan")
    parser.add_argument("--rapids-image", default="rapidsai/base:26.06-cuda13-py3.13")
    parser.add_argument("--source-identity", help="Stable generation/verification identity supplied by the caller")
    parser.add_argument("--source-manifest", type=Path, help="Existing generation or verification JSON manifest")
    parser.add_argument("--manifest", type=Path, help="Local atomic work manifest (default: staging root)")
    args = parser.parse_args(argv)
    if not 0 < args.file_size_tolerance < 1:
        parser.error("--file-size-tolerance must be between 0 and 1")
    if args.max_size_retries < 1:
        parser.error("--max-size-retries must be positive")
    if args.resume and args.overwrite:
        parser.error("--resume and --overwrite are mutually exclusive")
    for name in ("target_file_bytes", "row_group_bytes", "gpu_read_chunk_bytes", "gpu_window_bytes"):
        if getattr(args, name) <= 0:
            parser.error("size options must be positive")
    if args.row_group_bytes > args.gpu_window_bytes:
        parser.error("--row-group-bytes cannot exceed --gpu-window-bytes")
    return args


if __name__ == "__main__":
    run(parse_args())
