#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Rewrite TPC-H as sorted, approximately size-targeted Parquet files.

Orders and lineitem are Hive-partitioned by month. Large flat tables are split
without breaking leading-key groups, and nation and region are copied. Source
and staging are filesystem paths; the destination may be a filesystem path or
an S3 URI. Independent workers can share a destination using --work-spec:
partition-table assignments must cover disjoint whole-month ranges, flat-table
assignments must cover disjoint half-open row-group ranges, and each copy table
must have exactly one owner.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

plc = None

try:
    from .publish_output_files import DestinationLocation, OutputDestination, make_destination
except ImportError:
    from publish_output_files import DestinationLocation, OutputDestination, make_destination

MIB = 1024 * 1024
PARTITION_TABLES = ("orders", "lineitem")
FLAT_SPLIT_SPECS = {
    "customer": ("c_custkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "supplier": ("s_suppkey",),
}
FLAT_SPLIT_TABLES = tuple(FLAT_SPLIT_SPECS)
COPY_TABLES = ("nation", "region")
GPU_WORKING_SET_MULTIPLIER = 3
GPU_PLANNING_SAFETY_PERCENT = 25


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
class DateRange:
    partition_value: str
    minimum_date: date
    maximum_date: date
    estimated_rows: int
    estimated_uncompressed_bytes: int


@dataclass(frozen=True)
class PartitionWork:
    first_month: str
    last_month: str

    def bounds(self) -> tuple[date, date]:
        first = datetime.strptime(self.first_month, "%Y-%m").date().replace(day=1)
        last = datetime.strptime(self.last_month, "%Y-%m").date().replace(day=1)
        if first > last:
            raise ValueError(f"Partition month range is reversed: {self.first_month}..{self.last_month}")
        return first, last.replace(day=monthrange(last.year, last.month)[1])


@dataclass(frozen=True)
class FlatWork:
    first_row_group: int
    last_row_group: int


@dataclass(frozen=True)
class WorkSpec:
    worker_id: str
    partition_tables: dict[str, PartitionWork]
    flat_tables: dict[str, FlatWork]
    copy_tables: tuple[str, ...]


@dataclass
class SizeCalibration:
    target_bytes: int
    observed_rows: int
    observed_bytes: int

    @property
    def rows_per_byte(self) -> float:
        if self.observed_rows <= 0 or self.observed_bytes <= 0:
            raise ValueError("Size calibration must be positive")
        return self.observed_rows / self.observed_bytes

    def predicted_rows(self) -> int:
        return max(1, round(self.target_bytes * self.rows_per_byte))

    def observe(self, rows: int, physical_bytes: int) -> None:
        self.observed_rows += rows
        self.observed_bytes += physical_bytes


def month_string(value: date) -> str:
    return value.strftime("%Y-%m")


def partition_path(table: str, partition_column: str, partition_value: str, index: int) -> str:
    return (
        PurePosixPath(table).joinpath(f"{partition_column}={partition_value}", f"part-{index:05d}.parquet").as_posix()
    )


def parquet_files(table_dir: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in table_dir.rglob("*.parquet") if path.is_file()))


def inspect_schema(files: Sequence[Path]) -> pa.Schema:
    if not files:
        raise ValueError("No Parquet source files found")
    schema = pq.ParquetFile(files[0]).schema_arrow
    for path in files[1:]:
        candidate = pq.ParquetFile(path).schema_arrow
        if not candidate.equals(schema, check_metadata=True):
            raise ValueError(f"Source schema differs from {files[0]}: {path}")
    return schema


def table_statistics(files: Sequence[Path]) -> dict[str, float | int]:
    rows = compressed = uncompressed = 0
    for path in files:
        metadata = pq.ParquetFile(path).metadata
        rows += metadata.num_rows
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            uncompressed += group.total_byte_size
            compressed += sum(group.column(i).total_compressed_size for i in range(group.num_columns))
    if rows <= 0 or compressed <= 0 or uncompressed <= 0:
        raise ValueError("Source table is empty")
    return {
        "rows": rows,
        "compressed_bytes": compressed,
        "uncompressed_bytes": uncompressed,
        "uncompressed_bytes_per_row": uncompressed / rows,
    }


def load_codec_definitions(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    data = json.loads(path.read_text())
    if "tables" not in data:
        raise ValueError(f"Codec definitions must contain 'tables': {path}")
    result = {}
    for table in data["tables"]:
        if "name" not in table:
            raise ValueError(f"Codec table entry has no name: {path}")
        result[table["name"]] = table
    return result


def load_work_spec(path: Path) -> WorkSpec:
    data = json.loads(path.read_text())
    worker_id = data.get("worker_id")
    if not isinstance(worker_id, str) or not worker_id:
        raise ValueError("Work spec requires a non-empty worker_id")
    partition_data = data.get("partition_tables", {})
    flat_data = data.get("flat_tables", {})
    copy_tables = tuple(data.get("copy_tables", ()))
    unknown = (
        set(partition_data) - set(PARTITION_TABLES)
        | set(flat_data) - set(FLAT_SPLIT_TABLES)
        | set(copy_tables) - set(COPY_TABLES)
    )
    if unknown:
        raise ValueError(f"Unknown tables in work spec: {sorted(unknown)}")
    partition_tables = {
        table: PartitionWork(item["first_month"], item["last_month"])
        for table, item in partition_data.items()
    }
    for item in partition_tables.values():
        item.bounds()
    flat_tables = {
        table: FlatWork(int(item["first_row_group"]), int(item["last_row_group"]))
        for table, item in flat_data.items()
    }
    for table, item in flat_tables.items():
        if item.first_row_group < 0 or item.last_row_group <= item.first_row_group:
            raise ValueError(f"Invalid row-group range for {table}: {item}")
    if not partition_tables and not flat_tables and not copy_tables:
        raise ValueError("Work spec has no assigned work")
    if len(copy_tables) != len(set(copy_tables)):
        raise ValueError("Work spec contains duplicate copy tables")
    return WorkSpec(worker_id, partition_tables, flat_tables, copy_tables)


def _table_input_metadata(
    gpu_table: Any,
    schema: pa.Schema,
    codec: dict[str, Any],
) -> Any:
    metadata = plc.io.types.TableInputMetadata(gpu_table)
    overrides = {item["name"]: item for item in codec.get("columns", [])}
    for target, field in zip(metadata.column_metadata, schema, strict=True):
        target.set_name(field.name)
        target.set_nullability(field.nullable)
        if pa.types.is_decimal(field.type):
            target.set_decimal_precision(field.type.precision)
        override = overrides.get(field.name, {})
        encoding = override.get("encoding")
        if encoding:
            name = encoding.upper()
            if name == "RLE_DICTIONARY":
                raise ValueError("Use dictionary=true instead of the RLE_DICTIONARY encoding")
            target.set_encoding(getattr(plc.io.types.ColumnEncoding, name))
        elif override.get("dictionary") is False:
            target.set_encoding(plc.io.types.ColumnEncoding.PLAIN)
        if override.get("compressed") is False:
            target.set_skip_compression(True)
    return metadata


def write_gpu_parquet(
    path: Path,
    gpu_table: Any,
    schema: pa.Schema,
    row_group_rows: int,
    codec: dict[str, Any],
) -> None:
    compression_name = codec.get("compression", "SNAPPY").upper().split("(", 1)[0]
    if compression_name == "UNCOMPRESSED":
        compression_name = "NONE"
    compression = getattr(plc.io.types.CompressionType, compression_name)
    options = (
        plc.io.parquet.ParquetWriterOptions.builder(plc.io.SinkInfo([str(path)]), gpu_table)
        .metadata(_table_input_metadata(gpu_table, schema, codec))
        .compression(compression)
        .dictionary_policy(plc.io.types.DictionaryPolicy.ADAPTIVE)
        .stats_level(plc.io.types.StatisticsFreq.STATISTICS_ROWGROUP)
        .write_v2_headers(True)
        .build()
    )
    options.set_row_group_size_rows(max(1, row_group_rows))
    plc.io.parquet.write_parquet(options)


def validate_basic_output(path: Path, schema: pa.Schema, expected_rows: int) -> dict[str, int]:
    parquet_file = pq.ParquetFile(path)
    if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
        raise RuntimeError(f"Output schema differs from source: {path}")
    if parquet_file.metadata.num_rows != expected_rows:
        raise RuntimeError(f"Output row count differs: {path}")
    return {"rows": expected_rows, "physical_bytes": path.stat().st_size}


def scaled_candidate_rows(
    candidate_rows: int,
    physical_bytes: int,
    target_bytes: int,
    available_rows: int,
) -> int:
    """Scale a measured row count toward the physical byte target."""
    if candidate_rows <= 0 or physical_bytes <= 0 or target_bytes <= 0 or available_rows <= 0:
        raise ValueError("Measured sizing inputs must be positive")
    return max(
        1,
        min(available_rows, round(candidate_rows * target_bytes / physical_bytes)),
    )


def source_row_group_batches(path: Path, chunk_bytes: int) -> list[tuple[int, ...]]:
    metadata = pq.ParquetFile(path).metadata
    batches: list[tuple[int, ...]] = []
    current: list[int] = []
    current_bytes = 0
    for index in range(metadata.num_row_groups):
        group_bytes = metadata.row_group(index).total_byte_size
        if current and current_bytes + group_bytes > chunk_bytes:
            batches.append(tuple(current))
            current = []
            current_bytes = 0
        current.append(index)
        current_bytes += group_bytes
    if current:
        batches.append(tuple(current))
    return batches


def read_source_row_groups(
    path: Path,
    row_groups: Sequence[int],
    schema: pa.Schema | None = None,
    date_column: str | None = None,
    lower: date | None = None,
    upper: date | None = None,
) -> Any:
    options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([str(path)])).build()
    options.set_row_groups([list(row_groups)])
    if date_column is not None:
        if schema is None or lower is None or upper is None:
            raise ValueError("Date-filtered reads require schema and bounds")
    table = plc.io.parquet.read_parquet(options).tbl
    if date_column is None:
        return table
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


def sort_gpu_table(gpu_table: Any, schema: pa.Schema, sort_columns: Sequence[str]) -> Any:
    indices = [schema.names.index(name) for name in sort_columns]
    keys = plc.Table([gpu_table.columns()[index] for index in indices])
    return plc.sorting.sort_by_key(
        gpu_table,
        keys,
        [plc.types.Order.ASCENDING] * len(indices),
        [plc.types.NullOrder.AFTER] * len(indices),
    )


def gpu_input_budget(gpu_memory_bytes: int) -> int:
    return gpu_memory_bytes // GPU_WORKING_SET_MULTIPLIER


def gpu_planning_budget(gpu_memory_bytes: int) -> int:
    return gpu_input_budget(gpu_memory_bytes) * GPU_PLANNING_SAFETY_PERCENT // 100


def predicate_read_run(
    path: Path,
    schema: pa.Schema,
    date_column: str,
    lower: date,
    upper: date,
) -> Any:
    options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([str(path)])).build()
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


def stage_sorted_runs(
    args: Any,
    table: str,
    schema: pa.Schema,
    source_files: Sequence[Path],
    codec: dict[str, Any],
    work: PartitionWork | None = None,
) -> list[Path]:
    spec = TABLE_SPECS[table]
    statistics = table_statistics(source_files)
    row_group_rows = max(1, round(args.row_group_bytes / statistics["uncompressed_bytes_per_row"]))
    run_dir = args.staging / table / "runs"
    run_dir.mkdir(parents=True)
    runs = []
    bounds = work.bounds() if work is not None else None
    for source_index, source_file in enumerate(source_files):
        for batch_index, row_groups in enumerate(source_row_group_batches(source_file, args.gpu_read_chunk_bytes)):
            output = run_dir / f"source-{source_index:05d}-batch-{batch_index:05d}.parquet"
            chunk = (
                read_source_row_groups(
                    source_file,
                    row_groups,
                    schema,
                    spec.date_column,
                    *bounds,
                )
                if bounds is not None
                else read_source_row_groups(source_file, row_groups)
            )
            if chunk.num_rows() == 0:
                continue
            sorted_chunk = sort_gpu_table(chunk, schema, spec.sort_columns)
            write_gpu_parquet(output, sorted_chunk, schema, row_group_rows, codec)
            runs.append(output)
    return runs


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


def date_estimates(runs: Sequence[Path], date_column: str) -> tuple[list[DateEstimate], float]:
    rows: Counter[date] = Counter()
    byte_counts: Counter[date] = Counter()
    physical_bytes = uncompressed_bytes = 0
    for run in runs:
        parquet_file = pq.ParquetFile(run)
        physical_bytes += run.stat().st_size
        metadata = parquet_file.metadata
        date_index = parquet_file.schema_arrow.names.index(date_column)
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            statistics = group.column(date_index).statistics
            if statistics is None or not statistics.has_min_max:
                raise ValueError(f"Missing {date_column} row-group statistics in {run}")
            lower = _stat_date(statistics.min)
            upper = _stat_date(statistics.max)
            days = (upper - lower).days + 1
            uncompressed_bytes += group.total_byte_size
            for offset in range(days):
                day = lower + timedelta(days=offset)
                rows[day] += group.num_rows // days + (offset < group.num_rows % days)
                byte_counts[day] += group.total_byte_size // days + (offset < group.total_byte_size % days)
    if physical_bytes <= 0 or uncompressed_bytes <= 0:
        raise ValueError("Sorted-run calibration is empty")
    estimates = [DateEstimate(day, rows[day], byte_counts[day]) for day in sorted(rows)]
    return estimates, physical_bytes / uncompressed_bytes


def build_date_ranges(
    estimates: Sequence[DateEstimate],
    compression_ratio: float,
    target_file_bytes: int,
    planning_budget_bytes: int,
    file_size_tolerance: float = 0.05,
) -> list[DateRange]:
    """Build month-local ranges, flushing after reaching the size target."""
    ranges: list[DateRange] = []
    current: list[DateEstimate] = []

    def flush() -> None:
        if not current:
            return
        ranges.append(
            DateRange(
                partition_value=month_string(current[0].day),
                minimum_date=current[0].day,
                maximum_date=current[-1].day,
                estimated_rows=sum(item.rows for item in current),
                estimated_uncompressed_bytes=sum(item.uncompressed_bytes for item in current),
            )
        )
        current.clear()

    for estimate in estimates:
        month_changed = current and month_string(current[0].day) != month_string(estimate.day)
        projected_uncompressed = sum(item.uncompressed_bytes for item in current) + estimate.uncompressed_bytes
        if current and (month_changed or projected_uncompressed > planning_budget_bytes):
            flush()
        current_compressed = sum(item.uncompressed_bytes for item in current) * compression_ratio
        projected_compressed = projected_uncompressed * compression_ratio
        if (
            current
            and current_compressed >= target_file_bytes * (1 - file_size_tolerance)
            and projected_compressed > target_file_bytes * (1 + file_size_tolerance)
        ):
            flush()
        if estimate.uncompressed_bytes > planning_budget_bytes:
            raise ValueError(f"One day exceeds the GPU planning budget: {estimate.day}")
        current.append(estimate)
        projected_compressed = sum(item.uncompressed_bytes for item in current) * compression_ratio
        if projected_compressed >= target_file_bytes:
            flush()
    flush()
    return ranges


def rewrite_partition_table(
    args: Any,
    destination: OutputDestination,
    table: str,
    codecs: dict[str, dict[str, Any]],
    work: PartitionWork | None = None,
) -> None:
    source_files = parquet_files(args.source / table)
    schema = inspect_schema(source_files)
    statistics = table_statistics(source_files)
    codec = codecs.get(table, {})
    runs = stage_sorted_runs(args, table, schema, source_files, codec, work)
    if not runs:
        raise RuntimeError(f"No source rows found for assigned {table} months")
    assigned_statistics = table_statistics(runs)
    estimates, compression_ratio = date_estimates(runs, TABLE_SPECS[table].date_column)
    ranges = build_date_ranges(
        estimates,
        compression_ratio,
        args.target_file_bytes,
        gpu_planning_budget(args.gpu_memory_bytes),
        args.file_size_tolerance,
    )
    row_group_rows = max(1, round(args.row_group_bytes / statistics["uncompressed_bytes_per_row"]))
    spec = TABLE_SPECS[table]
    partition_indices: Counter[str] = Counter()
    output_rows = 0
    buffer = None
    calibration = SizeCalibration(
        args.target_file_bytes,
        assigned_statistics["rows"],
        sum(path.stat().st_size for path in runs),
    )
    has_measured_output = False
    minimum_file_bytes = round(args.target_file_bytes * (1 - args.file_size_tolerance))
    maximum_file_bytes = round(args.target_file_bytes * (1 + args.file_size_tolerance))

    for range_index, output_range in enumerate(ranges):
        pieces = [
            piece
            for run in runs
            if (
                piece := predicate_read_run(
                    run,
                    schema,
                    spec.date_column,
                    output_range.minimum_date,
                    output_range.maximum_date,
                )
            ).num_rows()
        ]
        if not pieces:
            raise RuntimeError(f"No rows found for {table} {output_range.minimum_date}")
        allocated = sum(column.device_buffer_size() for piece in pieces for column in piece.columns())
        if allocated > gpu_input_budget(args.gpu_memory_bytes):
            raise RuntimeError(f"{table} date range exceeds the GPU input budget")
        merged = plc.concatenate.concatenate(pieces)
        sorted_table = sort_gpu_table(merged, schema, spec.sort_columns)
        buffer = sorted_table if buffer is None else plc.concatenate.concatenate([buffer, sorted_table])
        partition_ends = (
            range_index + 1 == len(ranges)
            or ranges[range_index + 1].partition_value != output_range.partition_value
        )

        while buffer is not None:
            available_rows = buffer.num_rows()
            candidate_rows = min(available_rows, calibration.predicted_rows())
            if candidate_rows == available_rows and not partition_ends:
                break

            relative = partition_path(
                table,
                spec.partition_column,
                output_range.partition_value,
                partition_indices[output_range.partition_value],
            )
            local_output = args.staging / table / "output" / relative
            local_output.parent.mkdir(parents=True, exist_ok=True)
            attempted_rows: set[int] = set()
            details = None

            for _ in range(12):
                attempted_rows.add(candidate_rows)
                candidate = plc.copying.slice(buffer, [0, candidate_rows])[0]
                local_output.unlink(missing_ok=True)
                write_gpu_parquet(local_output, candidate, schema, row_group_rows, codec)
                measured = validate_basic_output(local_output, schema, candidate_rows)
                physical_bytes = measured["physical_bytes"]
                is_partition_tail = partition_ends and candidate_rows == available_rows

                if (
                    minimum_file_bytes <= physical_bytes <= maximum_file_bytes
                    or (is_partition_tail and physical_bytes <= maximum_file_bytes)
                ):
                    details = measured
                    break
                if physical_bytes < minimum_file_bytes and candidate_rows == available_rows:
                    if not partition_ends:
                        local_output.unlink()
                        break
                    details = measured
                    break

                next_rows = scaled_candidate_rows(
                    candidate_rows,
                    physical_bytes,
                    args.target_file_bytes,
                    available_rows,
                )
                if next_rows == candidate_rows:
                    next_rows += -1 if physical_bytes > maximum_file_bytes else 1
                    next_rows = max(1, min(available_rows, next_rows))
                if next_rows in attempted_rows:
                    raise RuntimeError(
                        f"Measured file sizing did not converge for {relative}: "
                        f"{candidate_rows} rows produced {physical_bytes} bytes"
                    )
                candidate_rows = next_rows

            if details is None:
                if local_output.exists():
                    local_output.unlink()
                if not partition_ends:
                    break
                raise RuntimeError(f"Measured file sizing did not converge for {relative}")

            details.update(destination.publish_file(local_output, relative))
            print(f"{relative}: {details['rows']} rows, {details['physical_bytes'] / MIB:.1f} MiB")
            if not has_measured_output:
                calibration = SizeCalibration(
                    args.target_file_bytes,
                    details["rows"],
                    details["physical_bytes"],
                )
                has_measured_output = True
            else:
                calibration.observe(details["rows"], details["physical_bytes"])
            output_rows += details["rows"]
            partition_indices[output_range.partition_value] += 1
            local_output.unlink()
            buffer = (
                plc.copying.slice(buffer, [candidate_rows, available_rows])[0]
                if candidate_rows < available_rows
                else None
            )

        if partition_ends and buffer is not None:
            raise RuntimeError(f"Failed to flush the {table} partition {output_range.partition_value}")

    if output_rows != assigned_statistics["rows"]:
        raise RuntimeError(f"Output row count differs from source for {table}")
    shutil.rmtree(args.staging / table / "runs")


def _row_group_endpoint_key(
    parquet_file: pq.ParquetFile,
    row_group_index: int,
    sort_columns: Sequence[str],
    first: bool,
) -> list[Any]:
    table = parquet_file.read_row_group(row_group_index, columns=list(sort_columns))
    index = 0 if first else table.num_rows - 1
    return [table.column(column)[index].as_py() for column in sort_columns]


def ordered_source_row_groups(
    source_files: Sequence[Path],
    sort_columns: Sequence[str],
) -> list[tuple[Path, int]]:
    keyed_groups = []
    for path in source_files:
        parquet_file = pq.ParquetFile(path)
        for index in range(parquet_file.metadata.num_row_groups):
            first = _row_group_endpoint_key(parquet_file, index, sort_columns, first=True)
            last = _row_group_endpoint_key(parquet_file, index, sort_columns, first=False)
            if first > last:
                raise RuntimeError(f"Source row group is not ordered by {sort_columns}: {path}")
            keyed_groups.append((first, last, path, index))
    keyed_groups.sort(key=lambda item: item[0])
    previous = None
    for first, last, path, _ in keyed_groups:
        if previous is not None and first <= previous:
            raise RuntimeError(f"Source key ranges overlap before {path}")
        previous = last
    return [(path, index) for _, _, path, index in keyed_groups]


def key_boundary_index(
    table: Any,
    schema: pa.Schema,
    key_column: str,
    index: int,
) -> int:
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


def flat_table_calibration(
    args: Any,
    table: str,
    schema: pa.Schema,
    first_group: tuple[Path, int],
    row_group_rows: int,
    codec: dict[str, Any],
) -> SizeCalibration:
    path, group_index = first_group
    sample = sort_gpu_table(
        read_source_row_groups(path, (group_index,)),
        schema,
        FLAT_SPLIT_SPECS[table],
    )
    sample_path = args.staging / table / "calibration.parquet"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    write_gpu_parquet(sample_path, sample, schema, row_group_rows, codec)
    calibration = SizeCalibration(args.target_file_bytes, sample.num_rows(), sample_path.stat().st_size)
    sample_path.unlink()
    return calibration


def rewrite_flat_table(
    args: Any,
    destination: OutputDestination,
    table: str,
    codecs: dict[str, dict[str, Any]],
    work: FlatWork | None = None,
) -> None:
    source_files = parquet_files(args.source / table)
    schema = inspect_schema(source_files)
    statistics = table_statistics(source_files)
    sort_columns = FLAT_SPLIT_SPECS[table]
    groups = ordered_source_row_groups(source_files, sort_columns)
    global_group_start = 0
    if work is not None:
        if work.last_row_group > len(groups):
            raise ValueError(f"{table} row-group range ends after {len(groups)}")
        global_group_start = work.first_row_group
        if work.first_row_group:
            previous_path, previous_index = groups[work.first_row_group - 1]
            current_path, current_index = groups[work.first_row_group]
            previous_key = _row_group_endpoint_key(
                pq.ParquetFile(previous_path), previous_index, sort_columns[:1], False
            )
            current_key = _row_group_endpoint_key(
                pq.ParquetFile(current_path), current_index, sort_columns[:1], True
            )
            if current_key <= previous_key:
                raise ValueError(f"{table} work starts inside a {sort_columns[0]} key group")
        if work.last_row_group < len(groups):
            current_path, current_index = groups[work.last_row_group - 1]
            next_path, next_index = groups[work.last_row_group]
            current_key = _row_group_endpoint_key(
                pq.ParquetFile(current_path), current_index, sort_columns[:1], False
            )
            next_key = _row_group_endpoint_key(
                pq.ParquetFile(next_path), next_index, sort_columns[:1], True
            )
            if next_key <= current_key:
                raise ValueError(f"{table} work ends inside a {sort_columns[0]} key group")
        groups = groups[work.first_row_group : work.last_row_group]
    row_group_rows = max(1, round(args.row_group_bytes / statistics["uncompressed_bytes_per_row"]))
    codec = codecs.get(table, {})
    calibration = flat_table_calibration(args, table, schema, groups[0], row_group_rows, codec)
    input_budget = gpu_input_budget(args.gpu_memory_bytes)
    group_index = 0
    buffer = None
    output_rows = 0
    file_index = 0

    def load_one(current: Any | None) -> Any | None:
        nonlocal group_index
        if group_index == len(groups):
            return current
        path, row_group_index = groups[group_index]
        group_index += 1
        piece = read_source_row_groups(path, (row_group_index,))
        combined = piece if current is None else plc.concatenate.concatenate([current, piece])
        if sum(column.device_buffer_size() for column in combined.columns()) > input_budget:
            raise RuntimeError(f"{table} flat-output buffer exceeds the GPU input budget")
        return combined

    while group_index < len(groups) or buffer is not None:
        if buffer is None:
            buffer = load_one(None)
        predicted_rows = calibration.predicted_rows()
        while buffer.num_rows() < predicted_rows and group_index < len(groups):
            buffer = load_one(buffer)
        available_rows = buffer.num_rows()
        candidate_rows = min(available_rows, predicted_rows)
        if len(sort_columns) > 1 and candidate_rows < available_rows:
            candidate_rows = key_boundary_index(buffer, schema, sort_columns[0], candidate_rows)
            if candidate_rows <= 0:
                raise RuntimeError(f"{table} leading-key group exceeds one output buffer")
        source_slice = plc.copying.slice(buffer, [0, candidate_rows])[0]
        output_table = sort_gpu_table(source_slice, schema, sort_columns)
        relative = (
            f"{table}/part-rg-{global_group_start:08d}-{file_index:05d}.parquet"
            if work is not None
            else f"{table}/part-{file_index:05d}.parquet"
        )
        local_output = args.staging / table / "output" / relative
        local_output.parent.mkdir(parents=True, exist_ok=True)
        write_gpu_parquet(local_output, output_table, schema, row_group_rows, codec)
        details = validate_basic_output(local_output, schema, candidate_rows)
        details.update(destination.publish_file(local_output, relative))
        print(f"{relative}: {details['rows']} rows, {details['physical_bytes'] / MIB:.1f} MiB")
        calibration.observe(candidate_rows, details["physical_bytes"])
        output_rows += candidate_rows
        file_index += 1
        local_output.unlink()
        buffer = (
            plc.copying.slice(buffer, [candidate_rows, available_rows])[0] if candidate_rows < available_rows else None
        )

    expected_rows = sum(pq.ParquetFile(path).metadata.row_group(index).num_rows for path, index in groups)
    if output_rows != expected_rows:
        raise RuntimeError(f"Output row count differs from source for {table}")


def publish_copy_tables(
    args: Any,
    destination: OutputDestination,
    tables: Sequence[str] = COPY_TABLES,
) -> None:
    for table in tables:
        source_table = args.source / table
        if not source_table.is_dir():
            raise FileNotFoundError(f"Missing source table: {source_table}")
        for source_file in sorted(path for path in source_table.rglob("*") if path.is_file()):
            relative = source_file.relative_to(args.source).as_posix()
            details = destination.publish_file(source_file, relative)
            print(f"{relative}: copied {details['physical_bytes'] / MIB:.1f} MiB")


def prepare_staging(staging: Path, overwrite: bool) -> None:
    if staging.exists():
        if not overwrite:
            raise FileExistsError(f"Staging exists; use --overwrite: {staging}")
        shutil.rmtree(staging)
    staging.mkdir(parents=True)


def run(args: Any) -> None:
    global plc
    import pylibcudf as plc

    args.source = args.source.expanduser().resolve()
    args.staging = args.staging.expanduser().resolve()
    if not args.source.is_dir():
        raise FileNotFoundError(f"Source dataset does not exist: {args.source}")
    location = DestinationLocation.parse(args.destination)
    path_pairs = [(args.staging, args.source)]
    if location.scheme == "file":
        destination_path = Path(location.root)
        path_pairs.extend(
            (
                (destination_path, args.source),
                (destination_path, args.staging),
            )
        )
    for left, right in path_pairs:
        if left == right or left in right.parents or right in left.parents:
            raise ValueError("Source, destination, and staging paths must be disjoint")

    codecs = load_codec_definitions(args.codec_definitions)
    work_spec = load_work_spec(args.work_spec) if args.work_spec is not None else None
    prepare_staging(args.staging, args.overwrite or args.overwrite_staging)
    destination = make_destination(location, args.s3_region)
    if work_spec is None:
        destination.prepare(args.overwrite)
        for table in args.tables:
            rewrite_partition_table(args, destination, table, codecs)
    else:
        if args.overwrite:
            raise ValueError("--overwrite cannot be used with a shared work-spec destination")
        for table, work in work_spec.partition_tables.items():
            rewrite_partition_table(args, destination, table, codecs, work)
        for table, work in work_spec.flat_tables.items():
            rewrite_flat_table(args, destination, table, codecs, work)
        publish_copy_tables(args, destination, work_spec.copy_tables)

    if work_spec is None and set(args.tables) == set(PARTITION_TABLES):
        for table in FLAT_SPLIT_TABLES:
            rewrite_flat_table(args, destination, table, codecs)
        publish_copy_tables(args, destination)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Source filesystem dataset root")
    parser.add_argument("--destination", required=True, help="Filesystem path, file:// URI, or s3:// URI")
    parser.add_argument("--staging", type=Path, required=True, help="Local staging directory, preferably NVMe")
    parser.add_argument(
        "--s3-region",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
    )
    parser.add_argument("--target-file-bytes", type=int, default=512 * MIB)
    parser.add_argument("--file-size-tolerance", type=float, default=0.05)
    parser.add_argument("--row-group-bytes", type=int, default=128 * MIB)
    parser.add_argument("--gpu-read-chunk-bytes", type=int, default=2 * 1024 * MIB)
    parser.add_argument("--gpu-memory-bytes", type=int, default=48 * 1024 * MIB)
    parser.add_argument("--tables", nargs="+", choices=PARTITION_TABLES, default=list(PARTITION_TABLES))
    parser.add_argument(
        "--work-spec",
        type=Path,
        help="JSON assignment for one independent worker sharing a destination",
    )
    parser.add_argument("--codec-definitions", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--overwrite-staging",
        action="store_true",
        help="Replace only local staging; safe for workers sharing a destination",
    )
    args = parser.parse_args(argv)
    for name in ("target_file_bytes", "row_group_bytes", "gpu_read_chunk_bytes", "gpu_memory_bytes"):
        if getattr(args, name) <= 0:
            parser.error("size options must be positive")
    if not 0 <= args.file_size_tolerance < 1:
        parser.error("--file-size-tolerance must be in [0, 1)")
    return args


if __name__ == "__main__":
    run(parse_args())
