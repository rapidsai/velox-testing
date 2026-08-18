#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Create a date-range-partitioned, GPU-sorted copy of a Parquet table directory.

Rewrites --source-table into --output-table: files are range-partitioned on
--partition-key and sorted by that key (plus optional --secondary-order-key).
The output keeps the same file count as the source; row counts per file may
differ.

Pass --identity to rewrite through pylibcudf without sorting or repartitioning
(useful as a writer-only control).
"""

import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pylibcudf as plc


def resolve_existing_path(path: Path) -> Path:
    """Follow symlinks and verify the path exists."""
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {path} (resolved to {resolved})")
    return resolved


@dataclass(frozen=True)
class ColumnLayout:
    name: str
    compression: str
    encoding: str


@dataclass(frozen=True)
class TableLayout:
    files: tuple[Path, ...]
    schema: pa.Schema
    columns: tuple[ColumnLayout, ...]
    row_group_rows: int
    rows: int
    minimum_date: date | None
    maximum_date: date | None


def preferred_encoding(encodings: tuple[str, ...]) -> str:
    """Choose the source's data-page encoding, ignoring level encodings."""
    for encoding in (
        "DELTA_BINARY_PACKED",
        "DELTA_LENGTH_BYTE_ARRAY",
        "DELTA_BYTE_ARRAY",
        "BYTE_STREAM_SPLIT",
        "RLE_DICTIONARY",
        "PLAIN_DICTIONARY",
        "PLAIN",
    ):
        if encoding in encodings:
            return "DICTIONARY" if "DICTIONARY" in encoding else encoding
    raise ValueError(f"Unsupported Parquet encodings: {encodings}")


def inspect_table(table_dir: Path, partition_key: str | None) -> TableLayout:
    files = tuple(sorted(table_dir.glob("*.parquet")))
    if not files:
        raise ValueError(f"No Parquet files found in {table_dir}")

    first = pq.ParquetFile(files[0])
    schema = first.schema_arrow
    partition_index = None
    if partition_key is not None:
        try:
            partition_index = schema.names.index(partition_key)
        except ValueError as error:
            raise ValueError(f"{partition_key!r} is not present in {files[0]}") from error

        if not (
            pa.types.is_date(schema.field(partition_index).type)
            or pa.types.is_timestamp(schema.field(partition_index).type)
        ):
            raise TypeError(
                f"{partition_key} must be a date or timestamp, got {schema.field(partition_index).type}"
            )

    first_row_group = first.metadata.row_group(0)
    columns = tuple(
        ColumnLayout(
            name=schema.names[index],
            compression=first_row_group.column(index).compression,
            encoding=preferred_encoding(first_row_group.column(index).encodings),
        )
        for index in range(len(schema))
    )
    row_group_rows = first_row_group.num_rows
    total_rows = 0
    minima = []
    maxima = []

    for path in files:
        parquet_file = pq.ParquetFile(path)
        if not parquet_file.schema_arrow.equals(schema):
            raise ValueError(f"Schema differs from the first source file: {path}")
        total_rows += parquet_file.metadata.num_rows

        for row_group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for column_index, expected in enumerate(columns):
                column = row_group.column(column_index)
                actual_encoding = preferred_encoding(column.encodings)
                if column.compression != expected.compression or actual_encoding != expected.encoding:
                    raise ValueError(
                        f"Inconsistent codec for {path}, row group {row_group_index}, {expected.name}: "
                        f"expected {expected.compression}/{expected.encoding}, "
                        f"got {column.compression}/{actual_encoding}"
                    )

            if partition_index is not None:
                statistics = row_group.column(partition_index).statistics
                if statistics is None or not statistics.has_min_max:
                    raise ValueError(f"Missing min/max statistics for {path}:{partition_key}")
                minima.append(statistics.min)
                maxima.append(statistics.max)

    return TableLayout(
        files=files,
        schema=schema,
        columns=columns,
        row_group_rows=row_group_rows,
        rows=total_rows,
        minimum_date=min(minima) if minima else None,
        maximum_date=max(maxima) if maxima else None,
    )


def date_ranges(minimum: date, maximum: date, count: int) -> list[tuple[date, date]]:
    """Split an inclusive date domain into non-overlapping, near-equal ranges."""
    day_count = (maximum - minimum).days + 1
    if count > day_count:
        raise ValueError(f"Cannot create {count} nonempty date ranges from a {day_count}-day domain")

    boundaries = [minimum + timedelta(days=(index * day_count) // count) for index in range(count + 1)]
    return [(boundaries[index], boundaries[index + 1] - timedelta(days=1)) for index in range(count)]


def make_filter(column_name: str, lower: date, upper: date, arrow_type: pa.DataType):
    expressions = plc.expressions
    lower_literal = expressions.Literal(plc.Scalar.from_arrow(pa.scalar(lower, type=arrow_type)))
    upper_literal = expressions.Literal(plc.Scalar.from_arrow(pa.scalar(upper, type=arrow_type)))
    return expressions.Operation(
        expressions.ASTOperator.LOGICAL_AND,
        expressions.Operation(
            expressions.ASTOperator.GREATER_EQUAL,
            expressions.ColumnNameReference(column_name),
            lower_literal,
        ),
        expressions.Operation(
            expressions.ASTOperator.LESS_EQUAL,
            expressions.ColumnNameReference(column_name),
            upper_literal,
        ),
    )


def read_range(files: tuple[Path, ...], partition_key: str, lower: date, upper: date, arrow_type):
    source = plc.io.SourceInfo([str(path) for path in files])
    options = plc.io.parquet.ParquetReaderOptions.builder(source).build()
    # ParquetReaderOptions keeps a non-owning reference to the AST.
    filter_expression = make_filter(partition_key, lower, upper, arrow_type)
    options.set_filter(filter_expression)
    return plc.io.parquet.read_parquet(options)


def sort_batch(tables, column_names: list[str], sort_columns: tuple[str, ...]):
    table = tables[0] if len(tables) == 1 else plc.concatenate.concatenate(tables)
    return sort_table(table, column_names, sort_columns)


def read_range_one_file_at_a_time(
    files: tuple[Path, ...],
    partition_key: str,
    lower: date,
    upper: date,
    arrow_type,
    sort_columns: tuple[str, ...],
    batch_rows: int,
):
    """Read filtered subsets file-by-file, sort locally in batches, then assemble."""
    sorted_runs = []
    pending_tables = []
    pending_rows = 0
    column_names = None
    total_rows = 0

    for source_index, source_path in enumerate(files, start=1):
        table_with_metadata = read_range(
            (source_path,),
            partition_key,
            lower,
            upper,
            arrow_type,
        )
        rows = table_with_metadata.tbl.num_rows()
        total_rows += rows
        print(
            f"  source {source_index}/{len(files)}: {source_path.name} -> {rows:,} rows "
            f"({total_rows:,} scanned)",
            flush=True,
        )
        if rows == 0:
            continue
        if column_names is None:
            column_names = table_with_metadata.column_names()
        pending_tables.append(table_with_metadata.tbl)
        pending_rows += rows

        if pending_rows >= batch_rows:
            print(f"  local sort batch of {pending_rows:,} rows ({len(sorted_runs) + 1} run)", flush=True)
            sorted_runs.append(sort_batch(pending_tables, column_names, sort_columns))
            pending_tables = []
            pending_rows = 0

    if pending_tables:
        print(f"  local sort final batch of {pending_rows:,} rows ({len(sorted_runs) + 1} run)", flush=True)
        sorted_runs.append(sort_batch(pending_tables, column_names, sort_columns))

    if not sorted_runs or column_names is None:
        raise RuntimeError(f"No rows found for output range {lower} through {upper}")

    if len(sorted_runs) == 1:
        return sorted_runs[0], column_names

    print(f"  concatenating {len(sorted_runs)} locally sorted runs for global sort", flush=True)
    return plc.concatenate.concatenate(sorted_runs), column_names


def sort_table(table, column_names: list[str], sort_columns: tuple[str, ...]):
    indices = [column_names.index(name) for name in sort_columns]
    keys = plc.Table([table.columns()[index] for index in indices])
    return plc.sorting.sort_by_key(
        table,
        keys,
        [plc.types.Order.ASCENDING] * len(indices),
        [plc.types.NullOrder.AFTER] * len(indices),
    )


def configure_metadata(table, layout: TableLayout):
    metadata = plc.io.types.TableInputMetadata(table)
    encoding_enum = plc.io.types.ColumnEncoding
    for target, source, field in zip(metadata.column_metadata, layout.columns, layout.schema, strict=True):
        target.set_name(source.name)
        target.set_nullability(field.nullable)
        # Dictionary encoding is controlled by DictionaryPolicy. Requesting the
        # DICTIONARY column encoding directly makes libcudf skip compression.
        # Do not call set_skip_compression: with set_encoding it suppresses
        # SNAPPY on other columns in libcudf 26.06. Let cuDF decide
        # compressibility under the file-level codec.
        if source.encoding != "DICTIONARY":
            target.set_encoding(getattr(encoding_enum, source.encoding))
    return metadata


def write_partition(path: Path, table, layout: TableLayout) -> None:
    compressed_codecs = {column.compression for column in layout.columns if column.compression != "UNCOMPRESSED"}
    if len(compressed_codecs) > 1:
        raise ValueError(f"pylibcudf cannot preserve multiple compressed codecs in one file: {compressed_codecs}")
    compression = next(iter(compressed_codecs), "NONE")

    options = (
        plc.io.parquet.ParquetWriterOptions.builder(plc.io.SinkInfo([str(path)]), table)
        .metadata(configure_metadata(table, layout))
        .compression(getattr(plc.io.types.CompressionType, compression))
        .dictionary_policy(plc.io.types.DictionaryPolicy.ADAPTIVE)
        .stats_level(plc.io.types.StatisticsFreq.STATISTICS_ROWGROUP)
        .write_v2_headers(True)
        .build()
    )
    options.set_row_group_size_rows(layout.row_group_rows)
    plc.io.parquet.write_parquet(options)


def read_file(path: Path):
    options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([str(path)])).build()
    return plc.io.parquet.read_parquet(options)


def rewrite_identity(layout: TableLayout, output_table_dir: Path) -> None:
    """Rewrite each source file through pylibcudf with no filter or sort."""
    for index, source_path in enumerate(layout.files, start=1):
        output_path = output_table_dir / source_path.name
        if output_path.exists():
            print(
                f"[{index}/{len(layout.files)}] {source_path.name} (skip, already exists)",
                flush=True,
            )
            continue
        print(f"[{index}/{len(layout.files)}] {source_path.name}: identity rewrite", flush=True)
        result = read_file(source_path)
        if result.column_names() != list(layout.schema.names):
            raise RuntimeError(f"Column names changed while reading {source_path}")
        write_partition(output_path, result.tbl, layout)


def verify_output(output_table_dir: Path, layout: TableLayout) -> None:
    output_files = tuple(sorted(output_table_dir.glob("*.parquet")))
    if len(output_files) != len(layout.files):
        raise RuntimeError(f"Expected {len(layout.files)} output files, found {len(output_files)}")

    output_rows = 0
    for path in output_files:
        parquet_file = pq.ParquetFile(path)
        if not parquet_file.schema_arrow.equals(layout.schema):
            raise RuntimeError(f"Output schema differs from source: {path}")
        output_rows += parquet_file.metadata.num_rows
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for column_index, expected in enumerate(layout.columns):
                column = row_group.column(column_index)
                actual_encoding = preferred_encoding(column.encodings)
                # libcudf may store a page uncompressed when compression would
                # make it larger, even when the requested codec is preserved in
                # writer options. This is common after sorting dictionary data.
                compression_fallback = (
                    expected.compression != "UNCOMPRESSED" and column.compression == "UNCOMPRESSED"
                )
                # High-cardinality dictionary columns (e.g. o_clerk) may fall back to
                # plain or delta-length after sorting; that is acceptable.
                encoding_fallback = expected.encoding == "DICTIONARY" and actual_encoding in {
                    "DICTIONARY",
                    "PLAIN",
                    "DELTA_LENGTH_BYTE_ARRAY",
                }
                if (column.compression != expected.compression and not compression_fallback) or (
                    actual_encoding != expected.encoding and not encoding_fallback
                ):
                    raise RuntimeError(
                        f"Codec mismatch in {path}, row group {row_group_index}, {expected.name}: "
                        f"expected {expected.compression}/{expected.encoding}, "
                        f"got {column.compression}/{actual_encoding}"
                    )

    if output_rows != layout.rows:
        raise RuntimeError(f"Row count changed: source={layout.rows}, output={output_rows}")


def run(args) -> None:
    source_table = resolve_existing_path(args.source_table)
    output_table = args.output_table.resolve()

    if args.identity:
        if args.partition_key is not None or args.secondary_order_key is not None:
            raise ValueError("--identity cannot be combined with ordering keys")
        sort_columns = ()
    else:
        if args.partition_key is None:
            raise ValueError("either --partition-key or --identity is required")
        sort_columns = (args.partition_key,)
        if args.secondary_order_key is not None:
            sort_columns += (args.secondary_order_key,)

    layout = inspect_table(source_table, args.partition_key)
    if args.secondary_order_key is not None and args.secondary_order_key not in layout.schema.names:
        raise ValueError(f"{args.secondary_order_key!r} is not present in {layout.files[0]}")
    if args.identity:
        plan = {
            "source_table": str(source_table),
            "identity": True,
            "source_rows": layout.rows,
            "source_files": len(layout.files),
        }
    else:
        if layout.minimum_date is None or layout.maximum_date is None:
            raise RuntimeError("Partition bounds were not collected")
        ranges = date_ranges(layout.minimum_date, layout.maximum_date, len(layout.files))
        plan = {
            "source_table": str(source_table),
            "partition_key": args.partition_key,
            "secondary_order_key": args.secondary_order_key,
            "sort_columns": sort_columns,
            "source_rows": layout.rows,
            "source_files": len(layout.files),
            "source_date_range": [str(layout.minimum_date), str(layout.maximum_date)],
            "output_ranges": [[str(lower), str(upper)] for lower, upper in ranges],
        }
    print(json.dumps(plan, indent=2))
    if args.dry_run:
        return

    if args.resume:
        if not output_table.is_dir():
            raise ValueError(f"--resume requires an existing output table directory: {output_table}")
    else:
        if output_table.exists():
            raise FileExistsError(f"Output table already exists: {output_table}")
        output_table.mkdir(parents=True)

    if args.identity:
        rewrite_identity(layout, output_table)
    else:
        arrow_type = layout.schema.field(args.partition_key).type
        for index, ((lower, upper), source_name) in enumerate(zip(ranges, layout.files, strict=True), start=1):
            output_path = output_table / source_name.name
            if output_path.exists():
                print(
                    f"[{index}/{len(ranges)}] {lower} through {upper} -> {source_name.name} "
                    f"(skip, already exists)",
                    flush=True,
                )
                continue
            print(f"[{index}/{len(ranges)}] {lower} through {upper} -> {source_name.name}", flush=True)
            table, column_names = read_range_one_file_at_a_time(
                layout.files,
                args.partition_key,
                lower,
                upper,
                arrow_type,
                sort_columns,
                args.batch_rows,
            )
            print(f"  global sort of {table.num_rows():,} rows", flush=True)
            sorted_table = sort_table(table, column_names, sort_columns)
            write_partition(output_path, sorted_table, layout)

    verify_output(output_table, layout)
    with (output_table / "sort_metadata.json").open("w") as file:
        json.dump(plan, file, indent=2)
        file.write("\n")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", type=Path, required=True, help="Existing Parquet table directory")
    parser.add_argument("--output-table", type=Path, required=True, help="New table directory; must not exist")
    parser.add_argument(
        "--partition-key",
        help="Date or timestamp column used to range-partition and primarily sort the output",
    )
    parser.add_argument(
        "--secondary-order-key",
        help="Optional column used as the secondary sort key within each output partition",
    )
    parser.add_argument(
        "--identity",
        action="store_true",
        help="Rewrite through pylibcudf without sorting or date-range repartitioning",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=15_000_000,
        help="Sort and merge in GPU batches of about this many rows (default: 15 million)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume into an existing output table; skip files that already exist",
    )
    parser.add_argument("--dry-run", action="store_true", help="Inspect metadata and print the partition plan only")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
