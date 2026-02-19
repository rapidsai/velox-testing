#!/usr/bin/env python3
# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import re
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def parse_table_name(schema_path: Path) -> str:
    content = schema_path.read_text()
    match = re.search(r"CREATE\s+TABLE\s+([^\s(]+)", content, re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find CREATE TABLE in {schema_path}")
    full_name = match.group(1)
    return full_name.split(".")[-1]


def find_parquet_files(data_dir: Path, table_name: str) -> list[Path]:
    table_dir = data_dir / table_name
    if table_dir.is_dir():
        return sorted(table_dir.rglob("*.parquet"))
    # Fallback: search the whole data directory for any parquet file containing
    # the table name as a path segment.
    return sorted(
        p for p in data_dir.rglob("*.parquet") if table_name in p.parts
    )


def decimal_descriptors(parquet_path: Path) -> dict[str, set[tuple]]:
    pf = pq.ParquetFile(parquet_path)
    arrow_schema = pf.schema_arrow
    parquet_schema = pf.schema

    results = defaultdict(set)
    for i, field in enumerate(arrow_schema):
        if not pa.types.is_decimal(field.type):
            continue
        col = parquet_schema.column(i)
        physical = getattr(col, "physical_type", "UNKNOWN")
        logical = getattr(col, "logical_type", None)
        converted = getattr(col, "converted_type", None)
        type_length = getattr(col, "type_length", None)
        logical_str = str(logical) if logical is not None else None
        converted_str = str(converted) if converted is not None else None
        descriptor = (
            str(field.type),
            physical,
            logical_str,
            converted_str,
            type_length,
        )
        results[field.name].add(descriptor)
    return results


def inspect_table(table_name: str, parquet_files: list[Path], max_files: int) -> None:
    if max_files != 0:
        parquet_files = parquet_files[:max_files]
    if not parquet_files:
        print(f"[WARN] No parquet files found for table '{table_name}'.")
        return

    per_column = defaultdict(set)
    for path in parquet_files:
        for col_name, descriptors in decimal_descriptors(path).items():
            per_column[col_name].update(descriptors)

    print(f"Table: {table_name}")
    print(f"  scanned_files={len(parquet_files)}")
    if not per_column:
        print("  (no decimal columns found)")
        return
    for col_name in sorted(per_column.keys()):
        print(f"  column: {col_name}")
        for desc in sorted(per_column[col_name]):
            arrow_type, physical, logical, converted, type_length = desc
            print(
                f"    arrow={arrow_type} physical={physical} "
                f"logical={logical} converted={converted} "
                f"type_length={type_length}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect Parquet files and report decimal column physical types "
            "for tables defined by schema files."
        )
    )
    parser.add_argument(
        "-s",
        "--schema-path",
        required=True,
        help="Path to a schema .sql file or a directory of schema files.",
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        required=True,
        help="Path to the directory containing table parquet data.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5,
        help="Max parquet files to scan per table (0 = all).",
    )
    args = parser.parse_args()

    schema_path = Path(args.schema_path)
    data_dir = Path(args.data_dir)

    if not schema_path.exists():
        raise SystemExit(f"Schema path not found: {schema_path}")
    if not data_dir.exists():
        raise SystemExit(f"Data dir not found: {data_dir}")

    if schema_path.is_dir():
        schema_files = sorted(schema_path.rglob("*.sql"))
    else:
        schema_files = [schema_path]

    if not schema_files:
        raise SystemExit(f"No schema files found under {schema_path}")

    for schema_file in schema_files:
        table_name = parse_table_name(schema_file)
        parquet_files = find_parquet_files(data_dir, table_name)
        inspect_table(table_name, parquet_files, args.max_files)


if __name__ == "__main__":
    main()
