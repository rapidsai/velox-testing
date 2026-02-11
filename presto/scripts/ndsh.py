#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "numpy",
#     "pyarrow",
# ]
# ///

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Validation script for NDSH benchmark results.

Compares benchmark output parquet files against expected parquet files,
with configurable tolerance for type mismatches, floating-point precision,
column naming differences, and date/string coercion.

Usage:
    python ndsh.py validate \\
        --results-path /path/to/results/ \\
        --expected-path /path/to/expected/ \\
        --ignore-integer-sign \\
        --ignore-integer-size \\
        --ignore-string-type \\
        --ignore-timezone \\
        --ignore-decimal-int \\
        --ignore-column-names \\
        --coerce-date-strings
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def discover_parquet_files(directory: Path) -> dict[str, Path]:
    """
    Discover parquet files matching the qDD.parquet pattern.

    Parameters
    ----------
    directory
        Directory containing parquet files (q03.parquet, q09.parquet, etc.)

    Returns
    -------
    Dictionary mapping query_name (e.g., 'q03') to file path.
    """
    pattern = re.compile(r"^q_?(\d+)\.parquet$")
    files = {}

    for file in directory.iterdir():
        if not file.is_file():
            continue
        match = pattern.match(file.name)
        if match:
            query_name = f"q{int(match.group(1))}"
            files[query_name] = file

    return files


def _types_compatible(
    o_type: pa.DataType,
    e_type: pa.DataType,
    *,
    ignore_timezone: bool = False,
    ignore_string_type: bool = False,
    ignore_integer_sign: bool = False,
    ignore_integer_size: bool = False,
    ignore_decimal_int: bool = False,
) -> bool:
    """
    Check if two Arrow types are compatible given the ignore flags.

    Returns True if the types should be considered equal.
    """
    if o_type.equals(e_type):
        return True

    # Ignore differences in timezone and precision for timestamps
    if (
        ignore_timezone
        and pa.types.is_timestamp(o_type)
        and pa.types.is_timestamp(e_type)
    ):
        return True

    # Ignore large_string vs string differences
    if ignore_string_type:
        string_types = {pa.string(), pa.large_string()}
        if o_type in string_types and e_type in string_types:
            return True

    # Check integer compatibility
    if pa.types.is_integer(o_type) and pa.types.is_integer(e_type):
        o_signed = pa.types.is_signed_integer(o_type)
        e_signed = pa.types.is_signed_integer(e_type)
        o_width = o_type.bit_width
        e_width = e_type.bit_width

        sign_matches = o_signed == e_signed or ignore_integer_sign
        size_matches = o_width == e_width or ignore_integer_size

        if sign_matches and size_matches:
            return True

    # Ignore decimal vs integer differences
    if ignore_decimal_int:
        o_is_numeric = pa.types.is_integer(o_type) or pa.types.is_decimal(o_type)
        e_is_numeric = pa.types.is_integer(e_type) or pa.types.is_decimal(e_type)
        if o_is_numeric and e_is_numeric:
            return True

    # Ignore float vs decimal differences
    if ignore_decimal_int:
        o_is_numeric = (
            pa.types.is_floating(o_type) or pa.types.is_decimal(o_type)
        )
        e_is_numeric = (
            pa.types.is_floating(e_type) or pa.types.is_decimal(e_type)
        )
        if o_is_numeric and e_is_numeric:
            return True

    return False


def compare_parquet(
    output_path: Path,
    expected_path: Path,
    decimal: int = 2,
    *,
    ignore_timezone: bool = False,
    ignore_string_type: bool = False,
    ignore_integer_sign: bool = False,
    ignore_integer_size: bool = False,
    ignore_decimal_int: bool = False,
    ignore_column_names: bool = False,
    coerce_date_strings: bool = False,
) -> tuple[bool, str | None]:
    """
    Compare two parquet files for exact equality.

    Parameters
    ----------
    output_path
        Path to the benchmark output parquet
    expected_path
        Path to the expected parquet
    decimal
        Number of decimal places to compare for floating point values
    ignore_timezone
        Ignore differences in timezone and precision for timestamp types
    ignore_string_type
        Ignore differences between string and large_string types.
        Note that the values will still be compared.
    ignore_integer_sign
        Ignore differences between signed and unsigned integer types
        Note that the values will still be compared.
    ignore_integer_size
        Ignore differences in integer bit width (e.g., int32 vs int64)
        Note that the values will still be compared.
    ignore_decimal_int
        Ignore differences between decimal and integer types
        Note that the values will still be compared.
    ignore_column_names
        Ignore differences in column names (compare by position instead).
    coerce_date_strings
        When one column is a string and the other is a date/timestamp,
        attempt to cast the string column to the date type before comparing.

    Returns
    -------
    Tuple of boolean indicating success and list of error messages. A non-empty list indicates failure.
    """
    try:
        output = pq.read_table(output_path)
        expected = pq.read_table(expected_path)
    except Exception as e:
        return False, f"Failed to read parquet files: {e}"

    # Check column count
    if len(output.schema) != len(expected.schema):
        return (
            False,
            f"Column count mismatch: output={len(output.schema)}, "
            f"expected={len(expected.schema)}",
        )

    # Check the schema and data by validating...
    # 1. names...
    if not ignore_column_names and output.schema.names != expected.schema.names:
        return (
            False,
            f"Schema name mismatch: {output.schema.names} != {expected.schema.names}",
        )

    # 2. row count (check early so empty results don't produce
    #    misleading null-type errors)...
    if output.num_rows != expected.num_rows:
        return False, (
            f"Row count mismatch: output={output.num_rows}, expected={expected.num_rows}"
        )

    # Helper: check if a type is string-like
    def _is_string_type(t: pa.DataType) -> bool:
        return t in (pa.string(), pa.large_string(), pa.utf8(), pa.large_utf8())

    # Helper: check if a type is date/timestamp-like
    def _is_date_type(t: pa.DataType) -> bool:
        return pa.types.is_date(t) or pa.types.is_timestamp(t)

    # 3. types (compare by position to handle ignore_column_names)...
    #    Also coerce columns where one side is string and the other is date.
    errors = []
    coerced_columns: dict[int, str] = {}  # col index -> "output" or "expected"
    for i in range(len(output.schema)):
        o_field = output.schema.field(i)
        e_field = expected.schema.field(i)
        label = (
            o_field.name
            if not ignore_column_names
            else f"col[{i}] ({o_field.name} / {e_field.name})"
        )

        # Check if coerce_date_strings applies
        if coerce_date_strings:
            if _is_string_type(o_field.type) and _is_date_type(e_field.type):
                coerced_columns[i] = "output"
                continue
            if _is_date_type(o_field.type) and _is_string_type(e_field.type):
                coerced_columns[i] = "expected"
                continue

        # We only care about the type, not the metadata or nullability
        if not _types_compatible(
            o_field.type,
            e_field.type,
            ignore_timezone=ignore_timezone,
            ignore_string_type=ignore_string_type,
            ignore_integer_sign=ignore_integer_sign,
            ignore_integer_size=ignore_integer_size,
            ignore_decimal_int=ignore_decimal_int,
        ):
            errors.append(f"\t{label}: {o_field.type} != {e_field.type}")
    if errors:
        return False, "\n".join(["Field type mismatch (output != expected)", *errors])

    # 4. and values (compare by position).
    rtol = 10 ** (-decimal)
    for i, (out_col, expected_col) in enumerate(
        zip(output.columns, expected.columns, strict=False)
    ):
        name = output.schema.field(i).name

        # Apply date-string coercion if needed
        if i in coerced_columns:
            target_type = expected.schema.field(i).type
            if coerced_columns[i] == "output":
                try:
                    out_col = pa.compute.cast(out_col, target_type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
                    errors.append(
                        f"{name}: failed to coerce output string to "
                        f"{target_type}: {e}"
                    )
                    continue
            else:
                target_type = output.schema.field(i).type
                try:
                    expected_col = pa.compute.cast(expected_col, target_type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
                    errors.append(
                        f"{name}: failed to coerce expected string to "
                        f"{target_type}: {e}"
                    )
                    continue

        if pa.types.is_floating(out_col.type) or pa.types.is_floating(
            expected_col.type
        ):
            # We don't promise exact equality; use relative tolerance
            # so that large-magnitude values aren't penalized.
            try:
                np.testing.assert_allclose(
                    out_col.to_numpy(zero_copy_only=False).astype(float),
                    expected_col.to_numpy(zero_copy_only=False).astype(float),
                    rtol=rtol,
                    atol=rtol,
                )
            except AssertionError as e:
                errors.append(f"{name} differs. {e}")
        else:
            try:
                np.testing.assert_array_equal(
                    out_col.to_numpy(zero_copy_only=False),
                    expected_col.to_numpy(zero_copy_only=False),
                )
            except AssertionError as e:
                errors.append(f"{name} differs. {e}")

    if errors:
        return False, "\n".join(errors)

    return True, None


def cmd_validate(args: argparse.Namespace) -> int:
    """Execute the 'validate' subcommand."""
    if not args.results_path.exists():
        print(f"Error: Results directory does not exist: {args.results_path}")
        return 1

    if not args.expected_path.exists():
        print(f"Error: Expected directory does not exist: {args.expected_path}")
        return 1

    # Discover parquet files in both directories
    # But we treat *results* as the source of truth. If we have a result
    # but not an expected we error; if we have an expected but not a result,
    # that's fine.
    results_files = discover_parquet_files(args.results_path)
    expected_files = discover_parquet_files(args.expected_path)

    if not results_files:
        print(f"No qDD.parquet files found in results directory: {args.results_path}")
        return 1

    print(f"\nValidating {len(results_files)} query(ies):")

    # Validate each matching pair
    results = {}
    def _query_sort_key(name: str) -> int:
        return int(name.lstrip("q"))

    for query_name in sorted(results_files, key=_query_sort_key):
        print(f"\nValidating {query_name}...")
        result_path = results_files[query_name]

        if query_name not in expected_files:
            print(
                f"  FAILED: No expected file found for {query_name} "
                f"in {args.expected_path}"
            )
            results[query_name] = False
            continue

        expected_path = expected_files[query_name]

        is_equal, message = compare_parquet(
            result_path,
            expected_path,
            decimal=args.decimal,
            ignore_timezone=args.ignore_timezone,
            ignore_string_type=args.ignore_string_type,
            ignore_integer_sign=args.ignore_integer_sign,
            ignore_integer_size=args.ignore_integer_size,
            ignore_decimal_int=args.ignore_decimal_int,
            ignore_column_names=args.ignore_column_names,
            coerce_date_strings=args.coerce_date_strings,
        )

        if is_equal:
            print("  PASSED")
            results[query_name] = True
        else:
            print(f"  FAILED:\n{message}")
            results[query_name] = False

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    passed = sum(results.values())
    failed = len(results) - passed

    for query_name, result in sorted(results.items(), key=lambda x: _query_sort_key(x[0])):
        status = "PASSED" if result else "FAILED"
        print(f"  {query_name}: {status}")

    print("-" * 60)
    print(f"Total: {passed} passed, {failed} failed")

    return int(failed > 0)


def main():
    """Run the NDSH validation tool."""
    parser = argparse.ArgumentParser(
        description="NDSH benchmark result validator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # 'validate' subcommand
    validate_parser = subparsers.add_parser(
        "validate",
        help="Compare results against expected",
        description="Validate benchmark results by comparing parquet files against expected results.",
    )
    validate_parser.add_argument(
        "--results-path",
        type=Path,
        required=True,
        help="Directory containing benchmark result parquet files (qDD.parquet)",
    )
    validate_parser.add_argument(
        "--expected-path",
        type=Path,
        required=True,
        help="Directory containing expected parquet files (qDD.parquet)",
    )
    validate_parser.add_argument(
        "-d",
        "--decimal",
        type=int,
        default=2,
        help="Number of decimal places to compare for floating point values (default: 2)",
    )
    validate_parser.add_argument(
        "--ignore-timezone",
        action="store_true",
        help="Ignore differences in timezone and precision for timestamp types",
    )
    validate_parser.add_argument(
        "--ignore-string-type",
        action="store_true",
        help="Ignore differences between string and large_string types",
    )
    validate_parser.add_argument(
        "--ignore-integer-sign",
        action="store_true",
        help="Ignore differences between signed and unsigned integer types",
    )
    validate_parser.add_argument(
        "--ignore-integer-size",
        action="store_true",
        help="Ignore differences in integer bit width (e.g., int32 vs int64)",
    )
    validate_parser.add_argument(
        "--ignore-decimal-int",
        action="store_true",
        help="Ignore differences between decimal and integer types",
    )
    validate_parser.add_argument(
        "--ignore-column-names",
        action="store_true",
        help="Ignore differences in column names (compare by position instead)",
    )
    validate_parser.add_argument(
        "--coerce-date-strings",
        action="store_true",
        help="Cast string columns to date type when comparing against date columns",
    )

    args = parser.parse_args()

    if args.command == "validate":
        sys.exit(cmd_validate(args))


if __name__ == "__main__":
    main()
