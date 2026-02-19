# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Shared result comparison utilities for correctness testing."""

import datetime
import decimal

from .query_utils import get_orderby_indices

FLOATING_POINT_TYPES = ("double", "float", "decimal")


def none_safe_sort_key(row):
    """Sort key that treats None as less than any other value."""
    return tuple((0, x) if x is not None else (1, None) for x in row)


def compare_results(test_rows, reference_rows, types, query, column_names, normalize_test_rows_fn=None):
    """
    Compare test results against reference results.

    Args:
        test_rows: Rows from the system under test (Presto, Spark, etc.)
        reference_rows: Rows from the reference system (DuckDB)
        types: DuckDB column types
        query: The SQL query string
        column_names: Column names from the reference result
        normalize_test_rows_fn: Optional function to normalize test rows (for Spark-specific types)
    """
    row_count = len(test_rows)
    assert row_count == len(reference_rows), f"Row count mismatch: test={row_count}, reference={len(reference_rows)}"

    reference_rows = normalize_rows(reference_rows, types)
    if normalize_test_rows_fn:
        test_rows = normalize_test_rows_fn(test_rows, types)
    else:
        test_rows = normalize_rows(test_rows, types)

    # We need a full sort for all non-ORDER BY columns because some ORDER BY comparison
    # will be equal and the resulting order of non-ORDER BY columns will be ambiguous.
    sorted_reference_rows = sorted(reference_rows, key=none_safe_sort_key)
    sorted_test_rows = sorted(test_rows, key=none_safe_sort_key)
    assert_rows_equal(sorted_test_rows, sorted_reference_rows, types)

    # If we have an ORDER BY clause we want to test that the resulting order of those
    # columns is correct, in addition to overall values being correct.
    # However, we can only validate the ORDER BY if we can extract column indices.
    # For complex ORDER BY expressions (aggregates, CASE statements, etc.), we skip
    # the ORDER BY validation but still validate overall result correctness.
    order_indices = get_orderby_indices(query, column_names)
    if order_indices:
        # Project both results to ORDER BY columns and compare in original order
        reference_proj = [[row[i] for i in order_indices] for row in reference_rows]
        test_proj = [[row[i] for i in order_indices] for row in test_rows]
        projected_types = [types[i] for i in order_indices]
        assert_rows_equal(test_proj, reference_proj, projected_types)


def normalize_rows(rows, types):
    """Normalize rows for comparison."""
    return [normalize_row(row, types) for row in rows]


def normalize_row(row, types):
    """Normalize a single row for comparison."""
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id
        if type_id == "date":
            normalized_row.append(str(value))
        elif type_id in FLOATING_POINT_TYPES:
            normalized_row.append(float(value))
        else:
            normalized_row.append(value)
    return normalized_row


def normalize_row_with_spark_types(row, types):
    """Normalize a Spark row for comparison (handles Spark-specific types)."""
    normalized_row = []
    for index, value in enumerate(row):
        if value is None:
            normalized_row.append(value)
            continue

        type_id = types[index].id

        # Handle Spark-specific types
        if isinstance(value, datetime.date):
            normalized_row.append(str(value))
        elif isinstance(value, decimal.Decimal):
            normalized_row.append(float(value))
        elif type_id in FLOATING_POINT_TYPES:
            normalized_row.append(float(value))
        else:
            normalized_row.append(value)
    return normalized_row


def normalize_spark_rows(rows, types):
    """Normalize Spark rows for comparison."""
    return [normalize_row_with_spark_types(row, types) for row in rows]


def assert_rows_equal(rows_1, rows_2, types):
    """Assert that two sets of rows are equal within tolerance."""
    if len(rows_1) != len(rows_2):
        raise AssertionError(f"Row count mismatch: {len(rows_1)} vs {len(rows_2)}")

    float_cols = {i for i, t in enumerate(types) if t.id in FLOATING_POINT_TYPES}
    mismatches = []
    abs_tolerance = 0.02
    max_mismatches = 5

    for row_idx, (row_1, row_2) in enumerate(zip(rows_1, rows_2)):
        if len(row_1) != len(row_2):
            mismatches.append(f"Row: {row_idx} length mismatch: {len(row_1)} vs {len(row_2)}")
            if len(mismatches) >= max_mismatches:
                break
            continue

        for col_idx, (value_1, value_2) in enumerate(zip(row_1, row_2)):
            if value_1 is None and value_2 is None:
                continue
            if value_1 is None or value_2 is None:
                mismatches.append(f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2} (null mismatch)")
            elif col_idx in float_cols:
                if abs(value_1 - value_2) > abs_tolerance:
                    mismatches.append(
                        f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2} "
                        f"(diff={abs(value_1 - value_2):.6f}, tolerance={abs_tolerance})"
                    )
            elif value_1 != value_2:
                mismatches.append(f"Row: {row_idx}, Column: {col_idx}: {value_1} vs {value_2}")

            if len(mismatches) >= max_mismatches:
                break

        if len(mismatches) >= max_mismatches:
            break

    if mismatches:
        truncated_msg = f" (showing first {max_mismatches})" if len(mismatches) >= max_mismatches else ""
        mismatch_details = "\n  ".join(mismatches)
        raise AssertionError(f"Found {len(mismatches)} mismatches{truncated_msg}:\n  {mismatch_details}")
