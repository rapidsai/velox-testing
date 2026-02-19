# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Shared query utilities for loading and parsing SQL queries."""

import json

import sqlglot


def load_queries_from_file(queries_file_path):
    """Load queries from a JSON file."""
    with open(queries_file_path, "r") as file:
        return json.load(file)


def get_queries(queries_dir, benchmark_type):
    """
    Load queries for a benchmark type from a queries directory.

    Args:
        queries_dir: Path to the directory containing query JSON files
        benchmark_type: Either 'tpch' or 'tpcds'

    Returns:
        Dictionary mapping query IDs to SQL strings
    """
    queries_file = f"{queries_dir}/{benchmark_type}/queries.json"
    return load_queries_from_file(queries_file)


def get_orderby_indices(query, column_names):
    """
    Extract ORDER BY column indices from a query.

    Returns empty list for complex ORDER BY expressions that can't be mapped to column indices.
    """
    expr = sqlglot.parse_one(query)
    order = next((e for e in expr.find_all(sqlglot.exp.Order)), None)
    if not order:
        return []

    indices = []
    for ordered in order.expressions:
        key = ordered.this

        # Handle numeric literals (e.g., ORDER BY 1, 2)
        if isinstance(key, sqlglot.exp.Literal):
            try:
                col_num = int(key.this)
                if 1 <= col_num <= len(column_names):
                    indices.append(col_num - 1)  # Convert to 0-based index
                    continue
            except (ValueError, TypeError):
                pass

        # Handle simple column references
        if isinstance(key, sqlglot.exp.Column):
            name = key.name
            if name in column_names:
                indices.append(column_names.index(name))
                continue

        # For complex expressions (CASE, SUM, etc.), skip ORDER BY validation
        # We still validate overall result correctness with full sorting
        # Just don't validate the specific ORDER BY column ordering
        pass

    return indices


def get_is_sorted_query(query):
    """Check if a query has an ORDER BY clause."""
    return any(isinstance(expr, sqlglot.exp.Order) for expr in sqlglot.parse_one(query).iter_expressions())
