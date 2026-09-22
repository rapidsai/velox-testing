# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Convert temporary distributed CTAS tables into canonical qN.parquet files."""

import os
import re
import shutil
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import sqlglot
from sqlglot import exp

from common.testing.result_comparison import get_orderby_sort_spec, restore_orderby


def _query_directories(source_dir: Path) -> list[Path]:
    return sorted(
        (path for path in source_dir.iterdir() if path.is_dir() and re.fullmatch(r"q\d+", path.name)),
        key=lambda path: int(path.name[1:]),
    )


def _is_wildcard_projection(expression: exp.Expression) -> bool:
    projection = expression.this if isinstance(expression, exp.Alias) else expression
    return isinstance(projection, exp.Star) or (
        isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
    )


def _source_output_names(query_sql: str, physical_names: list[str]) -> list[str]:
    """Return names from an explicit SELECT projection for ORDER BY lookup."""
    # Explicit CTAS projections are stored with unique positional names (c1,
    # c2, ...), but ORDER BY still refers to names from the original SELECT.
    # Recover those logical names so ORDER BY expressions can be mapped to the
    # correct physical column positions without changing the stored schema.
    parsed = sqlglot.parse_one(query_sql, read="presto")
    select = next(parsed.find_all(exp.Select))
    has_wildcard = any(_is_wildcard_projection(expression) for expression in select.expressions)
    if len(select.expressions) != len(physical_names) or has_wildcard:
        return physical_names
    return [
        expression.alias_or_name or physical_names[position] for position, expression in enumerate(select.expressions)
    ]


def _write_canonical_result(query_dir: Path, target: Path, query_sql: str | None) -> None:
    parquet_parts = sorted(query_dir.rglob("*.parquet"))
    sort_spec: tuple[list[int], list[bool], list[bool]] = ([], [], [])

    if parquet_parts and query_sql is not None:
        column_names = _source_output_names(query_sql, pq.read_schema(parquet_parts[0]).names)
        sort_spec = get_orderby_sort_spec(query_sql, column_names)

    temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    try:
        # A single unordered part is already a canonical Parquet file. Copying
        # it avoids decoding and rewriting large results.
        if len(parquet_parts) == 1 and not sort_spec[0]:
            shutil.copy2(parquet_parts[0], temporary)
        else:
            frame = pd.read_parquet(query_dir)
            if sort_spec[0]:
                frame = restore_orderby(frame, *sort_spec)
            frame.to_parquet(temporary, index=False)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_ctas_results(
    source_dir: Path,
    output_dir: Path,
    queries: dict[str, str],
) -> list[Path]:
    """Convert each committed qN CTAS table into one canonical Parquet file."""
    query_dirs = _query_directories(source_dir)
    if not query_dirs:
        raise ValueError(f"No committed CTAS result tables found in {source_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    normalized: list[Path] = []
    for query_dir in query_dirs:
        target = output_dir / f"{query_dir.name}.parquet"
        _write_canonical_result(query_dir, target, queries.get(query_dir.name.upper()))
        shutil.rmtree(query_dir)
        normalized.append(target)
    return normalized
