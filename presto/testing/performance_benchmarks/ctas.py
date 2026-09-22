# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Helpers for running Presto benchmark queries as temporary CTAS tables."""

import subprocess
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import exp

from common.testing.normalize_ctas_results import normalize_ctas_results

CTAS_CATALOG = "hive_output"
# This schema is temporary and reset before every CTAS benchmark. The regular
# benchmark --tag applies only to permanent output artifacts.
CTAS_SCHEMA = "benchmark_results"


@dataclass(frozen=True)
class CtasResults:
    catalog: str
    schema: str


def strip_trailing_semicolon(query):
    return query.rstrip().removesuffix(";").rstrip()


def ctas_table_name(query_id, iteration_num):
    return query_id.lower() if iteration_num == 0 else f"{query_id.lower()}_iteration_{iteration_num + 1}"


def _is_wildcard_projection(expression: exp.Expression) -> bool:
    projection = expression.this if isinstance(expression, exp.Alias) else expression
    return isinstance(projection, exp.Star) or (
        isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
    )


def ctas_output_column_aliases(query):
    """Return unique CTAS aliases without modifying the original SELECT.

    A wildcard's expanded column count depends on source schema metadata, so
    wildcard projections keep their original output names instead.
    """
    parsed = sqlglot.parse_one(strip_trailing_semicolon(query), read="presto")
    select = next(parsed.find_all(exp.Select))
    if any(_is_wildcard_projection(expression) for expression in select.expressions):
        return None
    return [f"c{position}" for position in range(1, len(select.expressions) + 1)]


def build_ctas_query(benchmark_type, query_id, query, catalog, schema, table):
    query = strip_trailing_semicolon(query)
    aliases = ctas_output_column_aliases(query)
    column_aliases = f" ({', '.join(aliases)})" if aliases else ""
    return (
        f"--{benchmark_type}_{query_id}--\n"
        f"CREATE TABLE {catalog}.{schema}.{table}{column_aliases} WITH (format = 'PARQUET') AS\n"
        f"{query}"
    )


def drop_results_schema(cursor, catalog, schema):
    schemas = {row[0] for row in cursor.execute(f"SHOW SCHEMAS FROM {catalog}").fetchall()}
    if schema not in schemas:
        return
    tables = cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}").fetchall()
    for (table,) in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {catalog}.{schema}."{table}"').fetchall()
    cursor.execute(f"DROP SCHEMA {catalog}.{schema}").fetchall()


def finalize_ctas_results(source_dir: Path, output_dir: Path, queries: dict[str, str]) -> list[Path]:
    """Prepare worker output for host access and write canonical result files."""
    script = Path(__file__).parents[2] / "scripts" / "prepare_ctas_results.sh"
    subprocess.run(["bash", script, source_dir], check=True)
    return normalize_ctas_results(source_dir, output_dir, queries)
