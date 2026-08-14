# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Generate a Parquet dataset that stresses GPU decimal arithmetic.

Standard TPC-H decimal data stays inside DECIMAL(15,2) bounds, so ADD/SUB/MUL
overflow and DIV/MOD-by-zero never fire. This generator writes crafted rows that
exercise those edge cases so feat/cudf-decimal-overflow-checks can be compared
against velox main.

Result-type widening matters when picking expectations. For DECIMAL(15,2)
operands the engine widens the result (add/sub -> DECIMAL(16,2), multiply ->
DECIMAL(30,4)), so boundary values there cannot overflow the arithmetic kernel;
those rows are labelled expect="ok". Genuine ADD/SUB/MUL overflow needs operands
already at precision 38, where the result type is capped and cannot widen.

Scale the dataset with --rows-per-case / --num-groups / --null-rate to exercise:
  * multi-row / batched kernels (many edge rows in one scan)
  * null masks mixed with overflow / div0 rows
  * groupby SUM across many groups
  * coarse timing (same edge values, just more of them)

Tables:
  decimal_cases       DECIMAL(15,2)  widening + divide/modulo by zero
  decimal_wide        DECIMAL(38,0)  real add/sub/multiply overflow
  decimal_wide_scaled DECIMAL(38,6)  rescale / divide overflow

Layout (Hive-style, same as TPC-H datasets under PRESTO_DATA_DIR):

  <data-dir>/
    metadata.json
    decimal_cases/decimal_cases-1.parquet
    decimal_wide/decimal_wide-1.parquet
    decimal_wide_scaled/decimal_wide_scaled-1.parquet

Register with the companion setup_decimal_overflow_dataset.sh wrapper (it uses
-b tpcds so nullable columns are preserved).
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# DECIMAL(15,2) absolute max / min.
DEC15_2_MAX = Decimal("9999999999999.99")
DEC15_2_MIN = Decimal("-9999999999999.99")

# DECIMAL(38,0) near the top of the range.
DEC38_MAX = Decimal("99999999999999999999999999999999999999")
DEC38_MIN = Decimal("-99999999999999999999999999999999999999")
DEC38_NEAR = Decimal("10000000000000000000000000000000000000")  # 10^37

# DECIMAL(38,6): 32 integer digits + 6 fractional digits.
DEC38_6_MAX = Decimal("99999999999999999999999999999999.999999")
DEC38_6_TINY = Decimal("0.000001")

Row = dict[str, Any]


def _decimal128(values: list[Decimal | None], precision: int, scale: int) -> pa.Array:
    return pa.array(values, type=pa.decimal128(precision, scale))


def seed_decimal_cases_rows() -> list[Row]:
    """DECIMAL(15,2) seed rows: result widening plus divide/modulo by zero."""
    return [
        {
            "case_id": 1,
            "tag": "add_ok",
            "expect": "ok",
            "op": "add",
            "a": Decimal("1.00"),
            "b": Decimal("2.00"),
            "note": "simple add",
        },
        {
            "case_id": 2,
            "tag": "sub_ok",
            "expect": "ok",
            "op": "sub",
            "a": Decimal("10.00"),
            "b": Decimal("3.50"),
            "note": "simple sub",
        },
        {
            "case_id": 3,
            "tag": "mul_ok",
            "expect": "ok",
            "op": "mul",
            "a": Decimal("12.34"),
            "b": Decimal("5.00"),
            "note": "simple mul",
        },
        {
            "case_id": 4,
            "tag": "div_ok",
            "expect": "ok",
            "op": "div",
            "a": Decimal("100.00"),
            "b": Decimal("4.00"),
            "note": "simple div",
        },
        {
            "case_id": 5,
            "tag": "mod_ok",
            "expect": "ok",
            "op": "mod",
            "a": Decimal("100.00"),
            "b": Decimal("30.00"),
            "note": "simple mod",
        },
        {
            "case_id": 6,
            "tag": "null_ok",
            "expect": "ok",
            "op": "add",
            "a": None,
            "b": Decimal("1.00"),
            "note": "null lhs propagates",
        },
        {
            "case_id": 101,
            "tag": "add_widen_ok",
            "expect": "ok",
            "op": "add",
            "a": DEC15_2_MAX,
            "b": Decimal("0.01"),
            "note": "max + 0.01 fits DECIMAL(16,2) result type",
        },
        {
            "case_id": 102,
            "tag": "add_widen_ok_neg",
            "expect": "ok",
            "op": "add",
            "a": DEC15_2_MIN,
            "b": Decimal("-0.01"),
            "note": "min + (-0.01) fits DECIMAL(16,2) result type",
        },
        {
            "case_id": 103,
            "tag": "sub_widen_ok",
            "expect": "ok",
            "op": "sub",
            "a": DEC15_2_MIN,
            "b": Decimal("0.01"),
            "note": "min - 0.01 fits DECIMAL(16,2) result type",
        },
        {
            "case_id": 104,
            "tag": "sub_widen_ok_pos",
            "expect": "ok",
            "op": "sub",
            "a": DEC15_2_MAX,
            "b": Decimal("-0.01"),
            "note": "max - (-0.01) fits DECIMAL(16,2) result type",
        },
        {
            "case_id": 105,
            "tag": "mul_widen_ok",
            "expect": "ok",
            "op": "mul",
            "a": Decimal("99999999.99"),
            "b": Decimal("99999999.99"),
            "note": "product fits the widened DECIMAL(30,4) result type",
        },
        {
            "case_id": 106,
            "tag": "mul_widen_ok_boundary",
            "expect": "ok",
            "op": "mul",
            "a": Decimal("100000000.00"),
            "b": Decimal("100000.00"),
            "note": "1e8 * 1e5 fits the widened result type",
        },
        {
            "case_id": 201,
            "tag": "div_zero",
            "expect": "div_by_zero",
            "op": "div",
            "a": Decimal("1.00"),
            "b": Decimal("0.00"),
            "note": "divide by zero",
        },
        {
            "case_id": 202,
            "tag": "div_zero_neg",
            "expect": "div_by_zero",
            "op": "div",
            "a": Decimal("-42.50"),
            "b": Decimal("0.00"),
            "note": "negative / zero",
        },
        {
            "case_id": 203,
            "tag": "mod_zero",
            "expect": "div_by_zero",
            "op": "mod",
            "a": Decimal("123.45"),
            "b": Decimal("0.00"),
            "note": "modulus by zero",
        },
        {
            "case_id": 204,
            "tag": "mod_zero_null_lhs",
            "expect": "ok",
            "op": "mod",
            "a": None,
            "b": Decimal("0.00"),
            "note": "null % 0 — usually null, not an error",
        },
        {
            "case_id": 301,
            "tag": "rescale_widen_ok",
            "expect": "ok",
            "op": "add",
            "a": DEC15_2_MAX,
            "b": Decimal("0.01"),
            "note": "operand rescale at the DECIMAL(15,2) limit; result widens",
        },
    ]


def seed_decimal_wide_rows() -> list[Row]:
    """DECIMAL(38,0) seed rows: operands already at max precision."""
    return [
        {
            "case_id": 1,
            "tag": "wide_add_ok",
            "expect": "ok",
            "op": "add",
            "a": Decimal("1"),
            "b": Decimal("2"),
            "note": "wide add ok",
        },
        {
            "case_id": 2,
            "tag": "wide_mul_ok",
            "expect": "ok",
            "op": "mul",
            "a": Decimal("1000"),
            "b": Decimal("1000"),
            "note": "wide mul ok",
        },
        {
            "case_id": 3,
            "tag": "wide_sub_ok",
            "expect": "ok",
            "op": "sub",
            "a": Decimal("1000"),
            "b": Decimal("1"),
            "note": "wide sub ok",
        },
        {
            "case_id": 101,
            "tag": "wide_add_overflow",
            "expect": "overflow",
            "op": "add",
            "a": DEC38_MAX,
            "b": Decimal("1"),
            "note": "DECIMAL(38,0) max + 1",
        },
        {
            "case_id": 102,
            "tag": "wide_mul_overflow",
            "expect": "overflow",
            "op": "mul",
            "a": DEC38_NEAR,
            "b": Decimal("10"),
            "note": "10^37 * 10 overflows DECIMAL(38,0)",
        },
        {
            "case_id": 103,
            "tag": "wide_sub_overflow",
            "expect": "overflow",
            "op": "sub",
            "a": DEC38_MIN,
            "b": Decimal("1"),
            "note": "DECIMAL(38,0) min - 1",
        },
        {
            "case_id": 104,
            "tag": "wide_sub_overflow_pos",
            "expect": "overflow",
            "op": "sub",
            "a": DEC38_MAX,
            "b": Decimal("-1"),
            "note": "DECIMAL(38,0) max - (-1)",
        },
        {
            "case_id": 201,
            "tag": "wide_div_zero",
            "expect": "div_by_zero",
            "op": "div",
            "a": Decimal("1"),
            "b": Decimal("0"),
            "note": "wide divide by zero",
        },
        {
            "case_id": 202,
            "tag": "wide_mod_zero",
            "expect": "div_by_zero",
            "op": "mod",
            "a": DEC38_MAX,
            "b": Decimal("0"),
            "note": "wide modulus by zero",
        },
    ]


def seed_decimal_wide_scaled_rows() -> list[Row]:
    """DECIMAL(38,6) seed rows: rescale and divide overflow."""
    return [
        {
            "case_id": 1,
            "tag": "scaled_add_ok",
            "expect": "ok",
            "op": "add",
            "a": Decimal("1.500000"),
            "b": Decimal("2.250000"),
            "note": "scaled add ok",
        },
        {
            "case_id": 2,
            "tag": "scaled_div_ok",
            "expect": "ok",
            "op": "div",
            "a": Decimal("100.000000"),
            "b": Decimal("4.000000"),
            "note": "scaled divide ok",
        },
        {
            "case_id": 101,
            "tag": "scaled_add_overflow",
            "expect": "overflow",
            "op": "add",
            "a": DEC38_6_MAX,
            "b": DEC38_6_TINY,
            "note": "DECIMAL(38,6) max + 0.000001",
        },
        {
            "case_id": 102,
            "tag": "scaled_div_overflow",
            "expect": "overflow",
            "op": "div",
            "a": DEC38_6_MAX,
            "b": DEC38_6_TINY,
            "note": "max / 0.000001 needs far more than 38 digits",
        },
        {
            "case_id": 103,
            "tag": "scaled_mul_overflow",
            "expect": "overflow",
            "op": "mul",
            "a": DEC38_6_MAX,
            "b": Decimal("10.000000"),
            "note": "max * 10 overflows DECIMAL(38,6)",
        },
        {
            "case_id": 201,
            "tag": "scaled_div_zero",
            "expect": "div_by_zero",
            "op": "div",
            "a": Decimal("1.000000"),
            "b": Decimal("0.000000"),
            "note": "scaled divide by zero",
        },
    ]


def expand_rows(
    seed_rows: list[Row],
    *,
    rows_per_case: int,
    num_groups: int,
    null_rate: float,
    rng: random.Random,
) -> list[Row]:
    """Repeat seed rows and attach grp / replica columns for stress modes.

    Null injection only clears column ``a`` on replicas after the first copy of
    each seed, and never on rows that already have a null ``a``. That keeps the
    original seed row intact so tag-filtered single-behavior queries still see
    a non-null overflow / div0 operand, while larger batches mix null masks.
    """
    if rows_per_case < 1:
        raise SystemExit("--rows-per-case must be >= 1")
    if num_groups < 1:
        raise SystemExit("--num-groups must be >= 1")
    if not 0.0 <= null_rate <= 1.0:
        raise SystemExit("--null-rate must be in [0.0, 1.0]")

    expanded: list[Row] = []
    for seed in seed_rows:
        for replica in range(rows_per_case):
            row = dict(seed)
            # Stable unique id: seed case_id in the high bits, replica in the low.
            row["case_id"] = int(seed["case_id"]) * 1_000_000 + replica
            row["replica"] = replica
            row["grp"] = replica % num_groups
            if (
                replica > 0
                and row["a"] is not None
                and null_rate > 0.0
                and rng.random() < null_rate
            ):
                row["a"] = None
                row["note"] = f"{seed['note']} [null-mask replica]"
            expanded.append(row)
    return expanded


def rows_to_table(
    rows: list[Row], *, precision: int, scale: int
) -> pa.Table:
    return pa.table(
        {
            "case_id": pa.array([r["case_id"] for r in rows], type=pa.int64()),
            "grp": pa.array([r["grp"] for r in rows], type=pa.int64()),
            "replica": pa.array([r["replica"] for r in rows], type=pa.int64()),
            "tag": pa.array([r["tag"] for r in rows], type=pa.string()),
            "expect": pa.array([r["expect"] for r in rows], type=pa.string()),
            "op": pa.array([r["op"] for r in rows], type=pa.string()),
            "a": _decimal128([r["a"] for r in rows], precision, scale),
            "b": _decimal128([r["b"] for r in rows], precision, scale),
            "note": pa.array([r["note"] for r in rows], type=pa.string()),
        }
    )


def write_table(table: pa.Table, table_dir: Path, table_name: str) -> Path:
    table_dir.mkdir(parents=True, exist_ok=True)
    out = table_dir / f"{table_name}-1.parquet"
    # Cap row-group size so large --rows-per-case still produces multiple
    # row groups for scan/batch coverage instead of one giant group.
    pq.write_table(table, out, compression="zstd", row_group_size=64 * 1024)
    return out


def generate(
    data_dir: Path,
    *,
    overwrite: bool,
    rows_per_case: int,
    num_groups: int,
    null_rate: float,
    seed: int,
) -> None:
    if data_dir.exists():
        if not overwrite:
            raise SystemExit(
                f"Refusing to overwrite existing directory: {data_dir}\n"
                f"Pass --overwrite to replace it."
            )
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)

    rng = random.Random(seed)
    expand_kwargs = {
        "rows_per_case": rows_per_case,
        "num_groups": num_groups,
        "null_rate": null_rate,
        "rng": rng,
    }

    cases = rows_to_table(
        expand_rows(seed_decimal_cases_rows(), **expand_kwargs),
        precision=15,
        scale=2,
    )
    wide = rows_to_table(
        expand_rows(seed_decimal_wide_rows(), **expand_kwargs),
        precision=38,
        scale=0,
    )
    wide_scaled = rows_to_table(
        expand_rows(seed_decimal_wide_scaled_rows(), **expand_kwargs),
        precision=38,
        scale=6,
    )

    cases_path = write_table(cases, data_dir / "decimal_cases", "decimal_cases")
    wide_path = write_table(wide, data_dir / "decimal_wide", "decimal_wide")
    wide_scaled_path = write_table(
        wide_scaled, data_dir / "decimal_wide_scaled", "decimal_wide_scaled"
    )

    metadata = {
        "dataset": "decimal_overflow",
        "description": (
            "Crafted decimal rows for overflow / division-by-zero regression testing "
            "between velox main and feat/cudf-decimal-overflow-checks."
        ),
        "scale": {
            "rows_per_case": rows_per_case,
            "num_groups": num_groups,
            "null_rate": null_rate,
            "seed": seed,
            "seed_case_count": {
                "decimal_cases": len(seed_decimal_cases_rows()),
                "decimal_wide": len(seed_decimal_wide_rows()),
                "decimal_wide_scaled": len(seed_decimal_wide_scaled_rows()),
            },
        },
        "tables": {
            "decimal_cases": {
                "rows": cases.num_rows,
                "file": str(cases_path.name),
                "columns": {
                    "a": "DECIMAL(15,2)",
                    "b": "DECIMAL(15,2)",
                    "grp": "BIGINT",
                    "replica": "BIGINT",
                },
            },
            "decimal_wide": {
                "rows": wide.num_rows,
                "file": str(wide_path.name),
                "columns": {
                    "a": "DECIMAL(38,0)",
                    "b": "DECIMAL(38,0)",
                    "grp": "BIGINT",
                    "replica": "BIGINT",
                },
            },
            "decimal_wide_scaled": {
                "rows": wide_scaled.num_rows,
                "file": str(wide_scaled_path.name),
                "columns": {
                    "a": "DECIMAL(38,6)",
                    "b": "DECIMAL(38,6)",
                    "grp": "BIGINT",
                    "replica": "BIGINT",
                },
            },
        },
        "expect_values": ["ok", "overflow", "div_by_zero"],
        "notes": (
            "DECIMAL(15,2) operands cannot overflow arithmetic: the result type "
            "widens (add/sub -> (16,2), multiply -> (30,4)). Real overflow "
            "coverage lives in decimal_wide and decimal_wide_scaled, where "
            "operands are already at precision 38. Use --rows-per-case / "
            "--num-groups / --null-rate to stress batched kernels, null masks, "
            "groupby SUM, and coarse timing without changing edge values."
        ),
    }
    (data_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    print(f"Wrote dataset under {data_dir}")
    print(
        f"  scale: rows_per_case={rows_per_case} num_groups={num_groups} "
        f"null_rate={null_rate} seed={seed}"
    )
    print(f"  decimal_cases:       {cases.num_rows} rows -> {cases_path}")
    print(f"  decimal_wide:        {wide.num_rows} rows -> {wide_path}")
    print(f"  decimal_wide_scaled: {wide_scaled.num_rows} rows -> {wide_scaled_path}")
    print(f"  metadata:            {data_dir / 'metadata.json'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a custom decimal overflow / division-by-zero Parquet dataset "
            "for comparing velox main vs a decimal-overflow branch."
        )
    )
    parser.add_argument(
        "--data-dir-path",
        type=Path,
        required=True,
        help=(
            "Output directory (Hive layout). Typically "
            "$PRESTO_DATA_DIR/decimal_overflow so Presto can mount it as user_data."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the output directory if it already exists.",
    )
    parser.add_argument(
        "--rows-per-case",
        type=int,
        default=1,
        help=(
            "Repeat each seed edge-case this many times (default: 1). "
            "Use 10k–100k for batched-kernel and coarse timing stress."
        ),
    )
    parser.add_argument(
        "--num-groups",
        type=int,
        default=1,
        help=(
            "Assign replica %% num_groups into column grp for groupby SUM stress "
            "(default: 1)."
        ),
    )
    parser.add_argument(
        "--null-rate",
        type=float,
        default=0.0,
        help=(
            "Fraction of replicas (after the first of each seed) whose column a "
            "is set to NULL, mixing null masks with overflow/div0 rows "
            "(default: 0.0)."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for null-mask injection (default: 42).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate(
        args.data_dir_path.resolve(),
        overwrite=args.overwrite,
        rows_per_case=args.rows_per_case,
        num_groups=args.num_groups,
        null_rate=args.null_rate,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
