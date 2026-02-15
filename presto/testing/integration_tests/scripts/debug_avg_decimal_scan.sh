#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

cleanup() {
  rm -rf .venv
}

trap cleanup EXIT

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Run exponential l_partkey range scans and compare Presto vs DuckDB for decimal avg behavior.
This wrapper creates an isolated Python environment, installs test dependencies, and runs
the integration debug script.

OPTIONS:
    -h, --help                    Show this help message.
    --hostname HOST               Presto coordinator hostname.
    --port PORT                   Presto coordinator port.
    --user USER                   Presto user.
    --mode MODE                   q17_predicate, avg_cast, threshold_correlated_only, threshold_grouped_only, grouped_avg_only, grouped_avg_double_only.
    --schema-name SCHEMA          Existing Hive schema to use (skip auto table creation).
    --keep-tables                 Keep auto-created tables/schema after script exits.
    --max-partkey N               Highest l_partkey to include in scan.
    --single-upper N              Run only one upper bound (no exponential scan).
    --require-min-max-partkey N   Require lineitem max(l_partkey) >= N (default 20000000).
    --decimal-cast TYPE           Decimal type for avg(CAST(l_quantity AS TYPE)).
    --q17-brand VALUE             Brand filter in q17_predicate mode (default Brand#23).
    --q17-container VALUE         Container filter in q17_predicate mode (default MED BOX).
    --q17-threshold-mode MODE     native (Q17 shape) or cast_decimal.
    --q17-filter-mode MODE        none, brand_only, or brand_and_container.
    --range-style MODE            between or bounds (>= and <=).
    --decimal-abs-tol VALUE       Absolute tolerance for decimal avg comparison.
    --double-abs-tol VALUE        Absolute tolerance for double avg comparison.
    --major-decimal-abs-diff V    Threshold for major decimal mismatch detection.
    --major-double-abs-diff V     Threshold for major double mismatch detection.
    --skip-refine-smallest-major  Skip binary search for smallest major prefix.
    --fail-on-any-mismatch        Return non-zero for any mismatch.
    --auto-simplify               Auto-run multiple forms and print repro/non-repro lists.
    --auto-forms LIST             Comma-separated forms for auto simplify.
    --stop-on-mismatch            Stop after the first mismatch.

EXAMPLES:
    $0
    $0 --schema-name tpch_sf100 --max-partkey 33554431
    $0 --decimal-cast "DECIMAL(18, 6)" --decimal-abs-tol 0
    $0 --schema-name decimal_sf100 --single-upper 19412603 --auto-simplify

EOF
}

for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    print_help
    exit 0
  fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTING_DIR="$(readlink -f "${SCRIPT_DIR}/../..")"
SCRIPT_PATH="${SCRIPT_DIR}/../debug_avg_decimal_scan.py"
REQUIREMENTS_PATH="${TESTING_DIR}/requirements.txt"

rm -rf .venv
echo "PROGRESS,phase=wrapper,event=venv_create_start"
python3 -m venv .venv
source .venv/bin/activate
echo "PROGRESS,phase=wrapper,event=venv_create_end"

echo "PROGRESS,phase=wrapper,event=pip_install_start,requirements=${REQUIREMENTS_PATH}"
pip install -r "${REQUIREMENTS_PATH}"
echo "PROGRESS,phase=wrapper,event=pip_install_end"

echo "PROGRESS,phase=wrapper,event=python_start,script=${SCRIPT_PATH}"
python -u "${SCRIPT_PATH}" "$@"
