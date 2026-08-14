#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DATA_TOOLS_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$BENCHMARK_DATA_TOOLS_DIR/.."
RUN_PY_SCRIPT="$REPO_ROOT/scripts/run_py_script.sh"
GENERATE_PY="$BENCHMARK_DATA_TOOLS_DIR/generate_decimal_overflow_dataset.py"

# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/py_env_functions.sh"

show_help() {
  cat <<EOF
Usage: $(basename "$0") [--reset-venv] --data-dir-path DIR [options]

Generate a custom decimal overflow / division-by-zero Parquet dataset for
comparing velox main vs feat/cudf-decimal-overflow-checks.

Wrapper options:
  --reset-venv     Recreate the Python virtual environment before running.
  -h, --help       Show this help.

Forwarded options:
  --data-dir-path DIR   Output directory (required). Use something like
                        \$PRESTO_DATA_DIR/decimal_overflow so Presto mounts it.
  --overwrite           Replace the output directory if it already exists.
  --rows-per-case N     Repeat each seed edge-case N times (default: 1).
  --num-groups G        Fill column grp = replica % G for groupby SUM (default: 1).
  --null-rate F         Null column a on fraction F of replicas after the first
                        of each seed (default: 0.0).
  --seed S              RNG seed for null injection (default: 42).

Examples:
  export PRESTO_DATA_DIR=/raid/ajayaseelan/datasets/tpch

  # Tiny correctness set (default):
  $(basename "$0") --data-dir-path "\$PRESTO_DATA_DIR/decimal_overflow" --overwrite

  # Batched kernels + null masks + groupby + timing stress:
  $(basename "$0") --data-dir-path "\$PRESTO_DATA_DIR/decimal_overflow" --overwrite \\
      --rows-per-case 100000 --num-groups 64 --null-rate 0.1

Then register (cluster must be running):
  ../presto/scripts/setup_decimal_overflow_dataset.sh --register-only \\
      -d decimal_overflow -s decimal_overflow
EOF
}

RESET_VENV=false
SCRIPT_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --reset-venv) RESET_VENV=true ;;
    -h|--help) show_help; exit 0 ;;
    *) SCRIPT_ARGS+=("$arg") ;;
  esac
done

cd "$BENCHMARK_DATA_TOOLS_DIR"

if [[ "$RESET_VENV" == true ]]; then
  delete_python_virtual_env
fi

"$RUN_PY_SCRIPT" -p "$GENERATE_PY" -- "${SCRIPT_ARGS[@]}"
