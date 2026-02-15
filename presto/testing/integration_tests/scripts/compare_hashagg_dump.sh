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

Compare HashAgg dump input sums against DuckDB.
This wrapper creates an isolated Python environment, installs test dependencies,
and runs the dump comparison script.

OPTIONS:
    -h, --help                    Show this help message.
    --dump-dir DIR                Dump directory containing manifest.txt (required).
    --schema-name SCHEMA          Hive schema to locate lineitem parquet.
    --lineitem-path DIR           Path to lineitem parquet (overrides schema).
    --hostname HOST               Presto coordinator hostname.
    --port PORT                   Presto coordinator port.
    --user USER                   Presto user.
    --batch-size N                DuckDB fetch batch size.
    --max-dense-keys N            Max key range size for dense sums.

EXAMPLES:
    $0 --dump-dir /tmp/hashagg_dump/hashagg_main_000008 --schema-name decimal_sf100
    $0 --dump-dir /tmp/hashagg_dump/hashagg_main_000008 --lineitem-path /data/lineitem

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
SCRIPT_PATH="${SCRIPT_DIR}/../compare_hashagg_dump.py"
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
