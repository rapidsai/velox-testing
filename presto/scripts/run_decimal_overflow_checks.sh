#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Run the decimal_overflow query set against a live Presto cluster and classify
# each query as ok / overflow / div_by_zero / other_error. Use this to A/B
# velox main vs feat/cudf-decimal-overflow-checks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEFAULT_QUERIES="${REPO_ROOT}/common/testing/queries/decimal_overflow/queries.json"
RUN_PY="${REPO_ROOT}/scripts/run_py_script.sh"
CHECKER_PY="${SCRIPT_DIR}/run_decimal_overflow_checks.py"
PRESTO_REQ="${REPO_ROOT}/presto/testing/requirements.txt"

SCHEMA_NAME="decimal_overflow"
CATALOG="hive"
HOST_NAME="localhost"
PORT="8080"
USER_NAME="decimal_overflow_check"
QUERIES_FILE="$DEFAULT_QUERIES"
EXPECTATIONS_FILE=""
OUTPUT_DIR=""
TAG=""

print_help() {
  cat <<EOF

Usage: $(basename "$0") [OPTIONS]

Execute crafted decimal overflow / div-by-zero queries and write a JSON report
summarizing which cases failed (and why) vs succeeded.

OPTIONS:
  -h, --help              Show this help.
  -s, --schema-name NAME  Hive schema (default: decimal_overflow).
  -c, --catalog NAME      Catalog (default: hive).
  -H, --hostname HOST     Coordinator host (default: localhost).
  -p, --port PORT         Coordinator port (default: 8080).
  -u, --user NAME         Presto user (default: decimal_overflow_check).
  -q, --queries-file PATH Queries JSON (default: common/testing/queries/decimal_overflow/queries.json).
  -e, --expectations PATH Expectations JSON (default: expectations.json beside the queries file).
  -o, --output-dir DIR    Directory for the JSON report (required).
  -t, --tag NAME          Label stored in the report (e.g. velox_main, decimal_overflow_branch).

Example A/B:
  # On decimal-overflow branch workers:
  $(basename "$0") -s decimal_overflow -t decimal_overflow_branch \\
      -o .../velox/scripts/benchmark-results/ocs/decimal_overflow_branch

  # After switching to main workers:
  $(basename "$0") -s decimal_overflow -t velox_main \\
      -o .../velox/scripts/benchmark-results/ocs/decimal_overflow_main

EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) print_help; exit 0 ;;
    -s|--schema-name) SCHEMA_NAME="$2"; shift 2 ;;
    -c|--catalog) CATALOG="$2"; shift 2 ;;
    -H|--hostname) HOST_NAME="$2"; shift 2 ;;
    -p|--port) PORT="$2"; shift 2 ;;
    -u|--user) USER_NAME="$2"; shift 2 ;;
    -q|--queries-file) QUERIES_FILE="$2"; shift 2 ;;
    -e|--expectations) EXPECTATIONS_FILE="$2"; shift 2 ;;
    -o|--output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    -t|--tag) TAG="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; print_help; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
  echo "Error: --output-dir is required." >&2
  print_help
  exit 1
fi

if [[ ! -f "$QUERIES_FILE" ]]; then
  echo "Error: queries file not found: $QUERIES_FILE" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

EXTRA_ARGS=()
if [[ -n "$EXPECTATIONS_FILE" ]]; then
  if [[ ! -f "$EXPECTATIONS_FILE" ]]; then
    echo "Error: expectations file not found: $EXPECTATIONS_FILE" >&2
    exit 1
  fi
  EXTRA_ARGS+=(--expectations-file "$EXPECTATIONS_FILE")
fi

"$RUN_PY" -p "$CHECKER_PY" -r "$PRESTO_REQ" -- \
  --host "$HOST_NAME" \
  --port "$PORT" \
  --user "$USER_NAME" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA_NAME" \
  --queries-file "$QUERIES_FILE" \
  --output-dir "$OUTPUT_DIR" \
  --tag "$TAG" \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
