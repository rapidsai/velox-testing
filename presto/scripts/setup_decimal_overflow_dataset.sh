#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Generate and/or register the custom decimal_overflow dataset with Presto.
# Reuses generate_table_schemas.py + create_hive_tables.py (same path as TPC-H).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GEN_WRAPPER="${REPO_ROOT}/benchmark_data_tools/scripts/generate_decimal_overflow_dataset.sh"
SCHEMA_GEN="${REPO_ROOT}/benchmark_data_tools/generate_table_schemas.py"
CREATE_TABLES="${REPO_ROOT}/presto/testing/integration_tests/create_hive_tables.py"
CREATE_TABLES_REQ="${REPO_ROOT}/presto/testing/requirements.txt"
RUN_PY="${REPO_ROOT}/scripts/run_py_script.sh"
QUERIES_FILE="${REPO_ROOT}/common/testing/queries/decimal_overflow/queries.json"

DATA_DIR_NAME="decimal_overflow"
SCHEMA_NAME="decimal_overflow"
GENERATE=true
REGISTER=true
OVERWRITE=false
ROWS_PER_CASE=""
NUM_GROUPS=""
NULL_RATE=""
SEED=""
HOST_NAME="${HOST_NAME:-localhost}"
PORT="${PORT:-8080}"

print_help() {
  cat <<EOF

Usage: $(basename "$0") [OPTIONS]

Generate a crafted decimal overflow / div-by-zero Parquet dataset under
\$PRESTO_DATA_DIR and register it as a Hive schema for Presto GPU/CPU testing.

OPTIONS:
  -h, --help                 Show this help.
  -d, --data-dir-name NAME   Directory name under \$PRESTO_DATA_DIR (default: decimal_overflow).
  -s, --schema-name NAME     Hive schema name (default: decimal_overflow).
  --generate-only            Only write Parquet; skip Presto registration.
  --register-only            Only register existing Parquet; skip generation.
  --overwrite                Replace an existing data directory when generating.
  --rows-per-case N          Repeat each seed edge-case N times (default: 1).
  --num-groups G             Fill column grp for groupby SUM stress (default: 1).
  --null-rate F              Null column a on fraction F of later replicas (default: 0).
  --seed S                   RNG seed for null injection (default: 42).
  -H, --hostname HOST        Presto coordinator host (default: localhost).
  -p, --port PORT            Presto coordinator port (default: 8080).

Environment:
  PRESTO_DATA_DIR            Required. Host path mounted into Presto as user_data.

Examples:
  export PRESTO_DATA_DIR=/raid/ajayaseelan/datasets/tpch

  # Generate + register against a running cluster:
  $(basename "$0") --overwrite

  # Larger stress set (batch / null masks / groupby / timing):
  $(basename "$0") --overwrite --rows-per-case 100000 --num-groups 64 --null-rate 0.1

  # Generate only:
  $(basename "$0") --generate-only --overwrite

  # Register existing data:
  $(basename "$0") --register-only

  # Run A/B checks (after registration):
  ./run_decimal_overflow_checks.sh -s decimal_overflow -o /tmp/decimal_overflow_results

Queries file: ${QUERIES_FILE}

EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) print_help; exit 0 ;;
    -d|--data-dir-name)
      DATA_DIR_NAME="$2"; shift 2 ;;
    -s|--schema-name)
      SCHEMA_NAME="$2"; shift 2 ;;
    --generate-only)
      GENERATE=true; REGISTER=false; shift ;;
    --register-only)
      GENERATE=false; REGISTER=true; shift ;;
    --overwrite)
      OVERWRITE=true; shift ;;
    --rows-per-case)
      ROWS_PER_CASE="$2"; shift 2 ;;
    --num-groups)
      NUM_GROUPS="$2"; shift 2 ;;
    --null-rate)
      NULL_RATE="$2"; shift 2 ;;
    --seed)
      SEED="$2"; shift 2 ;;
    -H|--hostname)
      HOST_NAME="$2"; shift 2 ;;
    -p|--port)
      PORT="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      print_help
      exit 1
      ;;
  esac
done

if [[ -z "${PRESTO_DATA_DIR:-}" ]]; then
  echo "Error: PRESTO_DATA_DIR must be set to the host directory Presto mounts as user_data." >&2
  print_help
  exit 1
fi

DATA_PATH="${PRESTO_DATA_DIR}/${DATA_DIR_NAME}"

if [[ "$GENERATE" == true ]]; then
  echo "Generating decimal overflow dataset at ${DATA_PATH} ..."
  GEN_ARGS=(--data-dir-path "${DATA_PATH}")
  if [[ "$OVERWRITE" == true ]]; then
    GEN_ARGS+=(--overwrite)
  fi
  if [[ -n "$ROWS_PER_CASE" ]]; then
    GEN_ARGS+=(--rows-per-case "$ROWS_PER_CASE")
  fi
  if [[ -n "$NUM_GROUPS" ]]; then
    GEN_ARGS+=(--num-groups "$NUM_GROUPS")
  fi
  if [[ -n "$NULL_RATE" ]]; then
    GEN_ARGS+=(--null-rate "$NULL_RATE")
  fi
  if [[ -n "$SEED" ]]; then
    GEN_ARGS+=(--seed "$SEED")
  fi
  chmod +x "$GEN_WRAPPER"
  "$GEN_WRAPPER" "${GEN_ARGS[@]}"
fi

if [[ "$REGISTER" != true ]]; then
  echo "Skipping registration (--generate-only)."
  exit 0
fi

if [[ ! -d "${DATA_PATH}/decimal_cases" ]]; then
  echo "Error: expected ${DATA_PATH}/decimal_cases (run without --register-only first)." >&2
  exit 1
fi

echo "Registering schema hive.${SCHEMA_NAME} from ${DATA_PATH} ..."
TEMP_SCHEMA_DIR="$(mktemp -d "${SCRIPT_DIR}/temp-decimal-overflow-schema.XXXXXX")"
cleanup() { rm -rf "$TEMP_SCHEMA_DIR"; }
trap cleanup EXIT

# Use -b tpcds (not tpch): tpch forces NOT NULL on every column, but
# decimal_cases intentionally has nulls in column a (null_ok / mod_zero_null_lhs).
# Table names still come from the Parquet subdirectories.
"$RUN_PY" -p "$SCHEMA_GEN" -- \
  -b tpcds \
  -d "$DATA_PATH" \
  -s "$TEMP_SCHEMA_DIR" \
  -v

HOSTNAME="$HOST_NAME" PORT="$PORT" \
  "$RUN_PY" -p "$CREATE_TABLES" -r "$CREATE_TABLES_REQ" -- \
  --schema-name "$SCHEMA_NAME" \
  --schemas-dir-path "$TEMP_SCHEMA_DIR" \
  --data-dir-name "$DATA_DIR_NAME"

echo
echo "Done. Schema: hive.${SCHEMA_NAME}"
echo "Tables:       decimal_cases, decimal_wide, decimal_wide_scaled"
echo "Queries:      ${QUERIES_FILE}"
echo
echo "Next:"
echo "  ${SCRIPT_DIR}/run_decimal_overflow_checks.sh -s ${SCHEMA_NAME} -t my_tag -o /tmp/decimal_overflow_results"
