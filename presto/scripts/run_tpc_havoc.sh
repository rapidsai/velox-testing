#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"

print_help() {
  cat << EOF

Usage: $0 -s SCHEMA [-e ENGINE] [OPTIONS]

Runs the TPC-Havoc benchmark (BenchBox) against a running Presto cluster: every selected TPC-H base
query and its semantically equivalent variants run sequentially, and each result is validated against
its base query (optionally also against DuckDB and an earlier run, e.g. GPU vs CPU). Failures not in
presto/testing/tpc_havoc/known_issues.yaml make the script exit with 1 (2 = setup/usage error).
Results go to <output-dir>/<tag>/summary.{json,md}. A query that fails because velox-cudf cannot run
part of its plan gets the reason from the worker logs (\${LOGS_DIR:-presto/scripts/presto_logs}).

Main options (every option of presto/testing/tpc_havoc/run_tpc_havoc.py is accepted):
    -s, --schema-name       Schema containing the TPC-H tables (required), e.g. tpch_sf10.
    -e, --engine            cpu, gpu, gpu-no-fallback (GPU worker started with --no-cpu-fallback) or java.
    -q, --queries           "N"/"QN" = base query and all its variants, "QN_VM" = one variant. Default: all.
    -o, --output-dir DIR, -t, --tag TAG, -H, --hostname HOST, -p, --port PORT
    --duckdb-data-dir DIR   Also validate against DuckDB on this Parquet data (one sub-directory per table).
    --duckdb-cache-dir DIR  Cache the DuckDB results.
    --reference-dir DIR     Compare results with an earlier run (e.g. the CPU run).
    --known-issues FILE, --timeout SECONDS, --session KEY=VALUE, --fail-on-any, --worker-log GLOB

Options of this script:
    --benchbox-path DIR     Install BenchBox from a local checkout (editable) instead of the pinned commit.
    --reuse-venv            Reuse the Python virtual environment (.tpc_havoc_venv) if it exists, and keep it.
    --skip-worker-check     Don't wait for a worker node to register with the coordinator.
    -h, --help              Show this help message.

EXAMPLES:
    $0 -s tpch_sf10 -e cpu -t cpu --duckdb-data-dir /data/tpch/sf10
    $0 -s tpch_sf10 -e gpu -t gpu --reference-dir tpc_havoc_output/cpu -q "1,6,Q13_V4"

EOF
}

# velox-cudf logs why it cannot run an operator to the worker logs written by start_presto_helper.sh.
# Passed first so that a --worker-log given by the caller takes precedence.
RUNNER_ARGS=(--worker-log "${LOGS_DIR:-${SCRIPT_DIR}/presto_logs}/worker_*.log")
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help) print_help; exit 0 ;;
    --benchbox-path) BENCHBOX_PATH=$2; shift 2 ;;
    --reuse-venv) REUSE_VENV=true; shift ;;
    --skip-worker-check) SKIP_WORKER_CHECK=true; shift ;;
    -H|--hostname) HOST_NAME=$2; RUNNER_ARGS+=("$1" "$2"); shift 2 ;;
    -p|--port) PORT=$2; RUNNER_ARGS+=("$1" "$2"); shift 2 ;;
    --) shift ;;
    *) RUNNER_ARGS+=("$1"); shift ;;
  esac
done

if [[ -n ${BENCHBOX_PATH} && ! -f ${BENCHBOX_PATH}/pyproject.toml ]]; then
  echo "Error: --benchbox-path is not a BenchBox checkout: ${BENCHBOX_PATH}"
  exit 2
fi

set_presto_coordinator_defaults
export HOST_NAME PORT

source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

VENV_DIR=".tpc_havoc_venv"
TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing/tpc_havoc")
REQUIREMENTS_FILE="${TEST_DIR}/requirements.txt"

if [[ "${REUSE_VENV}" != "true" ]]; then
  trap 'delete_python_virtual_env "$VENV_DIR"' EXIT
fi

if [[ ! -d ${VENV_DIR} || "${REUSE_VENV}" != "true" ]]; then
  init_python_virtual_env "${VENV_DIR}"
  PIP_ARGS=(--disable-pip-version-check --no-input --progress-bar off -q
            --retries "${PIP_RETRIES:-8}" --timeout "${PIP_TIMEOUT:-120}")
  if [[ -n ${BENCHBOX_PATH} ]]; then
    # Dev override: everything but the pinned BenchBox, then the local checkout (editable).
    grep -v -E '^benchbox[[:space:]]*@' "${REQUIREMENTS_FILE}" > "${VENV_DIR}/requirements_no_benchbox.txt"
    python -m pip install "${PIP_ARGS[@]}" -r "${VENV_DIR}/requirements_no_benchbox.txt"
    python -m pip install "${PIP_ARGS[@]}" -e "$(readlink -f "${BENCHBOX_PATH}")"
  else
    python -m pip install "${PIP_ARGS[@]}" -r "${REQUIREMENTS_FILE}"
  fi
else
  activate_python_virtual_env "${VENV_DIR}"
fi

if [[ "${SKIP_WORKER_CHECK}" != "true" ]]; then
  source "${SCRIPT_DIR}/common_functions.sh"
  wait_for_worker_node_registration "${HOST_NAME}" "${PORT}"
fi

python "${TEST_DIR}/run_tpc_havoc.py" "${RUNNER_ARGS[@]}"
