#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script runs timestamp support verification tests against a Presto cluster
with a native (velox-cudf) worker.

OPTIONS:
    -h, --help          Show this help message.
    -H, --hostname      Hostname of the Presto coordinator. Default: localhost
    -p, --port          Port number of the Presto coordinator. Default: 8080
    -u, --user          User who queries will be executed as. Default: test_user
    --catalog           Catalog to use. Default: tpch
    --schema            Schema to use. Default: sf1
    --reuse-venv        Reuse existing Python virtual environment if one exists.

EXAMPLES:
    $0
    $0 -H myhost.com -p 8080
    $0 -H myhost.com --catalog hive --schema default
    $0 -h

EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -H|--hostname)
        if [[ -n $2 ]]; then
          HOST_NAME=$2
          shift 2
        else
          echo "Error: --hostname requires a value"
          exit 1
        fi
        ;;
      -p|--port)
        if [[ -n $2 ]]; then
          PORT=$2
          shift 2
        else
          echo "Error: --port requires a value"
          exit 1
        fi
        ;;
      -u|--user)
        if [[ -n $2 ]]; then
          USER_NAME=$2
          shift 2
        else
          echo "Error: --user requires a value"
          exit 1
        fi
        ;;
      --catalog)
        if [[ -n $2 ]]; then
          CATALOG=$2
          shift 2
        else
          echo "Error: --catalog requires a value"
          exit 1
        fi
        ;;
      --schema)
        if [[ -n $2 ]]; then
          SCHEMA=$2
          shift 2
        else
          echo "Error: --schema requires a value"
          exit 1
        fi
        ;;
      --reuse-venv)
        REUSE_VENV=true
        shift
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

set_presto_coordinator_defaults

USER_NAME="${USER_NAME:-test_user}"
CATALOG="${CATALOG:-tpch}"
SCHEMA="${SCHEMA:-sf1}"

source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

VENV_DIR=".timestamp_test_venv"

if [[ "$REUSE_VENV" != "true" ]]; then
  trap 'delete_python_virtual_env "$VENV_DIR"' EXIT
fi

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")

if [[ ! -d $VENV_DIR || "$REUSE_VENV" != "true" ]]; then
  init_python_virtual_env $VENV_DIR

  PIP_TIMEOUT="${PIP_TIMEOUT:-120}"
  PIP_RETRIES="${PIP_RETRIES:-8}"
  python -m pip install \
    --disable-pip-version-check \
    --no-input \
    --progress-bar off \
    --retries "${PIP_RETRIES}" \
    --timeout "${PIP_TIMEOUT}" \
    -q -r "${TEST_DIR}/requirements.txt"
else
  activate_python_virtual_env $VENV_DIR
fi

source "${SCRIPT_DIR}/common_functions.sh"

wait_for_worker_node_registration "$HOST_NAME" "$PORT"

python "${TEST_DIR}/timestamp_status_test.py" \
  --host "$HOST_NAME" \
  --port "$PORT" \
  --user "$USER_NAME" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA"
