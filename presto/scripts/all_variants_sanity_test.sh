#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/presto_connection_defaults.sh"

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
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
      *)
        echo "Error: Unknown argument $1"
        echo "Usage: $0 [-H|--hostname <hostname>] [-p|--port <port>]"
        exit 1
        ;;
    esac
  done
}

parse_args "$@"
set_presto_coordinator_defaults

function cleanup() {
  "${SCRIPT_DIR}/stop_presto.sh"
  rm -rf .venv
  rm -f node_response.json
}

trap cleanup EXIT

source "${SCRIPT_DIR}/common_functions.sh"

rm -rf .venv
python3 -m venv .venv

source .venv/bin/activate

pip install -r "${SCRIPT_DIR}/../testing/requirements.txt"

startup_scripts=(start_java_presto.sh start_native_cpu_presto.sh start_native_gpu_presto.sh)
for startup_script in ${startup_scripts[@]}; do
  "${SCRIPT_DIR}/$startup_script"
  wait_for_worker_node_registration "$HOST_NAME" "$PORT"
  echo -e "\nExecuting sanity test ($startup_script)..."
  pytest "${SCRIPT_DIR}/../testing/integration_tests/sanity_test.py"
  echo -e "Sanity test completed\n"
done
