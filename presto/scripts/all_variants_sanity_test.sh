#!/bin/bash

set -e

function cleanup() {
  ./stop_presto.sh
  rm -rf .venv
  rm -f node_response.json
}

trap cleanup EXIT

function wait_for_worker_node_registration() {
  local -r MAX_RETRIES=5
  local retry_count=0
  until curl -s -f -o node_response.json http://localhost:8080/v1/node && \
        (( $(jq length node_response.json) > 0 )); do
    if (( $retry_count >= $MAX_RETRIES )); then
      echo "Error: Worker node not registered"
      exit 1
    fi
    sleep 5
    retry_count=$(( retry_count + 1 ))
  done
}

rm -rf .venv
python3 -m venv .venv

source .venv/bin/activate

pip install -r ../testing/integration_tests/requirements.txt

startup_scripts=(deployment/start_java_presto.sh deployment/start_native_cpu_presto.sh deployment/start_native_gpu_presto.sh)
for startup_script in ${startup_scripts[@]}; do
  ./$startup_script
  wait_for_worker_node_registration
  echo -e "\nExecuting sanity test ($startup_script)..."
  pytest ../testing/integration_tests/sanity_test.py
  echo -e "Sanity test completed\n"
done
