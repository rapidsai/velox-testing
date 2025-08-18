#!/bin/bash

set -e

function cleanup() {
  ./stop_presto.sh
  rm -rf .venv
  rm -f node_response.json
}

trap cleanup EXIT

source ./common_functions.sh

rm -rf .venv
python3 -m venv .venv

source .venv/bin/activate

pip install -r ../testing/integration_tests/requirements.txt

startup_scripts=(start_java_presto.sh start_native_cpu_presto.sh start_native_gpu_presto.sh)
for startup_script in ${startup_scripts[@]}; do
  ./$startup_script
  wait_for_worker_node_registration
  echo -e "\nExecuting sanity test ($startup_script)..."
  pytest ../testing/integration_tests/sanity_test.py
  echo -e "Sanity test completed\n"
done
