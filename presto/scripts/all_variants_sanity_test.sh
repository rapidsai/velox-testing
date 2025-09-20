#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
