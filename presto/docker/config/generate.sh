#!/usr/bin/env bash

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

set -euo pipefail

# get host values
NPROC=`nproc`
RAM_GB=`lsmem | awk '/Total online/ { print $4 }'`
RAM_GB=${RAM_GB::-1}

# generate the config.json file
mkdir -p generated
cat > generated/config.json << EOF
{
    "cluster_size": "small",
    "coordinator_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "coordinator_instance_ebs_size": 50,
    "worker_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "worker_instance_ebs_size": 50,
    "number_of_workers": 1,
    "memory_per_node_gb": ${RAM_GB},
    "vcpu_per_worker": ${NPROC},
    "fragment_result_cache_enabled": true,
    "data_cache_enabled": true
}
EOF

# run pbench to generate the config files
../../pbench/pbench genconfig -p params.json -t template generated
