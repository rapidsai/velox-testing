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

if [ ! -x ../pbench/pbench ]; then
    echo "ERROR: generate_presto_config.sh script must only be run from presto:presto/scripts"
    exit 1
fi

# get host values
NPROC=`nproc`
# lsmem will report in SI.  Make sure we get values in GB.
RAM_GB=$(lsmem -b | grep "Total online memory" | awk '{print $4 / (1024*1024*1024)}')

echo "Generating Presto Config files for ${NPROC} CPU cores and ${RAM_GB}GB RAM"

# move to config directory
pushd ../docker/config > /dev/null

# always move back even on failure
trap "popd > /dev/null" EXIT

# (re-)generate the config.json file
rm -rf generated
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
# hide default pbench logging which goes to stderr so we only see any errors
../../pbench/pbench genconfig -p params.json -t template generated 2>&1 | grep -v '\{\"level'
