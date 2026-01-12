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

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function echo_error {
  echo -e "${RED}$1${NC}"
  exit 1
}

function echo_warning {
  echo -e "${YELLOW}$1${NC}"
}

function echo_success {
  echo -e "${GREEN}$1${NC}"
}

if [ ! -x ../pbench/pbench ]; then
  echo_error "ERROR: generate_presto_config.sh script must only be run from presto:presto/scripts"
fi

# get host values
NPROC=`nproc`
# lsmem will report in SI.  Make sure we get values in GB.
RAM_GB=$(lsmem -b | grep "Total online memory" | awk '{print int($4 / (1024*1024*1024)); }')

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|gpu-dev|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, gpu-dev, java)."
fi
function is_gpu_variant() {
  [[ "${VARIANT_TYPE}" == "gpu" || "${VARIANT_TYPE}" == "gpu-dev" ]]
}
if is_gpu_variant; then
  VCPU_PER_WORKER=2
else
  VCPU_PER_WORKER=${NPROC}
fi

# move to config directory
pushd ../docker/config > /dev/null

# always move back even on failure
trap "popd > /dev/null" EXIT

CONFIG_DIR=generated/${VARIANT_TYPE}

# generate only if no existing config or overwrite flag is set
if [[ ! -d ${CONFIG_DIR} || "${OVERWRITE_CONFIG}" == "true" ]]; then
  echo "Generating Presto Config files for '${VARIANT_TYPE}' for host with ${NPROC} CPU cores and ${RAM_GB}GB RAM"

  # (re-)generate the config.json file
  rm -rf ${CONFIG_DIR}
  mkdir -p ${CONFIG_DIR}
  cat > ${CONFIG_DIR}/config.json << EOF
{
    "cluster_size": "small",
    "coordinator_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "coordinator_instance_ebs_size": 50,
    "worker_instance_type": "${NPROC}-core CPU and ${RAM_GB}GB RAM",
    "worker_instance_ebs_size": 50,
    "number_of_workers": 1,
    "memory_per_node_gb": ${RAM_GB},
    "vcpu_per_worker": ${VCPU_PER_WORKER},
    "fragment_result_cache_enabled": true,
    "data_cache_enabled": true
}
EOF

  # run pbench to generate the config files
  # hide default pbench logging which goes to stderr so we only see any errors
  if ../../pbench/pbench genconfig -p params.json -t template ${CONFIG_DIR} 2>&1 | grep '\{\"level":"error"'; then
    echo_error "ERROR: Errors reported by pbench genconfig. Configs were not generated successfully."
  fi

  # now perform other variant-specific modifications to the generated configs
  if is_gpu_variant; then
    # for GPU variant, uncomment these optimizer settings
    # optimizer.joins-not-null-inference-strategy=USE_FUNCTION_METADATA
    # optimizer.default-filter-factor-enabled=true
    COORD_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
    sed -i 's/\#optimizer/optimizer/g' ${COORD_CONFIG}
    
    # Adds a cluster tag for gpu variant
    WORKER_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
    echo "cluster-tag=native-gpu" >> ${WORKER_CONFIG}
  fi

  # now perform other variant-specific modifications to the generated configs
  if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
    # Adds a cluster tag for cpu variant
    WORKER_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
    echo "cluster-tag=native-cpu" >> ${WORKER_CONFIG}
  fi

  # for Java variant, disable some Parquet properties which are now rejected
  if [[ "${VARIANT_TYPE}" == "java" ]]; then
    HIVE_CONFIG="${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i 's/parquet\.reader\.chunk-read-limit/#parquet\.reader\.chunk-read-limit/' ${HIVE_CONFIG}
    sed -i 's/parquet\.reader\.pass-read-limit/#parquet\.reader\.pass-read-limit/' ${HIVE_CONFIG}
  fi

  # success message
  echo_success "Configs were generated successfully"
else
  # otherwise, reuse existing config
  echo_success "Reusing existing Presto Config files for '${VARIANT_TYPE}'"
fi
