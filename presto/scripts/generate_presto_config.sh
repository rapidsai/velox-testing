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

function duplicate_worker_configs() {
  echo "Duplicating worker configs for GPU ID $1"
  local worker_config="${CONFIG_DIR}/etc_worker_${1}"
  local coord_config="${CONFIG_DIR}/etc_coordinator"
  rm -rf ${worker_config}
  cp -r ${CONFIG_DIR}/etc_worker ${worker_config}

  # Single node execution needs to be disabled if we are running multiple workers.
  if [[ ${NUM_WORKERS} -gt 1 ]]; then
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" \
        ${coord_config}/config_native.properties
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" \
	${worker_config}/config_native.properties
  # make cudf.exchange=true if we are running multiple workers
    sed -i "s+cudf.exchange=false+cudf.exchange=true+g" ${worker_config}/config_native.properties
  fi
  echo "join-distribution-type=PARTITIONED" >> ${coord_config}/config_native.properties

  # Each worker node needs to have it's own http-server port.  This isn't used, but
  # the cudf.exchange server port is currently hard-coded to be the server port +3
  # and that needs to be unique for each worker.
  sed -i "s+http-server\.http\.port.*+http-server\.http\.port=80${1}0+g" \
      ${worker_config}/config_native.properties
  sed -i "s+cudf.exchange.server.port=.*+cudf.exchange.server.port=80${1}3+g" \
      ${worker_config}/config_native.properties
  if ! grep -q "^cudf.exchange.server.port=80${1}3" ${worker_config}/config_native.properties; then
    echo "cudf.exchange.server.port=80${1}3" >> ${worker_config}/config_native.properties
  fi
  echo "async-data-cache-enabled=false" >> ${worker_config}/config_native.properties
  # Give each worker a unique id.
  sed -i "s+node\.id.*+node\.id=worker_${1}+g" ${worker_config}/node.properties
}

# get host values
NPROC=`nproc`
# lsmem will report in SI.  Make sure we get values in GB.
RAM_GB=$(lsmem -b | grep "Total online memory" | awk '{print int($4 / (1024*1024*1024)); }')

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|gpu-dev|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, gpu-dev, java)."
fi
if [[ -z ${VCPU_PER_WORKER} ]]; then
  if [[ "${VARIANT_TYPE}" == "gpu" || "${VARIANT_TYPE}" == "gpu-dev" ]]; then
    VCPU_PER_WORKER=2
  else
    VCPU_PER_WORKER=${NPROC}
  fi
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
    "number_of_workers": ${NUM_WORKERS},
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

  if [ -n "${HIVE_METASTORE_URI:-}" ]; then
    sed -i 's/hive.metastore=file/#hive.metastore=file/' "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i "s|hive.metastore.catalog.dir=.*|hive.metastore.uri=${HIVE_METASTORE_URI}|" "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
  fi

  COORD_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
  # now perform other variant-specific modifications to the generated configs
  if is_gpu_variant; then
    # for GPU variant, uncomment these optimizer settings
    # optimizer.joins-not-null-inference-strategy=USE_FUNCTION_METADATA
    # optimizer.default-filter-factor-enabled=true
    sed -i 's/\#optimizer/optimizer/g' ${COORD_CONFIG}

    if [[ ${NUM_WORKERS} -eq 1 ]]; then
      # Adds a cluster tag for gpu variant
      echo "cluster-tag=native-gpu" >> ${COORD_CONFIG}
    fi
  fi

  # now perform other variant-specific modifications to the generated configs
  if [[ "${VARIANT_TYPE}" == "cpu" ]]; then
    # Adds a cluster tag for cpu variant
    echo "cluster-tag=native-cpu" >> ${COORD_CONFIG}
  fi

  # for Java variant, disable some Parquet properties which are now rejected
  if [[ "${VARIANT_TYPE}" == "java" ]]; then
    HIVE_CONFIG="${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i 's/parquet\.reader\.chunk-read-limit/#parquet\.reader\.chunk-read-limit/' ${HIVE_CONFIG}
    sed -i 's/parquet\.reader\.pass-read-limit/#parquet\.reader\.pass-read-limit/' ${HIVE_CONFIG}
    sed -i 's/^cudf/#cudf/' ${HIVE_CONFIG}
  fi

  # success message
  echo_success "Configs were generated successfully"
else
  # otherwise, reuse existing config
  echo_success "Reusing existing Presto Config files for '${VARIANT_TYPE}'"
fi

# We want to propagate any changes from the original worker config to the new worker configs even if
# we did not re-generate the configs.
if [[ -n "$NUM_WORKERS" && -n "$GPU_IDS" && "$VARIANT_TYPE" == "gpu" ]]; then
  # Count the number of GPU IDs provided
  IFS=',' read -ra GPU_ID_ARRAY <<< "$GPU_IDS"
  for i in "${GPU_ID_ARRAY[@]}"; do
    duplicate_worker_configs $i
  done
fi
