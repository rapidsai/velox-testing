#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

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

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -x "${SCRIPT_DIR}/../pbench/pbench" ]; then
  echo_error "ERROR: generate_presto_config.sh script cannot find pbench at ${SCRIPT_DIR}/../pbench/pbench"
fi

# This function duplicates the worker configs when we are running multiple workers.
# It also adds certain config options to the workers if those options apply only to multi-worker environments.
function duplicate_worker_configs() {
  local worker_id=$1
  echo "Duplicating worker configs for GPU ID $worker_id"
  local worker_config="${CONFIG_DIR}/etc_worker_${worker_id}"
  local coord_config="${CONFIG_DIR}/etc_coordinator"
  local worker_native_config="${worker_config}/config_native.properties"
  local coord_native_config="${coord_config}/config_native.properties"
  # Need to stagger the port numbers because ucx exchange currently expects to be exactly
  # 3 higher than the http port.
  local http_port="10$(printf "%02d\n" "$worker_id")0"
  local exch_port="10$(printf "%02d\n" "$worker_id")3"
  rm -rf ${worker_config}
  cp -r ${CONFIG_DIR}/etc_worker ${worker_config}

  # Some configs should only be applied if we are in a multi-worker environment.
  if [[ ${NUM_WORKERS} -gt 1 ]]; then
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${coord_native_config}
    sed -i "s+single-node-execution-enabled.*+single-node-execution-enabled=false+g" ${worker_native_config}
    # make cudf.exchange=true if we are running multiple workers
    sed -i "s+cudf.exchange=false+cudf.exchange=true+g" ${worker_native_config}
    # make join-distribution-type=PARTITIONED if we are running multiple workers
    # (ucx exchange does not currently support BROADCAST partition type)
    sed -i "s+join-distribution-type=.*+join-distribution-type=PARTITIONED+g" ${coord_native_config}
  fi

  # Each worker node needs to have it's own http-server port.  This isn't used, but
  # the cudf.exchange server port is currently hard-coded to be the server port +3
  # and that needs to be unique for each worker.
  sed -i "s+http-server\.http\.port.*+http-server\.http\.port=${http_port}+g" ${worker_native_config}
  sed -i "s+cudf.exchange.server.port=.*+cudf.exchange.server.port=${exch_port}+g" ${worker_native_config}
  # Give each worker a unique id.
  sed -i "s+node\.id.*+node\.id=worker_${worker_id}+g" ${worker_config}/node.properties
}

# get host values
NPROC=`nproc`
# lsmem will report in SI.  Make sure we get values in GB.
RAM_GB=$(lsmem -b | grep "Total online memory" | awk '{print int($4 / (1024*1024*1024)); }')

# variant-specific behavior
# for GPU you must set vcpu_per_worker to a small number, not the CPU count
if [[ -z ${VARIANT_TYPE} || ! ${VARIANT_TYPE} =~ ^(cpu|gpu|java)$ ]]; then
  echo_error "ERROR: VARIANT_TYPE must be set to a valid variant type (cpu, gpu, java)."
fi
if [[ -z ${VCPU_PER_WORKER:-} ]]; then
  if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
    VCPU_PER_WORKER=2
  else
    VCPU_PER_WORKER=${NPROC}
  fi
fi

# move to config directory
pushd "${SCRIPT_DIR}/../docker/config" > /dev/null

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
  if "${SCRIPT_DIR}/../pbench/pbench" genconfig -p params.json -t template ${CONFIG_DIR} 2>&1 | grep '\{\"level":"error"'; then
    echo_error "ERROR: Errors reported by pbench genconfig. Configs were not generated successfully."
  fi

  if [ -n "${HIVE_METASTORE_URI:-}" ]; then
    sed -i 's/hive.metastore=file/#hive.metastore=file/' "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
    sed -i "s|hive.metastore.catalog.dir=.*|hive.metastore.uri=${HIVE_METASTORE_URI}|" "${CONFIG_DIR}/etc_coordinator/catalog/hive.properties" "${CONFIG_DIR}/etc_worker/catalog/hive.properties"
  fi

  COORD_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"
  WORKER_CONFIG="${CONFIG_DIR}/etc_worker/config_native.properties"
  # now perform other variant-specific modifications to the generated configs
  if [[ "${VARIANT_TYPE}" == "gpu" ]]; then
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
if [[ -n "$NUM_WORKERS" && "$VARIANT_TYPE" == "gpu" ]]; then
  if [[ -n ${GPU_IDS:-} ]]; then
      WORKER_IDS=($(echo "$GPU_IDS" | tr ',' ' '))
  else
      WORKER_IDS=($(seq 0 $((NUM_WORKERS - 1))))
  fi
  for i in "${WORKER_IDS[@]}"; do
    duplicate_worker_configs $i
  done
fi
