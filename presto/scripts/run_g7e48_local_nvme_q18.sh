#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Reproduce the compression-free, local-NVMe SF3K Q18 EFA/SRD benchmark on
# the audited g7e.48xlarge topology. Iteration 1 warms caches; iteration 2 is
# the reported hot result.
#
# AWS prerequisite: provision one EFA-capable network interface on each of
# NetworkCardIndex 0, 1, 2, and 3. A default g7e.48xlarge launch may expose
# only the first card. See:
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

: "${PRESTO_IMAGE_TAG:?set PRESTO_IMAGE_TAG to the coordinator/worker image tag}"
: "${PRESTO_DATA_DIR:?set PRESTO_DATA_DIR to the parent of sf3k_v2_float on NVMe}"
SCHEMA=${SCHEMA:-tianyu_nvme_sf3k_v2_float}
RESULTS_DIR=${RESULTS_DIR:-$HOME/Development/results}
RESULT_TAG=${RESULT_TAG:-g7e48_local_nvme_sf3k_q18_ucx_srd_no_compression_i2}

if [[ -z ${PRESTO_WORKER_INTERNAL_ADDRESS:-} ]]; then
  PRESTO_WORKER_INTERNAL_ADDRESS=$(ip -4 -o addr show dev enp135s0 | awk '{split($4, a, "/"); print a[1]; exit}')
  export PRESTO_WORKER_INTERNAL_ADDRESS
fi
: "${PRESTO_WORKER_INTERNAL_ADDRESS:?could not determine the host private IPv4 address}"

export UCX_NET_DEVICES_GPU_0='rdmap145s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_1='rdmap145s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_2='rdmap162s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_3='rdmap162s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_4='rdmap179s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_5='rdmap179s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_6='rdmap196s0:1,enp135s0'
export UCX_NET_DEVICES_GPU_7='rdmap196s0:1,enp135s0'

export UCX_TLS='tcp,srd,cuda_copy'
export UCX_SOCKADDR_TLS_PRIORITY=tcp
export UCX_RNDV_FRAG_SIZE='cuda:32M'
export UCX_RNDV_FRAG_MEM_TYPES=cuda
export UCX_MAX_RNDV_RAILS=1

missing_rdma_devices=()
for device in rdma_cm uverbs0 uverbs1 uverbs2 uverbs3; do
  [[ -c "/dev/infiniband/$device" ]] || missing_rdma_devices+=("$device")
done
if (( ${#missing_rdma_devices[@]} )); then
  echo "Expected four EFA network cards; launch the instance with EFA interfaces assigned to NetworkCardIndex 0-3." >&2
  echo "Missing /dev/infiniband devices: ${missing_rdma_devices[*]}" >&2
  echo "See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html" >&2
  exit 1
fi

for nic in enp135s0 enp153s0 enp170s0 enp187s0; do
  sudo -n ethtool -L "$nic" combined 16
  [[ $(ethtool -l "$nic" | awk '/Current hardware settings:/{current=1; next} current && /Combined:/{print $2; exit}') == 16 ]] || {
    echo "$nic does not have 16 combined queues" >&2
    exit 1
  }
done

start_args=(
  --ucx-efa
  --overwrite-config
  --single-container
  --kvikio-threads 16
  --num-drivers 2
  --num-workers 8
  --gpu-ids 0,1,2,3,4,5,6,7
)
if [[ ${BUILD_WORKER:-false} == true ]]; then
  start_args+=(--build worker --num-threads "$(nproc)")
fi
if [[ ${NO_CACHE_BUILD:-false} == true ]]; then
  start_args+=(--no-cache)
fi

"$SCRIPT_DIR/start_native_gpu_presto.sh" "${start_args[@]}"

if grep -Rqs '^cudf\.exchange_compression=' \
    "$SCRIPT_DIR/../docker/config/generated/gpu"/etc_worker_*/config_native.properties; then
  echo "Compression configuration unexpectedly present in generated workers" >&2
  exit 1
fi

"$SCRIPT_DIR/run_benchmark.sh" \
  --benchmark-type tpch \
  --queries 18 \
  --schema-name "$SCHEMA" \
  --iterations 2 \
  --output-dir "$RESULTS_DIR" \
  --tag "$RESULT_TAG" \
  --skip-analyze-check
