#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Starts an eight-worker Presto GPU cluster with one EFA assigned to each
# consecutive pair of GPUs on a g7e.48xlarge. This script configures transport
# only; it does not prepare data, register schemas, or run a benchmark.
#
# Provisioning prerequisite: launch the instance with a primary ENA on network
# card 0 and EFA-only interfaces on network card indexes 0, 1, 2, and 3. See:
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html

set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

: "${PRESTO_IMAGE_TAG:?set PRESTO_IMAGE_TAG to the coordinator/worker image tag}"

for command in ip nvidia-smi rdma; do
  command -v "${command}" >/dev/null || {
    echo "Required command is unavailable: ${command}" >&2
    exit 1
  }
done

mapfile -t gpu_ids < <(
  nvidia-smi --query-gpu=index,pci.bus_id --format=csv,noheader,nounits \
    | awk -F, '{gsub(/ /, "", $1); gsub(/ /, "", $2); print $2, $1}' \
    | sort \
    | awk '{print $2}'
)
if (( ${#gpu_ids[@]} != 8 )); then
  echo "Expected 8 GPUs on g7e.48xlarge; found ${#gpu_ids[@]}." >&2
  exit 1
fi

mapfile -t rdma_devices < <(
  for path in /sys/class/infiniband/*; do
    [[ -e "${path}" ]] || continue
    printf '%s %s\n' "$(basename "$(readlink -f "${path}/device")")" "${path##*/}"
  done | sort | awk '{print $2}'
)
if (( ${#rdma_devices[@]} != 4 )); then
  echo "Expected 4 EFA devices on g7e.48xlarge; found ${#rdma_devices[@]}." >&2
  echo "Provision EFA-only interfaces on network card indexes 0-3." >&2
  echo "See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html" >&2
  exit 1
fi

primary_interface=$(ip -4 route show default | awk '{print $5; exit}')
: "${primary_interface:?could not discover the primary ENA interface}"
primary_driver=$(basename "$(readlink -f "/sys/class/net/${primary_interface}/device/driver")")
if [[ "${primary_driver}" != ena ]]; then
  echo "Primary interface ${primary_interface} does not use the ENA driver." >&2
  exit 1
fi
PRESTO_WORKER_INTERNAL_ADDRESS=$(
  ip -4 -o addr show dev "${primary_interface}" scope global \
    | awk '{split($4, address, "/"); print address[1]; exit}'
)
export PRESTO_WORKER_INTERNAL_ADDRESS
: "${PRESTO_WORKER_INTERNAL_ADDRESS:?could not discover the host private IPv4 address}"

for position in "${!gpu_ids[@]}"; do
  gpu_id=${gpu_ids[$position]}
  rdma_device=${rdma_devices[$((position / 2))]}
  rdma_link=$(rdma link show | awk -v device="${rdma_device}/" '$2 ~ ("^" device) {print $0; exit}')
  [[ "${rdma_link}" == *"state ACTIVE"* ]] || {
    echo "EFA device is not active: ${rdma_device}" >&2
    exit 1
  }

  mapfile -t uverbs_devices < <(
    find "/sys/class/infiniband/${rdma_device}/device/infiniband_verbs" \
      -maxdepth 1 -name 'uverbs*' -printf '%f\n'
  )
  if (( ${#uverbs_devices[@]} != 1 )) || [[ ! -c "/dev/infiniband/${uverbs_devices[0]}" ]]; then
    echo "Could not resolve a usable uverbs device for ${rdma_device}." >&2
    exit 1
  fi
  case "${uverbs_devices[0]}" in
    uverbs0|uverbs1|uverbs2|uverbs3) ;;
    *)
      echo "The current Compose template does not expose ${uverbs_devices[0]}; expected uverbs0-3." >&2
      exit 1
      ;;
  esac

  gpu_numa=$(nvidia-smi topo -C -M -i "${gpu_id}" | awk -F: '/NUMA IDs of closest memory/{gsub(/ /, "", $2); print $2}')
  rdma_numa=$(<"/sys/class/infiniband/${rdma_device}/device/numa_node")
  if [[ "${gpu_numa}" != "${rdma_numa}" ]]; then
    echo "PCI-order mapping is not NUMA-local: GPU ${gpu_id} is on ${gpu_numa}, ${rdma_device} is on ${rdma_numa}." >&2
    exit 1
  fi

  variable="UCX_NET_DEVICES_GPU_${gpu_id}"
  printf -v "${variable}" '%s:1,%s' "${rdma_device}" "${primary_interface}"
  export "${variable}"
  printf 'GPU %s -> %s\n' "${gpu_id}" "${!variable}"
done

export UCX_TLS=tcp,srd,cuda_copy
export UCX_SOCKADDR_TLS_PRIORITY=tcp
export UCX_RNDV_FRAG_SIZE=cuda:32M
export UCX_RNDV_FRAG_MEM_TYPES=cuda
export UCX_MAX_RNDV_RAILS=1

gpu_ids_csv=$(IFS=,; printf '%s' "${gpu_ids[*]}")
start_args=(
  --ucx-efa
  --overwrite-config
  --num-workers 8
  --gpu-ids "${gpu_ids_csv}"
)
if [[ ${SINGLE_CONTAINER:-false} == true ]]; then
  start_args+=(--single-container)
fi
if [[ ${BUILD_WORKER:-false} == true ]]; then
  start_args+=(--build worker --num-threads "$(nproc)")
fi

"${SCRIPT_DIR}/start_native_gpu_presto.sh" "${start_args[@]}"
