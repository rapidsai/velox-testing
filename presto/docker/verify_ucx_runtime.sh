#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${BUNDLED_UCX_VERSION:?BUNDLED_UCX_VERSION must be set by the worker image}"
: "${BUNDLED_UCX_ROOT:?BUNDLED_UCX_ROOT must be set by the worker image}"

ucx_info="${BUNDLED_UCX_ROOT}/bin/ucx_info"
if [[ ! -x "${ucx_info}" ]]; then
  echo "Bundled UCX executable is missing: ${ucx_info}" >&2
  exit 1
fi

actual_version=$("${ucx_info}" -v | awk '/Library version:/{print $4; exit}')
if [[ "${actual_version}" != "${BUNDLED_UCX_VERSION}" ]]; then
  echo "Bundled UCX version mismatch: expected ${BUNDLED_UCX_VERSION}, loaded ${actual_version:-unknown}" >&2
  "${ucx_info}" -v >&2 || true
  exit 1
fi

actual_library=$("${ucx_info}" -v | awk '/Library path:/{print $4; exit}')
case "${actual_library}" in
  "${BUNDLED_UCX_ROOT}"/*) ;;
  *)
    echo "Bundled UCX path mismatch: expected ${BUNDLED_UCX_ROOT}, loaded ${actual_library:-unknown}" >&2
    exit 1
    ;;
esac

for plugin in libuct_cuda.so libuct_ib_efa.so; do
  plugin_path="${BUNDLED_UCX_ROOT}/lib/ucx/${plugin}"
  if [[ ! -e "${plugin_path}" ]]; then
    echo "Bundled UCX plugin is missing: ${plugin_path}" >&2
    exit 1
  fi
  unresolved=$(ldd "${plugin_path}" \
    | grep 'not found' \
    | grep -vE 'libcuda\.so|libnvidia' || true)
  if [[ -n "${unresolved}" ]]; then
    echo "Bundled UCX plugin has unresolved dependencies: ${plugin_path}" >&2
    echo "${unresolved}" >&2
    exit 1
  fi
done

if [[ "${1:-}" == "--build-check" ]]; then
  exit 0
fi

ucx_devices=$(mktemp)
trap 'rm -f "${ucx_devices}"' EXIT
"${ucx_info}" -d > "${ucx_devices}"

for transport in cuda_copy srd; do
  if ! grep -q "Transport: ${transport}$" "${ucx_devices}"; then
    echo "Bundled UCX transport is unavailable at runtime: ${transport}" >&2
    exit 1
  fi
done

requested_devices=()
if [[ -n ${UCX_NET_DEVICES:-} ]]; then
  requested_devices+=("${UCX_NET_DEVICES}")
fi
while IFS='=' read -r _ value; do
  [[ -n "${value}" ]] && requested_devices+=("${value}")
done < <(env | grep -E '^UCX_NET_DEVICES_GPU_[0-9]+=' || true)

for device_list in "${requested_devices[@]}"; do
  IFS=',' read -ra devices <<< "${device_list}"
  for device in "${devices[@]}"; do
    [[ -z "${device}" ]] && continue
    if ! grep -q "Device: ${device}$" "${ucx_devices}"; then
      echo "Requested UCX device is unavailable at runtime: ${device}" >&2
      exit 1
    fi
  done
done

echo "Verified bundled UCX ${actual_version}: cuda_copy and SRD are available."
