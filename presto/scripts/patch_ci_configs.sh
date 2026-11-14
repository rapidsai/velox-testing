#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Patches generated Presto configs for CI container execution.
# generate_presto_config.sh sizes memory based on the host machine, which is
# too large for CI containers. This script applies constrained CI overrides.
#
# Usage: patch_ci_configs.sh <config_dir> [--jvm-heap-gb <N>] [--extra-overrides-file <path>]
#
# Arguments:
#   config_dir              Path to the generated config directory (e.g. .../generated/gpu)
#   --jvm-heap-gb           JVM heap size in GB for -Xmx/-Xms (default: 8)
#   --extra-overrides-file  Property overrides applied after the default overrides
#
# Property overrides are read from:
#   presto/docker/config/ci/coordinator_native_overrides.properties

set -euo pipefail

CONFIG_DIR="${1:?Usage: patch_ci_configs.sh <config_dir> [--jvm-heap-gb <N>] [--extra-overrides-file <path>]}"
shift

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JVM_HEAP_GB=8
OVERRIDES_FILES=("${SCRIPT_DIR}/../docker/config/ci/coordinator_native_overrides.properties")

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jvm-heap-gb) JVM_HEAP_GB="$2"; shift 2 ;;
    --extra-overrides-file) OVERRIDES_FILES+=("$2"); shift 2 ;;
    *) echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -d "${CONFIG_DIR}" ]]; then
  echo "ERROR: Config directory not found: ${CONFIG_DIR}" >&2
  exit 1
fi

for overrides_file in "${OVERRIDES_FILES[@]}"; do
  if [[ ! -f "${overrides_file}" ]]; then
    echo "ERROR: Overrides file not found: ${overrides_file}" >&2
    exit 1
  fi
done

JVM_CONFIG="${CONFIG_DIR}/etc_common/jvm.config"
COORD_CONFIG="${CONFIG_DIR}/etc_coordinator/config_native.properties"

echo "Patching JVM heap to ${JVM_HEAP_GB}G in ${JVM_CONFIG}"
sed -i "s/-Xmx[0-9]*G/-Xmx${JVM_HEAP_GB}G/;s/-Xms[0-9]*G/-Xms${JVM_HEAP_GB}G/" "${JVM_CONFIG}"

for overrides_file in "${OVERRIDES_FILES[@]}"; do
  echo "Applying coordinator overrides from ${overrides_file}"
  while IFS='=' read -r key value; do
    [[ -z "${key}" || "${key}" =~ ^[[:space:]]*# ]] && continue
    sed -i "s|${key}=.*|${key}=${value}|" "${COORD_CONFIG}"
  done < "${overrides_file}"
done

echo "CI config patching complete"
