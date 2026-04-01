#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../../scripts/profiler_functions_common.sh"

function get_container_id() {
  local container_id
  container_id=$(docker ps -q \
    --filter "label=com.nvidia.spark-connect.user=${USER}")
  if [[ -z "$container_id" ]]; then
    echo "Error: no running Spark Connect container found" >&2
    return 1
  fi
  echo "$container_id"
}
