#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../../scripts/profiler_functions_common.sh"

function get_container_id() {
  local -r image_tag="${PRESTO_IMAGE_TAG}"
  local -r image_name="presto-native-worker-gpu:${image_tag}"
  local -r container_id=$(docker ps -q --filter "ancestor=${image_name}")
  if [[ -z $container_id ]]; then
    echo "Error: no docker container found for image: ${image_name}" >&2
    return 1
  fi
  echo $container_id
}
