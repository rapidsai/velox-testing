#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${PRESTO_IMAGE_TAG}" ]; then
  export PRESTO_IMAGE_TAG="${USER:-latest}"
fi

GPU_FILE="${SCRIPT_DIR}/../docker/docker-compose/generated/docker-compose.native-gpu.rendered.yml"
CPU_RENDERED_FILE="${SCRIPT_DIR}/../docker/docker-compose/generated/docker-compose.native-cpu.rendered.yml"
JAVA_FILE="${SCRIPT_DIR}/../docker/docker-compose.java.yml"
CPU_FILE="${SCRIPT_DIR}/../docker/docker-compose.native-cpu.yml"

# Bring down each variant independently to avoid path resolution issues when
# combining files.
#
# Order matters: the coordinator is defined in docker-compose.common.yml and
# extended by every variant file, so the first `down` removes it. If workers
# from another variant are still attached to the shared network, that first
# `down` emits "Resource is still in use" for the network. Tear down in the
# order GPU -> CPU -> Java so whichever variant is actually running removes
# its workers together with the coordinator in a single step, leaving the
# remaining calls as silent no-ops.
if [ -f "$GPU_FILE" ]; then
  docker compose -f "$GPU_FILE" down
fi
if [ -f "$CPU_RENDERED_FILE" ]; then
  docker compose -f "$CPU_RENDERED_FILE" down
fi
docker compose -f "$CPU_FILE" down
docker compose -f "$JAVA_FILE" down
