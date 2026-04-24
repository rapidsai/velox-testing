#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

# Ignore empty var warnings from docker compose down
: "${PROFILE:=}"
: "${PROFILE_ARGS:=}"
export PROFILE PROFILE_ARGS

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
# Tear down whichever variant is currently running (if any). The coordinator
# is shared via `extends` across every variant's compose file, so calling
# `down` on a non-active variant would remove it and orphan the real workers
# on the shared network — hence the single-file policy below.
case "$(docker ps --format '{{.Names}}' 2>/dev/null | grep -m1 '^presto-')" in
  presto-native-worker-gpu*)        ACTIVE="$GPU_FILE" ;;
  presto-native-worker-cpu-[0-9]*)  ACTIVE="$CPU_RENDERED_FILE" ;;
  presto-native-worker-cpu)         [ -f "$CPU_RENDERED_FILE" ] && ACTIVE="$CPU_RENDERED_FILE" || ACTIVE="$CPU_FILE" ;;
  presto-java-worker*)              ACTIVE="$JAVA_FILE" ;;
  *)                                ACTIVE="" ;;
esac

[ -n "$ACTIVE" ] && [ -f "$ACTIVE" ] && docker compose -f "$ACTIVE" down
