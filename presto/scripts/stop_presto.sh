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

# Tear down whichever variant is currently running (if any). The coordinator
# is shared via `extends` across every variant's compose file, so calling
# `down` on a non-active variant would remove it and orphan the real workers
# on the shared network — hence the single-file policy below. Uses `docker
# ps -a` so we also catch containers in Created or Exited state left over
# from a half-finished prior run.
case "$(docker ps -a --format '{{.Names}}' 2>/dev/null | grep -m1 '^presto-native-worker\|^presto-java-worker')" in
  presto-native-worker-gpu*)        ACTIVE="$GPU_FILE" ;;
  presto-native-worker-cpu-[0-9]*)  ACTIVE="$CPU_RENDERED_FILE" ;;
  presto-native-worker-cpu)         [ -f "$CPU_RENDERED_FILE" ] && ACTIVE="$CPU_RENDERED_FILE" || ACTIVE="$CPU_FILE" ;;
  presto-java-worker*)              ACTIVE="$JAVA_FILE" ;;
  *)                                ACTIVE="" ;;
esac

if [ -n "$ACTIVE" ] && [ -f "$ACTIVE" ]; then
  docker compose -f "$ACTIVE" down
fi

# Safety net: force-remove any lingering presto-* containers. `docker
# compose down` only removes containers whose `com.docker.compose.project`
# label matches the compose file's project name, so an orphan coordinator
# started by a different project (e.g. the rendered-file project when
# we're now acting on the static file) is silently skipped. `docker rm -f`
# has no such constraint.
orphans=$(docker ps -a --format '{{.Names}}' 2>/dev/null | grep '^presto-' || true)
if [ -n "$orphans" ]; then
  echo "$orphans" | xargs -r docker rm -f
fi
