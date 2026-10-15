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

docker_compose_tty() {
  if [ -t 2 ] && [ ! -t 1 ]; then
    docker compose "$@" 1>&2
  else
    docker compose "$@"
  fi
}

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
containers=$(docker ps -a --format '{{.Names}}')
ACTIVE_WORKER=$(printf '%s\n' "$containers" | grep -m1 -E '^presto-(native-worker-(cpu|gpu)(-[0-9]+)?|java-worker)$' || true)
case "$ACTIVE_WORKER" in
  presto-native-worker-gpu*)        ACTIVE="$GPU_FILE" ;;
  presto-native-worker-cpu-[0-9]*)  ACTIVE="$CPU_RENDERED_FILE" ;;
  presto-native-worker-cpu)         [ -f "$CPU_RENDERED_FILE" ] && ACTIVE="$CPU_RENDERED_FILE" || ACTIVE="$CPU_FILE" ;;
  presto-java-worker*)              ACTIVE="$JAVA_FILE" ;;
  *)                                ACTIVE="" ;;
esac

if [ -n "$ACTIVE" ] && [ -f "$ACTIVE" ]; then
  PROJECT=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "$ACTIVE_WORKER")
  PROJECT_ARGS=()
  if [[ -n "$PROJECT" && "$PROJECT" != '<no value>' ]]; then
    PROJECT_ARGS=(-p "$PROJECT")
  fi
  docker_compose_tty "${PROJECT_ARGS[@]}" -f "$ACTIVE" down
fi

# Safety net: remove only the known coordinator/worker container names. `docker
# compose down` only removes containers whose `com.docker.compose.project`
# label matches the compose file's project name, so an orphan coordinator
# started by a different project (e.g. the rendered-file project when
# we're now acting on the static file) is silently skipped. `docker rm -f`
# has no such constraint.
containers=$(docker ps -a --format '{{.Names}}')
orphans=$(printf '%s\n' "$containers" | grep -E '^presto-(coordinator|native-worker-(cpu|gpu)(-[0-9]+)?|java-worker)$' || true)
if [ -n "$orphans" ]; then
  echo "$orphans" | xargs -r docker rm -f
fi
