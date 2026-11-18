#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: wait_for_cluster.sh <coordinator-address> <workers> <timeout-seconds>" >&2
  exit 2
fi

coordinator=$1
workers=$2
timeout=$3
deadline=$((SECONDS + timeout))

until [[ $(curl -fsS "http://${coordinator}:8080/v1/info/state" || true) == '"ACTIVE"' ]]; do
  if ((SECONDS >= deadline)); then
    echo "coordinator did not become ACTIVE within ${timeout}s" >&2
    docker logs presto-coordinator >&2 || true
    exit 1
  fi
  sleep 5
done

while true; do
  node_json=$(curl -fsS "http://${coordinator}:8080/v1/node" || echo '[]')
  # Discovery can retain stale descriptors briefly after workers restart.
  # Count unique worker endpoints so an in-place service restart does not
  # make a healthy cluster appear oversized.
  count=$(jq '[.[].uri] | unique | length' <<<"${node_json}")
  if [[ ${count} -eq ${workers} ]]; then
    printf '%s\n' "${node_json}" >/opt/presto-aws/registered_workers.json
    exit 0
  fi
  if ((SECONDS >= deadline)); then
    echo "expected ${workers} workers; observed ${count}" >&2
    printf '%s\n' "${node_json}" >&2
    exit 1
  fi
  sleep 5
done
