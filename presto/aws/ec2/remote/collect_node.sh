#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: collect_node.sh <run-id> <role> <instance-id> <result-s3-uri>" >&2
  exit 2
fi

run_id=$1
role=$2
instance_id=$3
result_uri=$4
runtime_root=/opt/presto-aws
artifact_root="${runtime_root}/artifacts/${role}-${instance_id}"

rm -rf "${artifact_root}"
mkdir -p "${artifact_root}"

cp -a "${runtime_root}/logs" "${artifact_root}/" 2>/dev/null || true
cp -a "${runtime_root}/telemetry" "${artifact_root}/" 2>/dev/null || true
cp -a "${runtime_root}/base_config" "${artifact_root}/" 2>/dev/null || true
cp -a "${runtime_root}/final_config" "${artifact_root}/" 2>/dev/null || true
cp -a "${runtime_root}/config_history" "${artifact_root}/" 2>/dev/null || true
cp -a "${runtime_root}/results" "${artifact_root}/" 2>/dev/null || true
cp "${runtime_root}"/*.json "${artifact_root}/" 2>/dev/null || true
cp "${runtime_root}"/*.sha256 "${artifact_root}/" 2>/dev/null || true
cp "${runtime_root}"/*.diff "${artifact_root}/" 2>/dev/null || true
cp "${runtime_root}"/gpu_exchange_* "${artifact_root}/" 2>/dev/null || true

docker ps -a --no-trunc >"${artifact_root}/docker_ps.txt" 2>&1 || true
mapfile -t presto_containers < <(
  docker ps -a --format '{{.Names}}' | awk '/^presto-/'
)
if ((${#presto_containers[@]})); then
  docker inspect "${presto_containers[@]}" \
    >"${artifact_root}/docker_inspect.json" 2>/dev/null || true
  for container in "${presto_containers[@]}"; do
    docker logs "${container}" \
      >"${artifact_root}/${container}.log" 2>&1 || true
  done
fi

uname -a >"${artifact_root}/uname.txt"
lscpu --json >"${artifact_root}/lscpu.json"
lsmem --json >"${artifact_root}/lsmem.json"
ip -json address >"${artifact_root}/ip_address.json"
ip -s -json link >"${artifact_root}/ip_link_stats.json"
lsblk --json --bytes --output NAME,PATH,MODEL,SERIAL,SIZE,FSTYPE,MOUNTPOINTS \
  >"${artifact_root}/lsblk.json"
cp /proc/diskstats "${artifact_root}/diskstats.txt"
df -B1 --output=source,fstype,size,used,avail,target \
  >"${artifact_root}/filesystems.txt"
if [[ ${role} == worker ]]; then
  curl -fsS http://localhost:8080/v1/info/metrics \
    >"${artifact_root}/worker_metrics.prom" 2>&1 || true
  curl -fsS http://localhost:8080/v1/status \
    >"${artifact_root}/worker_status.json" 2>&1 || true
  nvidia-smi -q >"${artifact_root}/nvidia_smi_q.txt" 2>&1 || true
  nvidia-smi dmon -s pucvmt -c 1 \
    >"${artifact_root}/nvidia_smi_dmon.txt" 2>&1 || true
fi
journalctl -u docker --no-pager >"${artifact_root}/docker_journal.log" 2>&1 || true
dmesg --ctime >"${artifact_root}/dmesg.log" 2>&1 || true

cat >"${artifact_root}/collection.json" <<EOF
{
  "run_id": "${run_id}",
  "role": "${role}",
  "instance_id": "${instance_id}",
  "collected_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

aws s3 sync "${artifact_root}/" \
  "${result_uri}/nodes/${role}-${instance_id}/" --only-show-errors
