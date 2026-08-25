#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: run_cache_series.sh <coordinator> <schema> <queries> <tag-prefix> <result-s3-uri>" >&2
  exit 2
fi

coordinator=$1
schema=$2
queries=$3
tag_prefix=$4
result_uri=$5
runner=/opt/velox-testing/presto/aws/ec2/remote/run_benchmark.sh
runtime_root=/opt/presto-aws

before_nodes=$(curl -fsS "http://${coordinator}:8080/v1/node" \
  | jq -c '[.[].uri] | sort | unique')
printf '%s\n' "${before_nodes}" >"${runtime_root}/results/${tag_prefix}_nodes_before.json"

snapshot_worker_metrics() {
  local label=$1 output_dir uri base_url worker_name
  output_dir="${runtime_root}/results/${tag_prefix}_metrics/${label}"
  mkdir -p "${output_dir}"
  while IFS= read -r uri; do
    base_url=${uri%/v1/status}
    worker_name=$(sed 's#^http://##; s#[/:]#_#g' <<<"${base_url}")
    curl -fsS "${base_url}/v1/info/metrics" \
      >"${output_dir}/${worker_name}.prom" || true
    curl -fsS "${base_url}/v1/status" \
      >"${output_dir}/${worker_name}_status.json" || true
  done < <(jq -r '.[]' <<<"${before_nodes}")
}

snapshot_worker_metrics before_cold
bash "${runner}" "${coordinator}" "${schema}" "${queries}" 1 \
  "${tag_prefix}_cold_fill" "${result_uri}"
snapshot_worker_metrics after_cold

for pass in 1 2 3 4; do
  bash "${runner}" "${coordinator}" "${schema}" "${queries}" 1 \
    "${tag_prefix}_warm_${pass}" "${result_uri}"
  snapshot_worker_metrics "after_warm_${pass}"
done

after_nodes=$(curl -fsS "http://${coordinator}:8080/v1/node" \
  | jq -c '[.[].uri] | sort | unique')
printf '%s\n' "${after_nodes}" >"${runtime_root}/results/${tag_prefix}_nodes_after.json"
if [[ ${before_nodes} != "${after_nodes}" ]]; then
  echo "worker membership changed during cache series" >&2
  diff -u \
    "${runtime_root}/results/${tag_prefix}_nodes_before.json" \
    "${runtime_root}/results/${tag_prefix}_nodes_after.json" >&2 || true
  exit 1
fi

aws s3 sync "${runtime_root}/results/" "${result_uri}/benchmark/" \
  --only-show-errors
