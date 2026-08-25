#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: run_benchmark.sh <coordinator> <schema> <queries> <iterations> <tag> <result-s3-uri>" >&2
  exit 2
fi

coordinator=$1
schema=$2
queries=$3
iterations=$4
tag=$5
result_uri=$6
repo=/opt/velox-testing
runtime_root=/opt/presto-aws
output="${runtime_root}/results/${tag}"

mkdir -p "${output}"
cat >"${output}/driver_info.json" <<EOF
{
  "coordinator": "${coordinator}",
  "schema": "${schema}",
  "queries": "${queries}",
  "iterations": ${iterations},
  "tag": "${tag}",
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

sync_results() {
  aws s3 sync "${runtime_root}/results/" "${result_uri}/benchmark/" \
    --only-show-errors
}
trap 'sync_results || true' EXIT

export PATH="${runtime_root}/venv/bin:${PATH}"
export LOGS_DIR="${runtime_root}/logs"
cd "${repo}/presto/scripts"

status=0
timeout --signal=TERM --kill-after=60s 14400s ./run_benchmark.sh \
  --benchmark-type tpch \
  --hostname "${coordinator}" \
  --port 8080 \
  --queries "${queries}" \
  --schema-name "${schema}" \
  --iterations "${iterations}" \
  --output-dir "${output}" \
  --tag "${tag}" \
  --metrics \
  --skip-drop-cache \
  --verbose \
  >"${output}/benchmark.log" 2>&1 || status=$?

python3 - "${output}/driver_info.json" "${status}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text())
payload["exit_code"] = int(sys.argv[2])
payload["finished_at"] = datetime.now(timezone.utc).isoformat()
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

upload_status=0
sync_results || upload_status=$?
trap - EXIT
if ((upload_status != 0)); then
  echo "benchmark artifact upload failed with exit code ${upload_status}" >&2
  exit "${upload_status}"
fi
exit "${status}"
