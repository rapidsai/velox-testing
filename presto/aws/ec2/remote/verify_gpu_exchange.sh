#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

runtime_root=/opt/presto-aws
config="${runtime_root}/final_config/worker/config.properties"
output="${runtime_root}/gpu_exchange_verification.json"
container=presto-native-worker-gpu

test -f "${config}"
enabled=$(awk -F= '$1 == "cudf.exchange" {value=$2} END {print value}' "${config}")
port=$(awk -F= '$1 == "cudf.exchange.server.port" {value=$2} END {print value}' "${config}")
test -n "${port}"

docker logs "${container}" >"${runtime_root}/gpu_exchange_container.log" 2>&1
curl -fsS http://localhost:8080/v1/info/metrics \
  >"${runtime_root}/gpu_exchange_metrics.prom"
ss -ltnp >"${runtime_root}/gpu_exchange_listeners.txt"
ss -tinp >"${runtime_root}/gpu_exchange_connections.txt"

python3 - \
  "${enabled}" \
  "${port}" \
  "${runtime_root}/gpu_exchange_container.log" \
  "${runtime_root}/gpu_exchange_metrics.prom" \
  "${runtime_root}/gpu_exchange_listeners.txt" \
  "${runtime_root}/gpu_exchange_connections.txt" \
  "${output}" <<'PY'
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

enabled, port, log_path, metrics_path, listeners_path, connections_path, output_path = (
    sys.argv[1:]
)
log_text = Path(log_path).read_text(errors="replace")
metrics_text = Path(metrics_path).read_text(errors="replace")
listeners_text = Path(listeners_path).read_text(errors="replace")
connections_text = Path(connections_path).read_text(errors="replace")

ucx_lines = [
    line
    for line in log_text.splitlines()
    if re.search(r"\bucx\b|cuda_(?:copy|ipc)|UCX_", line, re.IGNORECASE)
]
protocol_lines = [
    line
    for line in ucx_lines
    if re.search(
        r"proto|rndv|eager|endpoint|listener|transport|cuda_copy|cuda_ipc",
        line,
        re.IGNORECASE,
    )
]
exchange_metrics = []
positive_exchange_metrics = []
for line in metrics_text.splitlines():
    if not line or line.startswith("#"):
        continue
    name = line.split("{", 1)[0].split(None, 1)[0]
    if not re.search(r"ucx|cudf.*exchange|exchange.*cudf", name, re.IGNORECASE):
        continue
    exchange_metrics.append(name)
    try:
        value = float(line.rsplit(None, 1)[1])
    except (ValueError, IndexError):
        continue
    if value > 0:
        positive_exchange_metrics.append({"name": name, "value": value})

listener_active = bool(
    re.search(rf"[:.]({re.escape(port)})\s", listeners_text)
)
established_connections = sum(
    1
    for line in connections_text.splitlines()
    if re.search(rf"[:.]({re.escape(port)})\s", line)
)
active_evidence = bool(protocol_lines or positive_exchange_metrics)

payload = {
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "configured": enabled == "true",
    "server_port": int(port),
    "listener_active": listener_active,
    "established_connection_lines": established_connections,
    "ucx_log_line_count": len(ucx_lines),
    "ucx_protocol_line_count": len(protocol_lines),
    "ucx_protocol_examples": protocol_lines[:20],
    "exchange_metric_names": sorted(set(exchange_metrics)),
    "positive_exchange_metrics": positive_exchange_metrics[:50],
    "active_transfer_evidence": active_evidence,
}
Path(output_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload, indent=2, sort_keys=True))

if not payload["configured"]:
    raise SystemExit(3)
if not listener_active:
    raise SystemExit(4)
if not active_evidence:
    raise SystemExit(5)
PY
