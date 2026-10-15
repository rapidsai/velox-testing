#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: verify_gpu_exchange.sh <expected-peer-ip-csv> [sample-seconds]" >&2
  exit 2
fi

expected_peers=$1
sample_seconds=${2:-10}
local_address=$(hostname -I | awk '{print $1}')
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
  >"${runtime_root}/gpu_exchange_metrics_before.prom"
ss -ltnp >"${runtime_root}/gpu_exchange_listeners.txt"
ss -tinp >"${runtime_root}/gpu_exchange_connections_before.txt"
sleep "${sample_seconds}"
curl -fsS http://localhost:8080/v1/info/metrics \
  >"${runtime_root}/gpu_exchange_metrics_after.prom"
ss -tinp >"${runtime_root}/gpu_exchange_connections_after.txt"

python3 - \
  "${enabled}" \
  "${port}" \
  "${expected_peers}" \
  "${local_address}" \
  "${sample_seconds}" \
  "${runtime_root}/gpu_exchange_container.log" \
  "${runtime_root}/gpu_exchange_metrics_before.prom" \
  "${runtime_root}/gpu_exchange_metrics_after.prom" \
  "${runtime_root}/gpu_exchange_listeners.txt" \
  "${runtime_root}/gpu_exchange_connections_before.txt" \
  "${runtime_root}/gpu_exchange_connections_after.txt" \
  "${output}" <<'PY'
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    enabled,
    port,
    expected_peers_csv,
    local_address,
    sample_seconds,
    log_path,
    metrics_before_path,
    metrics_after_path,
    listeners_path,
    connections_before_path,
    connections_after_path,
    output_path,
) = sys.argv[1:]
log_text = Path(log_path).read_text(errors="replace")
metrics_before_text = Path(metrics_before_path).read_text(errors="replace")
metrics_after_text = Path(metrics_after_path).read_text(errors="replace")
listeners_text = Path(listeners_path).read_text(errors="replace")
connections_before_text = Path(connections_before_path).read_text(errors="replace")
connections_after_text = Path(connections_after_path).read_text(errors="replace")
connections_text = connections_before_text + "\n" + connections_after_text
expected_peers = sorted(
    peer
    for peer in set(filter(None, expected_peers_csv.split(",")))
    if peer != local_address
)

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


def parse_exchange_metrics(text):
    metrics = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        try:
            key, value_text = line.rsplit(None, 1)
            value = float(value_text)
        except (ValueError, IndexError):
            continue
        name = key.split("{", 1)[0]
        if re.search(r"ucx|exchange", name, re.IGNORECASE):
            metrics[key] = value
    return metrics


metrics_before = parse_exchange_metrics(metrics_before_text)
metrics_after = parse_exchange_metrics(metrics_after_text)
byte_counter_deltas = []
for key in sorted(metrics_before.keys() & metrics_after.keys()):
    name = key.split("{", 1)[0]
    if not re.search(r"bytes?", name, re.IGNORECASE):
        continue
    delta = metrics_after[key] - metrics_before[key]
    if delta > 0:
        byte_counter_deltas.append(
            {
                "metric": key,
                "before": metrics_before[key],
                "after": metrics_after[key],
                "delta": delta,
            }
        )

listener_active = bool(
    re.search(rf"[:.]({re.escape(port)})\s", listeners_text)
)
established_connections = sum(
    1
    for line in connections_text.splitlines()
    if re.search(rf"[:.]({re.escape(port)})\s", line)
)
connected_peers = [
    peer
    for peer in expected_peers
    if any(
        peer in line and "presto_server" in line
        for line in connections_text.splitlines()
    )
]
remote_peer_connections = bool(connected_peers)


def socket_exchange_bytes(text):
    total = 0
    matching_connection = False
    for line in text.splitlines():
        if line and not line[0].isspace():
            matching_connection = (
                "presto_server" in line
                and any(peer in line for peer in expected_peers)
            )
        elif matching_connection:
            total += sum(
                int(value)
                for value in re.findall(
                    r"bytes_(?:sent|received):(\d+)",
                    line,
                )
            )
    return total


socket_bytes_before = socket_exchange_bytes(connections_before_text)
socket_bytes_after = socket_exchange_bytes(connections_after_text)
socket_byte_delta = socket_bytes_after - socket_bytes_before
increasing_exchange_bytes = bool(byte_counter_deltas) or socket_byte_delta > 0
active_evidence = remote_peer_connections and increasing_exchange_bytes

payload = {
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "configured": enabled == "true",
    "server_port": int(port),
    "listener_active": listener_active,
    "established_connection_lines": established_connections,
    "expected_peer_addresses": expected_peers,
    "local_address": local_address,
    "connected_peer_addresses": connected_peers,
    "remote_peer_connections": remote_peer_connections,
    "sample_seconds": int(sample_seconds),
    "ucx_log_line_count": len(ucx_lines),
    "ucx_protocol_line_count": len(protocol_lines),
    "ucx_protocol_examples": protocol_lines[:20],
    "exchange_metric_names": sorted(
        {key.split("{", 1)[0] for key in metrics_before | metrics_after}
    ),
    "positive_exchange_byte_deltas": byte_counter_deltas[:100],
    "exchange_socket_bytes_before": socket_bytes_before,
    "exchange_socket_bytes_after": socket_bytes_after,
    "exchange_socket_byte_delta": socket_byte_delta,
    "increasing_exchange_bytes": increasing_exchange_bytes,
    "active_transfer_evidence": active_evidence,
}
Path(output_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload, indent=2, sort_keys=True))

if not payload["configured"]:
    raise SystemExit(3)
if not listener_active:
    raise SystemExit(4)
if not remote_peer_connections:
    raise SystemExit(5)
if not increasing_exchange_bytes:
    raise SystemExit(6)
PY
