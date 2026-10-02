#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: sample_host.sh <role> <output-jsonl>" >&2
  exit 2
fi

role=$1
output=$2
mkdir -p "$(dirname "${output}")"

while true; do
  python3 - "${role}" >>"${output}" <<'PY'
import json
import os
import resource
import time
from pathlib import Path

role = os.sys.argv[1]
meminfo = {}
for line in Path("/proc/meminfo").read_text().splitlines():
    key, value = line.split(":", 1)
    meminfo[key] = int(value.strip().split()[0]) * 1024

load1, load5, load15 = os.getloadavg()
network = {}
for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
    interface, counters = line.split(":", 1)
    fields = counters.split()
    interface = interface.strip()
    if interface != "lo":
        network[interface] = {
            "rx_bytes": int(fields[0]),
            "rx_packets": int(fields[1]),
            "tx_bytes": int(fields[8]),
            "tx_packets": int(fields[9]),
        }

disks = {}
for line in Path("/proc/diskstats").read_text().splitlines():
    fields = line.split()
    name = fields[2]
    if name.startswith("nvme"):
        disks[name] = {
            "reads_completed": int(fields[3]),
            "sectors_read": int(fields[5]),
            "writes_completed": int(fields[7]),
            "sectors_written": int(fields[9]),
            "io_time_ms": int(fields[12]),
        }

print(json.dumps({
    "timestamp_unix": time.time(),
    "role": role,
    "load": [load1, load5, load15],
    "mem_total_bytes": meminfo["MemTotal"],
    "mem_available_bytes": meminfo["MemAvailable"],
    "swap_free_bytes": meminfo.get("SwapFree", 0),
    "network": network,
    "disks": disks,
    "sampler_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
}, separators=(",", ":")))
PY
  docker stats --no-stream --format '{{json .}}' 2>/dev/null \
    | python3 -c '
import json
import sys
import time
for line in sys.stdin:
    print(json.dumps({
        "timestamp_unix": time.time(),
        "role": sys.argv[1],
        "docker": json.loads(line),
    }, separators=(",", ":")))
' "${role}" >>"${output}" || true
  if [[ ${role} == worker ]] && command -v nvidia-smi >/dev/null; then
    nvidia-smi \
      --query-gpu=index,utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu \
      --format=csv,noheader,nounits 2>/dev/null |
      python3 -c '
import json
import sys
import time
for line in sys.stdin:
    index, util, used, total, power, temperature = [
        value.strip() for value in line.split(",")
    ]
    print(json.dumps({
        "timestamp_unix": time.time(),
        "role": "worker",
        "gpu": {
            "index": int(index),
            "utilization_percent": float(util),
            "memory_used_mib": float(used),
            "memory_total_mib": float(total),
            "power_watts": float(power),
            "temperature_c": float(temperature),
        },
    }, separators=(",", ":")))
' >>"${output}" || true
  fi
  sleep 5
done
