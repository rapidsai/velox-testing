#!/bin/bash
set -e

ETC_BASE="/opt/presto-server/etc"

# Resolve the NUMA node closest to a GPU and launch presto_server pinned to it.
#   $1 — GPU ID
#   $2 — etc-dir path for this instance
launch_worker() {
  local gpu_id=$1 etc_dir=$2
  echo "Launching on GPU $gpu_id (config: $etc_dir)"

  local topo
  topo=$(nvidia-smi topo -C -M -i "$gpu_id")
  echo "$topo"

  local cpu_numa mem_numa
  cpu_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest CPU/{ gsub(/ /,"",$2); print $2 }')
  mem_numa=$(echo "$topo" | awk -F: '/NUMA IDs of closest memory/{ gsub(/ /,"",$2); print $2 }')

  local launcher=()
  if [[ $cpu_numa =~ ^[0-9]+$ ]]; then
    launcher=(numactl --cpunodebind="$cpu_numa")
    if [[ $mem_numa =~ ^[0-9]+$ ]]; then
      launcher+=(--membind="$mem_numa")
    else
      launcher+=(--membind="$cpu_numa")
    fi
  fi

  CUDA_VISIBLE_DEVICES="$gpu_id" "${launcher[@]}" presto_server --etc-dir="$etc_dir" &
}

# No args → single worker using CUDA_VISIBLE_DEVICES (default 0), shared config dir.
# With args → one worker per GPU ID, each with its own config dir (etc<gpu_id>).
if [ $# -eq 0 ]; then
  launch_worker "${CUDA_VISIBLE_DEVICES:-0}" "${ETC_BASE}/"
else
  for gpu_id in "$@"; do
    launch_worker "$gpu_id" "${ETC_BASE}${gpu_id}"
  done
fi

wait
