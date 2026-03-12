#!/bin/bash

set -e
# Run ldconfig once
echo ldconfig

if [ $# -eq 0 ]; then
  gpu_id=${CUDA_VISIBLE_DEVICES:-0}
  echo "For GPU $gpu_id"
  nvidia-smi topo -C -M -i $gpu_id
  numa_id=$(nvidia-smi topo -C -i $gpu_id | awk -F':' '/NUMA IDs of closest CPU/{gsub(/ /,"",$2); print $2}')
  if [[ $numa_id =~ ^[0-9]+$ ]]; then
    LAUNCHER="numactl --cpunodebind=$numa_id --membind=$numa_id"
  else
    LAUNCHER=""
  fi
  $LAUNCHER presto_server --etc-dir="/opt/presto-server/etc/" &
else
# Launch workers in parallel, each pinned to a different GPU
# The GPU IDs are passed as command-line arguments
for gpu_id in "$@"; do
  echo "For GPU $gpu_id"
  nvidia-smi topo -C -M -i $gpu_id
  numa_id=$(nvidia-smi topo -C -i $gpu_id | awk -F':' '/NUMA IDs of closest CPU/{gsub(/ /,"",$2); print $2}')
  if [[ $numa_id =~ ^[0-9]+$ ]]; then
    LAUNCHER="numactl --cpunodebind=$numa_id --membind=$numa_id"
  else
    LAUNCHER=""
  fi
  CUDA_VISIBLE_DEVICES=$gpu_id $LAUNCHER presto_server --etc-dir="/opt/presto-server/etc${gpu_id}" &
done
fi

# Wait for all background processes
wait
