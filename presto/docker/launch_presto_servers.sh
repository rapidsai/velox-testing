#!/bin/bash

set -e
# Run ldconfig once
echo ldconfig

if [ $# -eq 0 ]; then
  presto_server --etc-dir="/opt/presto-server/etc/" &
else
# Launch workers in parallel, each pinned to a different GPU
# The GPU IDs are passed as command-line arguments
for gpu_id in "$@"; do
  CUDA_VISIBLE_DEVICES=$gpu_id presto_server --etc-dir="/opt/presto-server/etc${gpu_id}" &
done
fi

# Wait for all background processes
wait
