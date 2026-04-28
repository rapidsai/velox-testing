#!/bin/bash
# Interactive shell on a compute node with a container image.
#
# By default Slurm picks any available node in the partition.  Set NODELIST
# to pin to a specific node or a range.
# IMAGE, GRES, and TIME_LIMIT are also overridable via environment.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/defaults.env"

: "${IMAGE:=${IMAGE_DIR}/presto-native-worker-gpu.sqsh}"
: "${GRES:=gpu:4}"
: "${TIME_LIMIT:=01:00:00}"

NODELIST_ARG=()
if [[ -n "${NODELIST:-}" ]]; then
    NODELIST_ARG=(--nodelist="${NODELIST}")
fi

srun --nodes=1 \
     "${NODELIST_ARG[@]}" \
     --ntasks-per-node=1 \
     --gres="${GRES}" \
     --exclusive \
     --time="${TIME_LIMIT}" \
     --container-image="${IMAGE}" \
     --container-mounts="${HOME}:${HOME},/scratch:/scratch" \
     --container-remap-root \
     --container-writable \
     --pty bash
