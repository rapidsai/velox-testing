#!/bin/bash
# Interactive shell on a compute node with a container image.
# Override IMAGE, NODELIST, GRES, or TIME_LIMIT via environment variables.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/defaults.env"

: "${IMAGE:=${IMAGE_DIR}/presto-native-worker-gpu-karth-Mar11-with-nsys.sqsh}"
: "${NODELIST:=${DEFAULT_SINGLE_NODE}}"
: "${GRES:=gpu:4}"
: "${TIME_LIMIT:=01:00:00}"

srun --nodes=1 \
     --nodelist="${NODELIST}" \
     --ntasks-per-node=1 \
     --gres="${GRES}" \
     --exclusive \
     --time="${TIME_LIMIT}" \
     --container-image="${IMAGE}" \
     --container-mounts="/scratch:/scratch" \
     --container-remap-root \
     --container-writable \
     --pty bash
