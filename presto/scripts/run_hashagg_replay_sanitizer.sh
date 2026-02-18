#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

print_help() {
  cat << EOF

Usage: $(basename "$0") [OPTIONS] [-- replay-args...]

Run velox_cudf_hashagg_replay under compute-sanitizer in the replay image.

OPTIONS:
    -h, --help               Show this help message.
    --image TAG              Docker image tag (default: velox-hashagg-replay-\$USER).
    --dump-dir DIR           Dump directory in the container.
                             Default: /tmp/hashagg_probe_dumps/hashagg_probe_1771056019538078_1
    --dump-input-samples     Print first/last/random input samples.
    --sanitizer-arg ARG      Extra compute-sanitizer argument (repeatable).
    --docker-arg ARG         Extra docker run argument (repeatable).

EXAMPLES:
    $0
    $0 --sanitizer-arg --tool --sanitizer-arg memcheck
    $0 --docker-arg "-e" --docker-arg "CUDA_VISIBLE_DEVICES=0"
    $0 -- --dump_dir /tmp/hashagg_probe_dumps/hashagg_probe_1771056019538078_1

EOF
}

IMAGE_TAG="velox-hashagg-replay-${USER:-latest}"
DUMP_DIR="/tmp/hashagg_probe_dumps/hashagg_probe_1771056019538078_1"
SANITIZER_ARGS=(--tool memcheck)
DOCKER_ARGS=()
REPLAY_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    --image)
      IMAGE_TAG="$2"
      shift 2
      ;;
    --dump-dir)
      DUMP_DIR="$2"
      shift 2
      ;;
    --dump-input-samples)
      REPLAY_ARGS+=(--dump_input_samples)
      shift
      ;;
    --sanitizer-arg)
      SANITIZER_ARGS+=("$2")
      shift 2
      ;;
    --docker-arg)
      DOCKER_ARGS+=("$2")
      shift 2
      ;;
    --)
      shift
      REPLAY_ARGS+=("$@")
      break
      ;;
    *)
      REPLAY_ARGS+=("$1")
      shift
      ;;
  esac
done

docker run --rm -it --gpus all \
  "${DOCKER_ARGS[@]}" \
  "${IMAGE_TAG}" \
  compute-sanitizer "${SANITIZER_ARGS[@]}" \
  /usr/bin/velox_cudf_hashagg_replay --dump_dir "${DUMP_DIR}" \
  "${REPLAY_ARGS[@]}"
