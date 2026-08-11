#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "${script_dir}" rev-parse --show-toplevel)

tpchgen_image=${TPCHGEN_IMAGE:?TPCHGEN_IMAGE must name the freshly built pinned source image}
output_image=${DATASET_A_GENERATOR_IMAGE:-cleanroom-dataset-a-generator:latest}

docker build --no-cache --progress plain \
  --file "${repo_root}/benchmark_data_tools/dataset_a_generator.dockerfile" \
  --build-arg "TPCHGEN_IMAGE=${tpchgen_image}" \
  --tag "${output_image}" \
  "${repo_root}"

docker inspect --format='{{.Id}}' "${output_image}"
