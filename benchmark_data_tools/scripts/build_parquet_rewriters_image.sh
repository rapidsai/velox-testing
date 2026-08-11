#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "${script_dir}" rev-parse --show-toplevel)

deps_image=${DEPS_IMAGE:?DEPS_IMAGE must name the freshly built dependency image}
output_image=${PARQUET_REWRITERS_IMAGE:-cleanroom-parquet-rewriters:latest}
cuda_architectures=${CUDA_ARCHITECTURES:-100}

docker build --no-cache --progress plain \
  --file "${repo_root}/benchmark_data_tools/native/parquet_rewriters.dockerfile" \
  --build-arg "BASE_IMAGE=${deps_image}" \
  --build-arg "CUDA_ARCHITECTURES=${cuda_architectures}" \
  --tag "${output_image}" \
  "${repo_root}"

docker inspect --format='{{.Id}}' "${output_image}"
