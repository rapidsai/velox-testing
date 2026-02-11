#!/bin/bash

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Optional local cuDF checkout for Velox-cuDF integration builds.
#
# Usage:
#   PRESTO_CUDF_DIR=/home/devavret/Development/Velox/cudf ./start_native_gpu_dev_presto.sh
#
# Notes:
# - When PRESTO_CUDF_DIR is set, gpu-dev will mount the checkout into the dev worker
#   container and point Velox's cuDF FetchContent at it.
# - Extra CMake flags should be added via PRESTO_EXTRA_CMAKE_FLAGS_APPEND to avoid
#   duplicating the default PRESTO_EXTRA_CMAKE_FLAGS across scripts.
# - The script also uses a different PRESTO_BUILD_DIR_NAME by default
#   ("relwithdebinfo-localcudf") so you can switch PRESTO_CUDF_DIR on/off without
#   clearing the build directory. Set PRESTO_BUILD_DIR_NAME explicitly to override.

VARIANT_TYPE=gpu-dev SCRIPT_NAME=$0 ./start_presto_helper.sh "$@"

