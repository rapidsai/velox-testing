# script which shares common variables and functions for the velox build and test scripts

# Container names for different build targets
GPU_CONTAINER_NAME="velox-adapters-build"
CPU_CONTAINER_NAME="velox-adapters-build-cpu"

# Docker Compose service names for different build targets
GPU_COMPOSE_SERVICE="velox-adapters-build"
CPU_COMPOSE_SERVICE="velox-adapters-build-cpu"

# Backward compatibility - default container name (GPU)
CONTAINER_NAME="$GPU_CONTAINER_NAME"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}

# expected output directory
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
EXPECTED_OUTPUT_LIB_DIR="${EXPECTED_OUTPUT_DIR}/lib"

COMPOSE_FILE="../docker/docker-compose.adapters.yml"
