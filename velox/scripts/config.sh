# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-build"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}

# expected output directory
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
EXPECTED_OUTPUT_LIB_DIR="${EXPECTED_OUTPUT_DIR}/lib"

COMPOSE_FILE="../docker/docker-compose.adapters.yml"
