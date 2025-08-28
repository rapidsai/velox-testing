# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-build"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}

# expected output directory
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
EXPECTED_OUTPUT_LIB_DIR="${EXPECTED_OUTPUT_DIR}/lib"

COMPOSE_FILE="../docker/docker-compose.adapters.yml"

# Create a dummy docker env file for building velox.
# The environment variables are not used for building velox but are required to
# be defined.
create_dummy_docker_env_file() {
    echo "Creating dummy docker env file"
  local env_file="./.env-build-velox"
  cat > "$env_file" << EOF
USER_ID=0
GROUP_ID=0
BENCHMARK_RESULTS_HOST_PATH=$(realpath .)
BENCHMARK_TPCH_DATA_HOST_PATH=$(realpath .)
EOF
}