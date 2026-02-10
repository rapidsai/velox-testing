# script which shares common variables and functions for the velox build and test scripts

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# container name
CONTAINER_NAME="velox-adapters-build"
COMPOSE_FILE="${SCRIPT_DIR}/../docker/docker-compose.adapters.build.yml"
COMPOSE_FILE_SCCACHE="${SCRIPT_DIR}/../docker/docker-compose.adapters.build.sccache.yml"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}
