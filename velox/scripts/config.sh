# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-build"
COMPOSE_FILE="../docker/docker-compose.adapters.build.yml"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}
