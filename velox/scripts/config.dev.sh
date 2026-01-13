# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-dev"
COMPOSE_FILE="../docker/docker-compose.adapters.dev.yml"
COMPOSE_FILE_SCCACHE="../docker/docker-compose.adapters.dev.sccache.yml"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}
