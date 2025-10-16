#!/bin/bash

set -e
container_name="velox-adapters-deps"
compose_file="../docker/docker-compose.adapters.build.yml"
docker compose -f "${compose_file}" up "${container_name}"
docker compose -f "${compose_file}" down "${container_name}"
echo "Velox dependencies/run-time container image built!"
