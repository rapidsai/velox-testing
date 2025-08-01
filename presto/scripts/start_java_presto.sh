#!/bin/bash

set -e

./stop_presto.sh
docker compose -f ../docker/docker-compose.java.yml up -d
