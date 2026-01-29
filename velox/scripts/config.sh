#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# script which shares common variables and functions for the velox build and test scripts

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# container name (exported for use by scripts that source this file)
export CONTAINER_NAME="velox-adapters-build"
export COMPOSE_FILE="${SCRIPT_DIR}/../docker/docker-compose.adapters.build.yml"
export COMPOSE_FILE_SCCACHE="${SCRIPT_DIR}/../docker/docker-compose.adapters.build.sccache.yml"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}
