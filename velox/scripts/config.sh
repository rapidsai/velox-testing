#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-build"
COMPOSE_FILE="../docker/docker-compose.adapters.build.yml"
COMPOSE_FILE_SCCACHE="../docker/docker-compose.adapters.build.sccache.yml"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}
