#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Tears down the Spark Connect server started by start_spark_connect.sh.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

docker compose -f "${REPO_ROOT}/spark_gluten/docker/docker-compose.spark-connect.yml" down 2>/dev/null || true
