#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
: "${SERVER_START_TIMESTAMP:?SERVER_START_TIMESTAMP must be set before starting the container}"
log_file="${LOGS_DIR}/worker_0_${SERVER_START_TIMESTAMP}.log"

exec /opt/presto-server/bin/launcher run >> "${log_file}" 2>&1
