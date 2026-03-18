#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
RUN_TIMESTAMP="${RUN_TIMESTAMP:-$(date +"%Y%m%dT%H%M%S")}"
log_file="${LOGS_DIR}/worker_0_${RUN_TIMESTAMP}.log"

exec /opt/presto-server/bin/launcher run >> "${log_file}" 2>&1
