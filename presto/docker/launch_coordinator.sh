#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

LOGS_DIR="/opt/presto-server/logs"
mkdir -p "${LOGS_DIR}"
log_file="${LOGS_DIR}/coordinator.log"

exec /opt/presto-server/bin/launcher run >> "${log_file}" 2>&1
