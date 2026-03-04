#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Centralized defaults for Presto coordinator connection.
DEFAULT_PRESTO_HOST_NAME="${DEFAULT_PRESTO_HOST_NAME:-localhost}"
DEFAULT_PRESTO_PORT="${DEFAULT_PRESTO_PORT:-8080}"

set_presto_coordinator_defaults() {
  HOST_NAME="${HOST_NAME:-${DEFAULT_PRESTO_HOST_NAME}}"
  PORT="${PORT:-${DEFAULT_PRESTO_PORT}}"
}
