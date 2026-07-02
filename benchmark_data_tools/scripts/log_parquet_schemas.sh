#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DATA_TOOLS_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$BENCHMARK_DATA_TOOLS_DIR/.."

cd "$BENCHMARK_DATA_TOOLS_DIR"
"$REPO_ROOT/scripts/run_py_script.sh" -p "$BENCHMARK_DATA_TOOLS_DIR/log_parquet_schemas.py" -- "$@"
