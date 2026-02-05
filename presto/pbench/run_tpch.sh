#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_DIR="${SCRIPT_DIR}/../.."
OUTPUT_DIR="$BASE_DIR/pbench_output/tpch"
PBENCH_DIR="${SCRIPT_DIR}"
COORD="localhost:8080"

mkdir -p $OUTPUT_DIR
"$PBENCH_DIR/pbench" run -s http://$COORD/ -o $OUTPUT_DIR "$PBENCH_DIR/benchmarks/tpch/sf100.json"
