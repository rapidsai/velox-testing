#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Discovers Velox cuDF test executables labeled cuda_driver via ctest.
# Outputs a JSON array of test names to GITHUB_OUTPUT for use in matrix strategies.

BUILD_DIR="/opt/velox-build/release"

if [ ! -d "${BUILD_DIR}" ]; then
  echo "Error: Build directory ${BUILD_DIR} not found" >&2
  exit 1
fi

cd "${BUILD_DIR}"

# Tests to exclude from compute-sanitizer (no CUDA API calls, test infra issues, etc.)
EXCLUDED_TESTS=(
  "velox_cudf_config_test"  # No CUDA API calls - only tests CPU config parsing
)

# Extract test names from ctest for the cuda_driver label
tests=$(ctest --show-only=json-v1 --label-regex cuda_driver \
  | python3 -c "
import sys, json
excluded = set('''${EXCLUDED_TESTS[*]}'''.split())
data = json.load(sys.stdin)
names = sorted(t['name'] for t in data.get('tests', []) if t['name'] not in excluded)
print(json.dumps(names))
")

count=$(echo "${tests}" | python3 -c "import sys, json; print(len(json.load(sys.stdin)))")

if [ "${count}" -eq 0 ]; then
  echo "Error: No test executables found matching cuda_driver label" >&2
  exit 1
fi

echo "Found ${count} cuda_driver tests:" >&2
echo "${tests}" | python3 -c "import sys, json; [print(f'  {t}') for t in json.load(sys.stdin)]" >&2

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "tests=${tests}" >> "${GITHUB_OUTPUT}"
fi
