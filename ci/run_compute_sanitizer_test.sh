#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Runs compute-sanitizer on a single Velox cuDF test executable.
# Usage: ./run_compute_sanitizer_test.sh TOOL_NAME TEST_NAME [additional gtest args...]
# Example: ./run_compute_sanitizer_test.sh memcheck velox_cudf_filter_project_test
# Example: ./run_compute_sanitizer_test.sh racecheck velox_cudf_aggregation_test --gtest_filter=*Sum*

if [ $# -lt 2 ]; then
  echo "Error: Tool and test name required"
  echo "Usage: $0 TOOL_NAME TEST_NAME [additional gtest args...]"
  echo "  TOOL_NAME: compute-sanitizer tool (memcheck, racecheck, initcheck, synccheck)"
  echo "  TEST_NAME: Velox cuDF test name (as reported by ctest)"
  exit 1
fi

TOOL_NAME="${1}"
shift
TEST_NAME="${1}"
shift

BUILD_DIR="/opt/velox-build/release"

# Install compute-sanitizer (not included in the base Velox build image)
CUDA_VERSION_DASHED="${CUDA_VERSION//./-}"
dnf install -y "cuda-sanitizer-${CUDA_VERSION_DASHED}"

echo "Checking GPU"
nvidia-smi

# Resolve test name to executable path and working directory via ctest
read -r TEST_EXECUTABLE TEST_WORKING_DIR < <(ctest --test-dir "${BUILD_DIR}" --show-only=json-v1 --label-regex cuda_driver \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
for t in data.get('tests', []):
    if t['name'] == '${TEST_NAME}':
        exe = t['command'][0]
        workdir = ''
        for prop in t.get('properties', []):
            if prop['name'] == 'WORKING_DIRECTORY':
                workdir = prop['value']
                break
        print(exe, workdir)
        sys.exit(0)
sys.exit(1)
")

if [ -z "${TEST_EXECUTABLE}" ] || [ ! -x "${TEST_EXECUTABLE}" ]; then
  echo "Error: Test executable for '${TEST_NAME}' not found or not executable"
  exit 1
fi

echo "Running compute-sanitizer --tool ${TOOL_NAME} on ${TEST_NAME}"
echo "  executable: ${TEST_EXECUTABLE}"
if [ -n "${TEST_WORKING_DIR}" ]; then
  echo "  working directory: ${TEST_WORKING_DIR}"
  cd "${TEST_WORKING_DIR}"
fi

CS_EXCLUDE_NAMES="kns=nvcomp,kns=zstd,kns=_no_sanitize,kns=_no_${TOOL_NAME}"

compute-sanitizer \
  --tool "${TOOL_NAME}" \
  --force-blocking-launches \
  --kernel-name-exclude "${CS_EXCLUDE_NAMES}" \
  --track-stream-ordered-races all \
  --error-exitcode=1 \
  "${TEST_EXECUTABLE}" \
  "$@"

EXITCODE=$?

echo "compute-sanitizer --tool ${TOOL_NAME} on ${TEST_NAME} exiting with value: ${EXITCODE}"
exit "${EXITCODE}"
