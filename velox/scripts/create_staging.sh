#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_SCRIPT="$(cd "${SCRIPT_DIR}/../.." && pwd)/scripts/create_staging_branch.sh"
DEFAULT_TARGET_PATH="$(cd "${SCRIPT_DIR}/../../.." && pwd)/velox"

usage() {
  cat << EOF
Velox Staging Branch Creator

Creates a staging branch by merging PRs from the NVIDIA libcudf project
board (Velox Staging = Staging) that target facebookincubator/velox.
Target path: ${DEFAULT_TARGET_PATH}

Examples:
  ./velox/scripts/create_staging.sh                              # Auto-fetch Staging-column PRs
  ./velox/scripts/create_staging.sh --manual-pr-numbers "1,2,3"  # Merge specific PRs
  ./velox/scripts/create_staging.sh --force-push true            # Force push to remote

Note: In local mode (default), push to remote is skipped. Use --mode ci to push.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  echo ""
  echo "=== Parent script options ==="
  "${PARENT_SCRIPT}" --help
  exit 0
fi

exec "${PARENT_SCRIPT}" \
  --target-path "${DEFAULT_TARGET_PATH}" \
  --base-repository "facebookincubator/velox" \
  --base-branch "main" \
  --target-branch "staging" \
  "$@"
