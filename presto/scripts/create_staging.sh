#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_SCRIPT="$(cd "${SCRIPT_DIR}/../.." && pwd)/scripts/create_staging_branch.sh"
DEFAULT_TARGET_PATH="$(cd "${SCRIPT_DIR}/../../.." && pwd)/presto"

exec "${PARENT_SCRIPT}" \
  --target-path "${DEFAULT_TARGET_PATH}" \
  --base-repository "prestodb/presto" \
  --base-branch "master" \
  --target-branch "staging" \
  --auto-fetch-prs false \
  "$@"
