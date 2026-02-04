#!/usr/bin/env bash
set -euo pipefail

# Velox Staging Branch Creator
# Creates a staging branch by merging PRs with "cudf" label from facebookincubator/velox
#
# Prerequisites:
#   - velox repo cloned as sibling to velox-testing
#   - gh CLI installed and authenticated (gh auth login)
#
# Examples (run from velox-testing directory):
#
#   # Basic usage - auto-fetches PRs with "cudf" label
#   ./velox/scripts/create_staging.sh
#
#   # Specify manual PR numbers (disables auto-fetch)
#   ./velox/scripts/create_staging.sh --manual-pr-numbers "16075,16050"
#
#   # Multiple PR labels
#   ./velox/scripts/create_staging.sh --auto-fetch-prs true --pr-labels "cudf,ready-to-merge"
#
#   # Force push (overwrites remote staging branch)
#   ./velox/scripts/create_staging.sh --force-push true
#
# Note: In local mode (default), push to remote is skipped.
#       Use --mode ci to enable pushing.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_SCRIPT="$(cd "${SCRIPT_DIR}/../.." && pwd)/scripts/create_staging_branch.sh"
DEFAULT_TARGET_PATH="$(cd "${SCRIPT_DIR}/../../.." && pwd)/velox"

exec "${PARENT_SCRIPT}" \
  --target-path "${DEFAULT_TARGET_PATH}" \
  --base-repository "facebookincubator/velox" \
  --base-branch "main" \
  --target-branch "staging" \
  --pr-labels "cudf" \
  "$@"
