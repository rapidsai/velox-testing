#!/usr/bin/env bash
set -euo pipefail

# Presto Staging Branch Creator
# Creates a staging branch by merging specified PRs from prestodb/presto
#
# Prerequisites:
#   - presto repo cloned as sibling to velox-testing
#   - gh CLI installed and authenticated (gh auth login)
#
# Examples (run from velox-testing directory):
#
#   # Specify PR numbers to merge (required - auto-fetch is disabled)
#   ./presto/scripts/create_staging.sh --manual-pr-numbers "27057,27054,27052"
#
#   # Enable auto-fetch with PR label
#   ./presto/scripts/create_staging.sh --auto-fetch-prs true --pr-labels "gpu"
#
#   # Force push (overwrites remote staging branch)
#   ./presto/scripts/create_staging.sh --manual-pr-numbers "27057,29056" --force-push true
#

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
