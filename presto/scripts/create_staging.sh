#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_SCRIPT="$(cd "${SCRIPT_DIR}/../.." && pwd)/scripts/create_staging_branch.sh"
DEFAULT_TARGET_PATH="$(cd "${SCRIPT_DIR}/../../.." && pwd)/presto"

usage() {
  cat << EOF
Presto Staging Branch Creator

Creates a staging branch by merging specified PRs from prestodb/presto.
Target path: ${DEFAULT_TARGET_PATH}

Examples:
  ./presto/scripts/create_staging.sh --manual-pr-numbers "1,2,3"   # Merge specific PRs (required)
  ./presto/scripts/create_staging.sh --manual-pr-numbers "1,2,3" --additional-pr-numbers "4"  # Append PR(s)
  ./presto/scripts/create_staging.sh --auto-fetch-prs true --pr-labels "gpu"  # Auto-fetch by label
  ./presto/scripts/create_staging.sh --manual-pr-numbers "1,2" --force-push true  # Force push

Note: Auto-fetch is disabled by default. Use --manual-pr-numbers or enable --auto-fetch-prs.
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
  --base-repository "prestodb/presto" \
  --base-branch "master" \
  --target-branch "staging" \
  --auto-fetch-prs false \
  "$@"
