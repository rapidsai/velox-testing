#!/usr/bin/env bash
set -euo pipefail

# validate_repo_layout <velox|presto> [depth]
# - target: which repository to validate is present one directory root above this tree
# - depth: how many directory levels up from the current working directory to treat as the root (default 3)
validate_repo_layout() {
  local target="${1:-}"
  local depth="${2:-3}"

  if [[ -z "${target}" ]]; then
    echo "Usage: validate_repo_layout <velox|presto> [depth]" >&2
    exit 2
  fi

  local root
  root="$(pwd)"
  for ((i=0; i<depth; i++)); do
    root="$(dirname "${root}")"
  done

  case "${target}" in
    velox)
      if [[ ! -d "${root}/velox" ]]; then
        echo "ERROR: Expected Velox checkout at ${root}/velox." >&2
        exit 1
      fi
      ;;
    presto)
      if [[ ! -d "${root}/presto" ]]; then
        echo "ERROR: Expected Presto checkout at ${root}/presto." >&2
        exit 1
      fi
      ;;
    *)
      echo "ERROR: Unknown target '${target}'. Expected 'velox' or 'presto'." >&2
      exit 2
      ;;
  esac
}

# If executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  validate_repo_layout "${1:-}" "${2:-3}"
fi 
