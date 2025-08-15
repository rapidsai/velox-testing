#!/usr/bin/env bash
set -euo pipefail

# validate_directories_exist <dir1> [<dir2> ...]
# Checks that each directory in the argument list exists. Exits with error if
# any are missing.
validate_directories_exist() {
  if [[ "$#" -eq 0 ]]; then
    echo "Usage: verify_directories_exist <dir1> [<dir2> ...]" >&2
    exit 2
  fi

  local missing=0
  for dir in "$@"; do
    if [[ ! -d "$dir" ]]; then
      echo "ERROR: Expected directory '$dir' does not exist." >&2
      missing=1
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

# If executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  validate_directories_exist "$@"
fi
