#!/usr/bin/env bash
set -euo pipefail

# Usage: apply_patches.sh [--target velox|presto|all]
# If --target is omitted, defaults to all.
# Examples:
#   apply_patches.sh --target velox
#   apply_patches.sh --target presto
#   apply_patches.sh --target all    # applies velox then presto
#   apply_patches.sh                 # defaults to all (applies velox then presto)

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VELOX_DIR_DEFAULT="$(realpath "$SCRIPT_DIR/../../velox")"
PRESTO_DIR_DEFAULT="$(realpath "$SCRIPT_DIR/../../presto")"
PATCH_ROOT_DEFAULT="$(realpath "$SCRIPT_DIR/../patches")"
TARGET_TYPE="all"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target|--type)
      TARGET_TYPE="$2"; shift 2 ;;
    -t)
      TARGET_TYPE="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1"; exit 2 ;;
  esac
done


apply_for() {
  local target="$1"
  local repo_dir="$2"
  local patch_dir="$3"

  echo "Target: $target"
  echo "Repository directory: $repo_dir"
  echo "Patch directory: $patch_dir"

  if [[ ! -d "$repo_dir/.git" ]]; then
    echo "Skipping: $repo_dir is not a git repository."
    return 0
  fi

  if [[ -d "$patch_dir" ]]; then
    shopt -s nullglob
    local patch_files=("$patch_dir"/*.patch)
    shopt -u nullglob

    if [[ ${#patch_files[@]} -eq 0 ]]; then
      echo "Patches directory exists, but no .patch files found. Skipping."
      return 0
    fi

    echo "Found ${#patch_files[@]} patch(es). Applying..."
    pushd "$repo_dir" >/dev/null
    for patch_file in "${patch_files[@]}"; do
      echo "Applying patch: $patch_file"
      if git apply "$patch_file"; then
        echo "Patch applied successfully."
      else
        echo "Patch failed to apply: $patch_file"
        popd >/dev/null
        return 1
      fi
    done
    popd >/dev/null
  else
    echo "No patches directory found at $patch_dir. Skipping patch application."
    return 0
  fi

  return 0
}

case "$TARGET_TYPE" in
  velox)
    REPO_DIR="$VELOX_DIR_DEFAULT"
    PATCH_DIR="$PATCH_ROOT_DEFAULT/velox"
    apply_for velox "$REPO_DIR" "$PATCH_DIR"
    exit $?
    ;;
  presto)
    REPO_DIR="$PRESTO_DIR_DEFAULT"
    PATCH_DIR="$PATCH_ROOT_DEFAULT/presto"
    apply_for presto "$REPO_DIR" "$PATCH_DIR"
    exit $?
    ;;
  all)
    overall=0
    apply_for velox "$VELOX_DIR_DEFAULT" "$PATCH_ROOT_DEFAULT/velox" || overall=1
    apply_for presto "$PRESTO_DIR_DEFAULT" "$PATCH_ROOT_DEFAULT/presto" || overall=1
    exit $overall
    ;;
  *)
    echo "Unknown --target: $TARGET_TYPE (expected velox|presto|all)"; exit 2 ;;
esac
