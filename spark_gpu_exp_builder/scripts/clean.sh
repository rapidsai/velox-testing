#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Clean build caches for Gluten and/or Velox source trees.
#
# Usage:
#   ./clean.sh --gluten_dir=/path/to/gluten --velox_dir=/path/to/velox
#   ./clean.sh --gluten_dir=/path/to/gluten          # clean Gluten only
#   ./clean.sh --velox_dir=/path/to/velox             # clean Velox only
#   ./clean.sh --gluten_dir=... --velox_dir=... --dry-run
#   ./clean.sh --gluten_dir=... --velox_dir=... --yes

set -euo pipefail

PROG_NAME="${GGBUILD_CMD:-$(basename "$0")}"

# ── Defaults ──────────────────────────────────────────────────────────────────
GLUTEN_DIR=""
VELOX_DIR=""
DRY_RUN=false
SKIP_CONFIRM=false

# ── Argument parsing ──────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: ${PROG_NAME} [options]

Clean build caches for Gluten and/or Velox source trees.

Paths (at least one):
  --gluten_dir=PATH     Path to Gluten source tree (optional)
  --velox_dir=PATH      Path to Velox source tree (optional)

Options:
  --dry-run             Show what would be deleted without removing anything
  --yes                 Skip confirmation prompt
  -h, --help            Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --gluten_dir=*) GLUTEN_DIR="${arg#*=}" ;;
    --velox_dir=*)  VELOX_DIR="${arg#*=}" ;;
    --dry-run)      DRY_RUN=true ;;
    --yes|-y)       SKIP_CONFIRM=true ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

if [ -z "$GLUTEN_DIR" ] && [ -z "$VELOX_DIR" ]; then
  echo "ERROR: At least one of --gluten_dir or --velox_dir is required."
  usage
  exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
TOTAL_BYTES=0
TARGETS=()

# Paths to preserve for clangd / VSCode intellisense.
# compile_commands.json  — compilation database
# .clangd               — clangd config
# .cache/clangd/        — clangd index cache
CLANGD_PRESERVE=("compile_commands.json" ".clangd")

# Check if a path is (or lives inside) a clangd-related file/dir.
is_clangd_path() {
  local p="$1"
  local base
  base=$(basename "$p")
  for keep in "${CLANGD_PRESERVE[@]}"; do
    [ "$base" = "$keep" ] && return 0
  done
  # .cache/clangd dirs (match both the "clangd" dir and its parent ".cache")
  [[ "$p" == */.cache/clangd ]] && return 0
  [[ "$p" == */.cache/clangd/* ]] && return 0
  return 1
}

# Queue a path for removal. Skips if it does not exist or is clangd-related.
queue() {
  local path="$1"
  if [ -e "$path" ]; then
    is_clangd_path "$path" && return 0
    local size
    size=$(du -sb "$path" 2>/dev/null | cut -f1)
    TOTAL_BYTES=$((TOTAL_BYTES + size))
    TARGETS+=("$path")
  fi
}

# Check if a directory tree contains any clangd files worth preserving.
has_clangd_files() {
  local dir="$1"
  [ ! -d "$dir" ] && return 1
  [ -f "$dir/compile_commands.json" ] && return 0
  [ -f "$dir/.clangd" ] && return 0
  [ -d "$dir/.cache/clangd" ] && return 0
  # Check one level deeper (e.g. _build/release/compile_commands.json)
  for sub in "$dir"/*/compile_commands.json "$dir"/*/.clangd "$dir"/*/.cache/clangd; do
    [ -e "$sub" ] && return 0
  done
  return 1
}

# Queue contents of a directory, preserving clangd files inside it.
# Recurses into subdirs that contain clangd files; queues the rest wholesale.
queue_dir_contents() {
  local dir="$1"
  [ ! -d "$dir" ] && return 0
  for item in "$dir"/* "$dir"/.*; do
    [ ! -e "$item" ] && continue
    local base
    base=$(basename "$item")
    [ "$base" = "." ] || [ "$base" = ".." ] && continue
    # Preserve clangd files at this level
    is_clangd_path "$item" && continue
    # Recurse into .cache to preserve .cache/clangd/ but remove the rest
    if [ "$base" = ".cache" ] && [ -d "$item" ]; then
      for sub in "$item"/* "$item"/.*; do
        [ ! -e "$sub" ] && continue
        local subbase
        subbase=$(basename "$sub")
        [ "$subbase" = "." ] || [ "$subbase" = ".." ] && continue
        is_clangd_path "$sub" || queue "$sub"
      done
      continue
    fi
    # If a subdir contains clangd files, recurse instead of removing wholesale
    if [ -d "$item" ] && has_clangd_files "$item"; then
      queue_dir_contents "$item"
      continue
    fi
    queue "$item"
  done
}

human_size() {
  local bytes=$1
  if   (( bytes >= 1073741824 )); then printf "%.1f GB" "$(echo "scale=1; $bytes/1073741824" | bc)"
  elif (( bytes >= 1048576 ));    then printf "%.1f MB" "$(echo "scale=1; $bytes/1048576" | bc)"
  elif (( bytes >= 1024 ));       then printf "%.1f KB" "$(echo "scale=1; $bytes/1024" | bc)"
  else printf "%d B" "$bytes"
  fi
}

# ── Collect Velox targets ────────────────────────────────────────────────────
if [ -n "$VELOX_DIR" ]; then
  if [ ! -d "$VELOX_DIR" ]; then
    echo "ERROR: Velox directory not found: $VELOX_DIR"
    exit 1
  fi
  VELOX_DIR=$(realpath "$VELOX_DIR")

  # CMake build directories (from .gitignore: build/, _build/)
  # Preserve compile_commands.json and .cache/clangd/ inside them.
  queue_dir_contents "$VELOX_DIR/build"
  queue_dir_contents "$VELOX_DIR/_build"

  # ccache inside source tree (from .gitignore: .ccache/, ccache/)
  queue "$VELOX_DIR/.ccache"
  queue "$VELOX_DIR/ccache"

  # Note: velox/compile_commands.json and velox/.cache/clangd/ are preserved.
fi

# ── Collect Gluten targets ───────────────────────────────────────────────────
if [ -n "$GLUTEN_DIR" ]; then
  if [ ! -d "$GLUTEN_DIR" ]; then
    echo "ERROR: Gluten directory not found: $GLUTEN_DIR"
    exit 1
  fi
  GLUTEN_DIR=$(realpath "$GLUTEN_DIR")

  # CMake build output (build/ minus preserved files).
  # Preserves: build/mvn, build/mvnd (Maven wrappers)
  #            build/compile_commands.json, build/.cache/clangd/ (clangd)
  for item in "$GLUTEN_DIR"/build/* "$GLUTEN_DIR"/build/.*; do
    [ ! -e "$item" ] && continue
    base=$(basename "$item")
    [ "$base" = "." ] || [ "$base" = ".." ] && continue
    # Preserve Maven wrappers (per .gitignore: !build/mvn, !build/mvnd)
    [ "$base" = "mvn" ] || [ "$base" = "mvnd" ] && continue
    # Recurse into .cache to preserve .cache/clangd/
    if [ "$base" = ".cache" ] && [ -d "$item" ]; then
      for sub in "$item"/* "$item"/.*; do
        [ ! -e "$sub" ] && continue
        local_base=$(basename "$sub")
        [ "$local_base" = "." ] || [ "$local_base" = ".." ] && continue
        is_clangd_path "$sub" || queue "$sub"
      done
      continue
    fi
    # Preserve clangd files (compile_commands.json, .clangd)
    is_clangd_path "$item" && continue
    queue "$item"
  done

  # C++ backend build (cpp/build/) — preserve compile_commands.json inside
  queue_dir_contents "$GLUTEN_DIR/cpp/build"

  # External project downloads (ep/_ep/)
  queue "$GLUTEN_DIR/ep/_ep"

  # vcpkg installed packages
  queue "$GLUTEN_DIR/dev/vcpkg/vcpkg_installed"

  # Maven target directories — find all **/target/ that git ignores
  while IFS= read -r -d '' tdir; do
    queue "$tdir"
  done < <(find "$GLUTEN_DIR" -name target -type d -not -path "*/node_modules/*" -print0 2>/dev/null)

  # scalastyle output files
  while IFS= read -r -d '' sfile; do
    queue "$sfile"
  done < <(find "$GLUTEN_DIR" -name "scalastyle-output.xml" -type f -print0 2>/dev/null)

  # Build logs
  queue "$GLUTEN_DIR/build.log"
  queue "$GLUTEN_DIR/thirdparty.log"
  queue "$GLUTEN_DIR/package/build.log"
fi

# ── Nothing to do? ────────────────────────────────────────────────────────────
if [ ${#TARGETS[@]} -eq 0 ]; then
  echo "Nothing to clean."
  exit 0
fi

# ── Display summary ──────────────────────────────────────────────────────────
echo ""
echo "The following will be removed:"
echo ""
for t in "${TARGETS[@]}"; do
  sz=$(du -sh "$t" 2>/dev/null | cut -f1)
  printf "  %-8s %s\n" "$sz" "$t"
done
echo ""
echo "Total: $(human_size $TOTAL_BYTES)"
echo ""

if [ "$DRY_RUN" = true ]; then
  echo "(dry-run — nothing was deleted)"
  exit 0
fi

# ── Confirm ──────────────────────────────────────────────────────────────────
if [ "$SKIP_CONFIRM" = false ]; then
  read -r -p "Proceed? [y/N] " answer
  case "$answer" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "Aborted."; exit 0 ;;
  esac
fi

# ── Remove ────────────────────────────────────────────────────────────────────
for t in "${TARGETS[@]}"; do
  rm -rf "$t"
  echo "  removed: $t"
done

echo ""
echo "Done. Freed ~$(human_size $TOTAL_BYTES)."
