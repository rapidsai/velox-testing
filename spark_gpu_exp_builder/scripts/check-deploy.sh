#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Validate deploy artifacts (bundle JAR + shared libraries).
#
# Checks:
#   1. Bundle JAR exists
#   2. All shared libraries present in libs/
#   3. RPATH set to $ORIGIN on key libraries (libgluten.so, libvelox.so)
#   4. No unresolved dependencies (ldd "not found")
#
# Usage:
#   ./check-deploy.sh --output-dir=/path/to/build_<epoch>
#   ./check-deploy.sh --image=gluten:spark-gpu-runtime [--deploy-path=/opt/gluten-deploy]

set -euo pipefail

PROG_NAME="${GGBUILD_CMD:-$(basename "$0")}"
OUTPUT_DIR=""
IMAGE=""
DEPLOY_PATH="/opt/gluten-deploy"  # default path inside the image

usage() {
  cat <<EOF
Usage: ${PROG_NAME} [options]

Validate deploy artifacts (bundle JAR + shared libraries).

Options:
  --output-dir=PATH     Check a local output directory
  --image=IMAGE         Check inside a Docker image
  --deploy-path=PATH    Path inside the image (default: /opt/gluten-deploy)
  -h, --help            Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --output-dir=*)  OUTPUT_DIR="${arg#*=}" ;;
    --image=*)       IMAGE="${arg#*=}" ;;
    --deploy-path=*) DEPLOY_PATH="${arg#*=}" ;;
    -h|--help)       usage; exit 0 ;;
    *)               echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

if [ -z "$OUTPUT_DIR" ] && [ -z "$IMAGE" ]; then
  echo "ERROR: Specify --output-dir or --image."
  usage; exit 1
fi
if [ -n "$OUTPUT_DIR" ] && [ -n "$IMAGE" ]; then
  echo "ERROR: Specify only one of --output-dir or --image."
  usage; exit 1
fi

# ── Execution helpers ────────────────────────────────────────────────────────
# _run: execute a command locally or inside the Docker image.
# _deploy: the deploy directory path (local or container).
if [ -n "$OUTPUT_DIR" ]; then
  if [ ! -d "$OUTPUT_DIR" ]; then
    echo "ERROR: Directory not found: $OUTPUT_DIR"
    exit 1
  fi
  _DEPLOY="$OUTPUT_DIR"
  _run() { eval "$1"; }
  echo "Checking local directory: $OUTPUT_DIR"
else
  _DEPLOY="$DEPLOY_PATH"
  _run() { docker run --rm "$IMAGE" bash -c "$1"; }
  echo "Checking image: $IMAGE (deploy path: $DEPLOY_PATH)"
fi

ERRORS=0
WARNINGS=0

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; ERRORS=$((ERRORS + 1)); }
warn() { echo "  WARN: $1"; WARNINGS=$((WARNINGS + 1)); }

# ── 1. Bundle JAR ───────────────────────────────────────────────────────────
echo ""
echo "=== 1/4 Bundle JAR ==="
JAR_LIST=$(_run "ls -1 ${_DEPLOY}/gluten-velox-bundle-*.jar 2>/dev/null" || true)
if [ -n "$JAR_LIST" ]; then
  while IFS= read -r jar; do
    SIZE=$(_run "du -h '$jar' | cut -f1")
    pass "$(basename "$jar") ($SIZE)"
  done <<< "$JAR_LIST"
else
  fail "No gluten-velox-bundle-*.jar found in ${_DEPLOY}/"
fi

# ── 2. Shared libraries ─────────────────────────────────────────────────────
echo ""
echo "=== 2/4 Shared libraries ==="
LIBS_DIR="${_DEPLOY}/libs"
LIB_LIST=$(_run "ls -1 ${LIBS_DIR}/ 2>/dev/null" || true)
if [ -z "$LIB_LIST" ]; then
  fail "libs/ directory is empty or missing"
else
  LIB_COUNT=$(echo "$LIB_LIST" | wc -l)
  echo "  Found $LIB_COUNT files in libs/"

  # Check key libraries are present.
  KEY_LIBS=(libgluten.so libvelox.so libcudf.so librmm.so libfolly.so libcudart.so)
  for lib in "${KEY_LIBS[@]}"; do
    FOUND=$(_run "ls ${LIBS_DIR}/${lib}* 2>/dev/null | head -1" || true)
    if [ -n "$FOUND" ]; then
      SIZE=$(_run "du -h '${FOUND}' | cut -f1")
      pass "$lib ($SIZE)"
    else
      fail "$lib not found"
    fi
  done
fi

# ── 3. RPATH check ──────────────────────────────────────────────────────────
echo ""
echo "=== 3/4 RPATH verification ==="
for lib in libgluten.so libvelox.so; do
  LIB_PATH="${LIBS_DIR}/${lib}"
  EXISTS=$(_run "test -f '${LIB_PATH}' && echo yes" || true)
  if [ "$EXISTS" != "yes" ]; then
    warn "$lib not found — skipping RPATH check"
    continue
  fi

  RUNPATH=$(_run "readelf -d '${LIB_PATH}' 2>/dev/null | grep -oP '(?<=Library runpath: \\[)[^\\]]+'" || true)
  RPATH=$(_run "readelf -d '${LIB_PATH}' 2>/dev/null | grep -oP '(?<=Library rpath: \\[)[^\\]]+'" || true)
  EFFECTIVE="${RUNPATH:-$RPATH}"

  if [ -z "$EFFECTIVE" ]; then
    fail "$lib has no RPATH/RUNPATH set"
  elif echo "$EFFECTIVE" | grep -qE '/opt/|/usr/local/'; then
    fail "$lib has container paths in RPATH: $EFFECTIVE"
  elif echo "$EFFECTIVE" | grep -q '$ORIGIN'; then
    pass "$lib RUNPATH: $EFFECTIVE"
  else
    warn "$lib RPATH: $EFFECTIVE (expected \$ORIGIN)"
  fi
done

# ── 4. Unresolved dependencies ──────────────────────────────────────────────
echo ""
echo "=== 4/4 Dependency resolution ==="
for lib in libgluten.so libvelox.so; do
  LIB_PATH="${LIBS_DIR}/${lib}"
  EXISTS=$(_run "test -f '${LIB_PATH}' && echo yes" || true)
  if [ "$EXISTS" != "yes" ]; then
    warn "$lib not found — skipping ldd check"
    continue
  fi

  NOT_FOUND=$(_run "LD_LIBRARY_PATH='${LIBS_DIR}' ldd '${LIB_PATH}' 2>&1 | grep 'not found'" || true)
  if [ -z "$NOT_FOUND" ]; then
    pass "$lib — all dependencies resolved"
  else
    fail "$lib has unresolved dependencies:"
    echo "$NOT_FOUND" | sed 's/^/         /'
  fi
done

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
if [ "$ERRORS" -eq 0 ]; then
  echo " ALL CHECKS PASSED ($WARNINGS warning(s))"
else
  echo " $ERRORS CHECK(S) FAILED, $WARNINGS WARNING(S)"
fi
echo "=============================================="
exit "$ERRORS"
