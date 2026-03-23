#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Collect all required shared libraries for LD_LIBRARY_PATH-based deployment.
#
# Produces a flat directory of .so files that can be used with LD_LIBRARY_PATH
# instead of the thirdparty JAR + loadLibFromJar mechanism.
#
# Adapted from gluten/dev/build-thirdparty.sh.  CentOS 9 only initially.
#
# Required env vars:
#   GLUTEN_DIR  — path to gluten source root
#
# Optional env vars:
#   DEPLOY_DIR  — output base directory (default: $GLUTEN_DIR/deploy)

set -eux

: "${GLUTEN_DIR:?ERROR: GLUTEN_DIR is not set}"

LINUX_OS=$(. /etc/os-release && echo ${ID})
VERSION=$(. /etc/os-release && echo ${VERSION_ID})
ARCH=$(uname -m)

DEPLOY_DIR="${DEPLOY_DIR:-${GLUTEN_DIR}/deploy}"
LIBS_DIR="${DEPLOY_DIR}/libs"

mkdir -p "$LIBS_DIR"

echo "============================================================"
echo " Collecting shared libraries for LD_LIBRARY_PATH deployment"
echo "============================================================"
echo "  OS      : $LINUX_OS $VERSION ($ARCH)"
echo "  Output  : $LIBS_DIR"
echo ""

# ── Gluten native libraries (from the build) ─────────────────────────────────
RELEASES_DIR="${GLUTEN_DIR}/cpp/build/releases"
if [ -d "$RELEASES_DIR" ]; then
  echo "[1/6] Copying Gluten native libraries..."
  for lib in libgluten.so libvelox.so; do
    if [ -f "$RELEASES_DIR/$lib" ]; then
      cp "$RELEASES_DIR/$lib" "$LIBS_DIR/"
      echo "      $lib ($(du -h "$RELEASES_DIR/$lib" | cut -f1))"
    fi
  done
else
  echo "[1/6] WARNING: $RELEASES_DIR not found — libgluten.so / libvelox.so not copied."
fi

# ── OS-specific system libraries ──────────────────────────────────────────────
echo ""
echo "[2/6] Copying system libraries..."

function collect_centos_9 {
  # Libraries from build-thirdparty.sh process_setup_centos_9
  cp /lib64/{libre2.so.9,libdouble-conversion.so.3,libevent-2.1.so.7,libdwarf.so.0,libicudata.so.67,libicui18n.so.67,libicuuc.so.67,libsodium.so.23} "$LIBS_DIR/"
  cp /usr/local/lib/{libboost_context.so.1.84.0,libboost_filesystem.so.1.84.0,libboost_program_options.so.1.84.0,libboost_regex.so.1.84.0,libboost_system.so.1.84.0,libboost_thread.so.1.84.0,libboost_atomic.so.1.84.0} "$LIBS_DIR/"
  cp /usr/local/lib64/{libgflags.so.2.2,libglog.so.1,libgeos.so.3.10.7} "$LIBS_DIR/"

  # Additional libraries needed for LD_LIBRARY_PATH mode that the thirdparty
  # JAR approach does NOT bundle (it relies on the host having them).
  # These are essential when deploying CentOS-9-built binaries to other distros.
  cp /lib64/libcrypto.so.3   "$LIBS_DIR/" 2>/dev/null || true
  cp /lib64/libssl.so.3      "$LIBS_DIR/" 2>/dev/null || true
  cp /lib64/libcurl.so.4     "$LIBS_DIR/" 2>/dev/null || true
  cp /lib64/liblz4.so.1      "$LIBS_DIR/" 2>/dev/null || true
  cp /lib64/libzstd.so.1     "$LIBS_DIR/" 2>/dev/null || true
}

case "$LINUX_OS" in
  centos|rhel)
    if [[ "$VERSION" == 9* ]]; then
      collect_centos_9
    else
      echo "ERROR: Only CentOS/RHEL 9 is supported. Found: $LINUX_OS $VERSION"
      exit 1
    fi
    ;;
  *)
    echo "ERROR: Only CentOS/RHEL 9 is supported. Found: $LINUX_OS $VERSION"
    echo "  To add support for other distros, add a collect_<os>_<version> function."
    exit 1
    ;;
esac

# ── GPU/RAPIDS shared libraries ──────────────────────────────────────────────
echo ""
echo "[3/6] Copying GPU/RAPIDS libraries..."

GPU_LIBS=(
  libcudf.so
  librmm.so
  libkvikio.so
  librapids_logger.so
  libnvcomp.so.5
  libnvcomp_cpu.so.5
  libprotobuf.so.32
)
for lib in "${GPU_LIBS[@]}"; do
  for dir in /usr/local/lib /usr/local/lib64; do
    if [ -f "$dir/$lib" ]; then
      cp "$dir/$lib" "$LIBS_DIR/"
      echo "      $lib"
      break
    fi
  done
done

# Folly (may or may not be present as a shared lib).
# Copy ALL libfolly variants (symlinks + versioned files) so the dynamic
# linker can resolve e.g. libfolly.so.0.58.0-dev which libvelox.so links against.
for dir in /usr/local/lib /usr/local/lib64; do
  if ls "$dir"/libfolly.so* >/dev/null 2>&1; then
    cp -a "$dir"/libfolly.so* "$LIBS_DIR/"
    for f in "$dir"/libfolly.so*; do
      echo "      $(basename "$f")"
    done
    break
  fi
done

# ── CUDA runtime libraries ───────────────────────────────────────────────────
echo ""
echo "[4/6] Copying CUDA runtime libraries..."

CUDA_LIB_DIRS=(
  /usr/local/cuda/lib64
  /usr/local/cuda/targets/${ARCH}-linux/lib
)
# Find the actual CUDA toolkit version directory (e.g., /usr/local/cuda-13.1)
for d in /usr/local/cuda-*/targets/${ARCH}-linux/lib; do
  [ -d "$d" ] && CUDA_LIB_DIRS+=("$d")
done

CUDA_LIB_PATTERNS=(
  "libcudart.so.*"
  "libnvJitLink.so.*"
  "libnvrtc.so.*"
  "libnvrtc-builtins.so.*"
)
for pattern in "${CUDA_LIB_PATTERNS[@]}"; do
  found=false
  for dir in "${CUDA_LIB_DIRS[@]}"; do
    # shellcheck disable=SC2086
    for f in $dir/$pattern; do
      [ -e "$f" ] || continue
      if [ -L "$f" ]; then
        # Copy symlinks as-is (e.g. libcudart.so.13 -> libcudart.so.13.1.105).
        cp -a "$f" "$LIBS_DIR/"
      elif [ -f "$f" ]; then
        cp "$f" "$LIBS_DIR/"
      fi
      echo "      $(basename "$f")"
      found=true
    done
    $found && break
  done
done

# ── Copy bundle JAR to deploy/ ───────────────────────────────────────────────
echo ""
echo "[5/6] Copying bundle JAR..."

BUNDLE_JAR=$(ls "$GLUTEN_DIR"/package/target/gluten-velox-bundle-spark*-linux_amd64-*.jar 2>/dev/null | head -1)
if [ -n "$BUNDLE_JAR" ]; then
  cp "$BUNDLE_JAR" "$DEPLOY_DIR/"
  echo "      $(basename "$BUNDLE_JAR")"
else
  echo "      WARNING: No bundle JAR found in $GLUTEN_DIR/package/target/"
fi

# ── Patch RPATH for portability ────────────────────────────────────────────────
echo ""
echo "[6/6] Patching RPATH to \$ORIGIN for portability..."
# Auto-install patchelf if not present (we're inside a build container).
if ! command -v patchelf &>/dev/null; then
  echo "      patchelf not found — installing..."
  if command -v dnf &>/dev/null; then
    dnf install -y patchelf 2>&1 | tail -1
  elif command -v yum &>/dev/null; then
    yum install -y patchelf 2>&1 | tail -1
  elif command -v apt-get &>/dev/null; then
    apt-get update -qq && apt-get install -y -qq patchelf 2>&1 | tail -1
  fi
fi

if ! command -v patchelf &>/dev/null; then
  echo "ERROR: patchelf could not be installed. Libraries will have container paths in RPATH."
  echo "       You MUST set LD_LIBRARY_PATH at runtime on the remote host."
else
  PATCH_OK=0
  PATCH_FAIL=0
  for lib in "$LIBS_DIR"/*.so*; do
    [ -f "$lib" ] || continue
    [ -L "$lib" ] && continue
    if patchelf --set-rpath '$ORIGIN' "$lib" 2>&1; then
      PATCH_OK=$((PATCH_OK + 1))
    else
      echo "      FAILED: $(basename "$lib")"
      PATCH_FAIL=$((PATCH_FAIL + 1))
    fi
  done
  echo "      Patched: $PATCH_OK, Failed: $PATCH_FAIL"
  if [ "$PATCH_FAIL" -gt 0 ]; then
    echo "      Retrying failed libraries with --page-size 65536..."
    for lib in "$LIBS_DIR"/*.so*; do
      [ -f "$lib" ] || continue
      [ -L "$lib" ] && continue
      CURRENT_RPATH=$(patchelf --print-rpath "$lib" 2>/dev/null || true)
      if [ "$CURRENT_RPATH" != '$ORIGIN' ]; then
        if patchelf --page-size 65536 --set-rpath '$ORIGIN' "$lib" 2>&1; then
          echo "      RETRY OK: $(basename "$lib")"
        else
          echo "      RETRY FAILED: $(basename "$lib") — set LD_LIBRARY_PATH at runtime for this lib."
        fi
      fi
    done
  fi
  echo "      Verifying RPATH on key libraries..."
  for lib in libvelox.so libgluten.so; do
    if [ -f "$LIBS_DIR/$lib" ]; then
      RPATH=$(patchelf --print-rpath "$LIBS_DIR/$lib" 2>/dev/null || true)
      echo "      $lib RPATH: ${RPATH:-(not set)}"
    fi
  done
fi

# ── Validation: check for unresolved dependencies ────────────────────────────
echo ""
echo "============================================================"
echo " Validating library completeness"
echo "============================================================"
MISSING=0
for lib in "$LIBS_DIR"/libgluten.so "$LIBS_DIR"/libvelox.so; do
  if [ -f "$lib" ]; then
    # Check RPATH — should be $ORIGIN after patchelf, not container paths.
    RPATH_VAL=$(readelf -d "$lib" 2>/dev/null | grep -oP '(?<=Library r(un)?path: \[)[^\]]+' || true)
    if echo "$RPATH_VAL" | grep -q '/opt/\|/usr/local/' 2>/dev/null; then
      echo "WARNING: $(basename "$lib") still has container paths in RPATH:"
      echo "         $RPATH_VAL"
      echo "         patchelf may have failed — libraries will not resolve on remote hosts."
      MISSING=1
    elif [ -n "$RPATH_VAL" ]; then
      echo "  $(basename "$lib") RPATH: $RPATH_VAL"
    fi

    # Check with RPATH alone (no LD_LIBRARY_PATH) to simulate remote host.
    NOT_FOUND=$(ldd "$lib" 2>/dev/null | grep "not found" || true)
    if [ -n "$NOT_FOUND" ]; then
      echo "WARNING: Unresolved dependencies for $(basename "$lib") (RPATH-only):"
      echo "$NOT_FOUND"
      MISSING=1
    else
      echo "  $(basename "$lib"): all dependencies resolved via RPATH"
    fi
  fi
done

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " Deploy directory ready"
echo "============================================================"
echo ""
echo " $DEPLOY_DIR/"
ls -1 "$DEPLOY_DIR"/*.jar 2>/dev/null | while read f; do echo "   $(basename "$f")"; done
echo "   libs/"
ls -1 "$LIBS_DIR"/ | while read f; do echo "     $f"; done
echo ""
LIB_COUNT=$(ls -1 "$LIBS_DIR"/ | wc -l)
echo " Total: $LIB_COUNT files in libs/"
echo ""
echo " Usage:"
echo "   export LD_LIBRARY_PATH=$LIBS_DIR:\${LD_LIBRARY_PATH:-}"
echo "   spark-submit --jars $DEPLOY_DIR/$(basename "${BUNDLE_JAR:-gluten-velox-bundle.jar}") \\"
echo "     --conf spark.executor.extraLibraryPath=$LIBS_DIR \\"
echo "     --conf spark.driver.extraLibraryPath=$LIBS_DIR \\"
echo "     --conf spark.plugins=org.apache.gluten.GlutenPlugin \\"
echo "     --conf spark.gluten.sql.columnar.cudf=true \\"
echo "     ..."
echo "   # Note: spark.gluten.loadLibFromJar is NOT set (defaults to false)"
echo "============================================================"
