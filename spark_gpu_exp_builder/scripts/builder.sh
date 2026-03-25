#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# GPU-only Gluten+Velox builder with Docker and Direct execution modes.
#
# Runs a 7-step build pipeline either inside a Docker container (developer
# workstation) or directly in the current shell (CI / k8s pod).
#
# Hardcoded:  ENABLE_GPU=ON, no vcpkg/QAT/GCS/ABFS.
# Kept as flags: cuda_arch, spark_version, enable_hdfs, enable_s3, skip_*,
#                build_cudf, output_dir, mode.
#
# ── Common recipes ──────────────────────────────────────────────────────────
# Full build (direct mode, default):
#   ./scripts/builder.sh --cuda_arch=80 \
#     --gluten_dir=/path/to/gluten --velox_dir=/path/to/velox
#
# Docker mode (developer workstation):
#   ./scripts/builder.sh --mode=docker --image=gluten:prebuild --cuda_arch=80 \
#     --gluten_dir=/path/to/gluten --velox_dir=/path/to/velox
#
# Incremental (Gluten C++ only, reuse Velox build):
#   ./scripts/builder.sh --skip_velox \
#     --gluten_dir=/path/to/gluten --velox_dir=/path/to/velox

set -euo pipefail

trap '
  _rc=$?
  [ $_rc -eq 0 ] && exit 0
  echo ""
  echo "=============================================="
  echo " BUILD FAILED (exit code: $_rc)"
  echo "  See build.log for details."
  echo "=============================================="
' EXIT

# ── Paths & config ────────────────────────────────────────────────────────────
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${MODULE_DIR}/.." && pwd)"

# Source shared CUDA helper from velox-testing.
if [ -f "${REPO_ROOT}/scripts/cuda_helper.sh" ]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/scripts/cuda_helper.sh"
fi

# Source config definitions (single source of truth for defaults & mappings).
# shellcheck source=config_def.sh
source "${SCRIPT_DIR}/config_def.sh"

# ── Defaults ─────────────────────────────────────────────────────────────────
# Read env aliases (CFG_FILE, GLUTEN_DIR, VELOX_DIR, etc.)
apply_env_aliases

# Flags not in config_def table.
CUDA_ARCH_EXPLICIT=false

# ── Argument parsing ─────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

GPU-only Gluten+Velox builder (CentOS 9, Docker or Direct mode).

Required:
  --gluten_dir=PATH       Path to gluten source tree
  --velox_dir=PATH        Path to velox source tree

General:
  --mode=docker|direct    Execution mode (default: direct)
                          direct  = run directly in current shell (CI/k8s)
                          docker  = run inside a Docker container (needs --image)

Image / container (docker mode only):
  --image=IMAGE           Prebuild image to run the build in (default: from config)
  --container=NAME        Reuse an existing running container

Build options:
  --spark_version=VER     Spark version (default: 3.5)
  --cuda_arch=ARCH        CUDA architectures (default: auto-detect local GPU)
  --enable_hdfs=ON|OFF    Build HDFS connector (default: ON)
  --enable_s3=ON|OFF      Build S3 connector (default: OFF)
  --build_cudf            Build cuDF from source (BUNDLED) instead of SYSTEM

Incremental build:
  --build_arrow           Force Arrow build even if pre-installed
  --skip_build_native     Skip Velox + Gluten C++ native builds
  --skip_velox            Skip Velox build only
  --rebuild_velox         Clean Velox build dir and rebuild
  --rebuild_gluten_cpp    Clean Gluten C++ build dir before building
  --ignore_version_check  Skip cudf commit consistency check

Output:
  --output_dir=PATH       Output directory (default: target/build_<epoch>)

  -h, --help              Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --gluten_dir=*)     GLUTEN_DIR="${arg#*=}" ;;
    --velox_dir=*)      VELOX_DIR="${arg#*=}" ;;
    --mode=*)           EXEC_MODE="${arg#*=}" ;;
    --image=*)          PREBUILD_IMAGE="${arg#*=}"; _PREBUILD_IMAGE_FROM_ENV="" ;;
    --container=*)      CONTAINER_NAME="${arg#*=}" ;;
    --spark_version=*)  SPARK_VERSION="${arg#*=}" ;;
    --cuda_arch=*)      CUDA_ARCH="${arg#*=}"; CUDA_ARCH_EXPLICIT=true ;;
    --enable_hdfs=*)    ENABLE_HDFS="${arg#*=}" ;;
    --enable_s3=*)      ENABLE_S3="${arg#*=}" ;;
    --build_arrow)       SKIP_ARROW=false ;;
    --skip_build_native) SKIP_VELOX=true; SKIP_GLUTEN_CPP=true ;;
    --skip_velox)        SKIP_VELOX=true ;;
    --rebuild_velox)    REBUILD_VELOX=true; REBUILD_GLUTEN_CPP=true ;;
    --rebuild_gluten_cpp) REBUILD_GLUTEN_CPP=true ;;
    --build_cudf)       BUILD_CUDF=true ;;
    --ignore_version_check) IGNORE_VERSION_CHECK=true ;;
    --output_dir=*)     OUTPUT_DIR="${arg#*=}" ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

# Apply defaults for anything still unset after CLI + env.
apply_defaults

# Resolve MAVEN_SETTINGS to absolute path if provided.
if [ -n "${MAVEN_SETTINGS:-}" ] && [ "${MAVEN_SETTINGS:0:1}" != "/" ]; then
  MAVEN_SETTINGS="${MODULE_DIR}/${MAVEN_SETTINGS}"
fi

# ── Validate ─────────────────────────────────────────────────────────────────
if [ -z "$GLUTEN_DIR" ]; then
  echo "ERROR: --gluten_dir is required."
  usage; exit 1
fi
if [ ! -d "$GLUTEN_DIR" ]; then
  echo "ERROR: Gluten directory not found: $GLUTEN_DIR"
  exit 1
fi
GLUTEN_DIR=$(realpath "$GLUTEN_DIR")

if [ -z "$VELOX_DIR" ]; then
  echo "ERROR: --velox_dir is required."
  usage; exit 1
fi
if [ ! -d "$VELOX_DIR" ]; then
  echo "ERROR: Velox directory not found: $VELOX_DIR"
  exit 1
fi
VELOX_DIR=$(realpath "$VELOX_DIR")

if [ "$EXEC_MODE" != "docker" ] && [ "$EXEC_MODE" != "direct" ]; then
  echo "ERROR: --mode must be 'docker' or 'direct', got: $EXEC_MODE"
  exit 1
fi

if [ "$BUILD_CUDF" = true ] && [ "$SKIP_VELOX" = true ]; then
  echo "ERROR: --build_cudf requires Velox to be built (do not use --skip_velox or --skip_build_native)."
  exit 1
fi

# Derive cuDF source mode.
if [ "$BUILD_CUDF" = true ]; then
  CUDF_SOURCE="BUNDLED"
else
  CUDF_SOURCE="SYSTEM"
fi

# Resolve output directory.
if [ -z "$OUTPUT_DIR" ]; then
  OUTPUT_DIR="${MODULE_DIR}/target/build_$(date +%s)"
fi
OUTPUT_DIR=$(realpath -m "$OUTPUT_DIR")
mkdir -p "$OUTPUT_DIR"

BUILD_LOG="${OUTPUT_DIR}/build.log"

# ── Determine container mode (docker only) ───────────────────────────────────
if [ "$EXEC_MODE" = "docker" ]; then
  if [ -z "${CONTAINER_NAME:-}" ]; then
    CONTAINER_MODE="fresh"
    CONTAINER_NAME="gluten_cudf_build"
  else
    CONTAINER_MODE="reuse"
  fi
fi

# ── CUDA arch prompt ─────────────────────────────────────────────────────────
NEED_CUDA=$( { [ "$SKIP_VELOX" = false ] || [ "$SKIP_GLUTEN_CPP" = false ]; } && echo true || echo false )

if [ "$NEED_CUDA" = false ] && [ "$CUDA_ARCH_EXPLICIT" = false ]; then
  CUDA_ARCH="native"
elif [ "$CUDA_ARCH_EXPLICIT" = false ]; then
  # Auto-detect GPU architecture; fall back to "native" (CMake detects at build time).
  DETECTED_CAP=""
  if type detect_cuda_architecture &>/dev/null; then
    DETECTED_CAP=$(detect_cuda_architecture 2>/dev/null || true)
  else
    DETECTED_CAP=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null \
      | head -1 | tr -d '.' || true)
  fi
  CUDA_ARCH="${DETECTED_CAP:-native}"
  echo "CUDA arch (auto-detected): ${CUDA_ARCH}"
fi

# CMake uses semicolons as list separators.
CUDA_ARCH="${CUDA_ARCH//,/;}"

# ── Banner ───────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " Gluten cuDF GPU Builder"
echo "=============================================="
echo " Gluten dir    : $GLUTEN_DIR"
echo " Velox dir     : $VELOX_DIR"
echo " CUDA arch     : $CUDA_ARCH"
echo " Exec mode     : $EXEC_MODE"
if [ "$EXEC_MODE" = "docker" ]; then
  echo " Docker image  : $PREBUILD_IMAGE"
  echo " Container     : $CONTAINER_NAME ($CONTAINER_MODE)"
fi
echo " Spark version : $SPARK_VERSION"
echo " HDFS          : $ENABLE_HDFS"
echo " S3            : $ENABLE_S3"
echo " cuDF source   : $CUDF_SOURCE"
[ -n "${MAVEN_SETTINGS:-}" ] && echo " Maven settings: $MAVEN_SETTINGS"
echo " Output dir    : $OUTPUT_DIR"
echo "----------------------------------------------"
echo " Build Arrow   : $( [ "$SKIP_ARROW" = false ] && echo "forced" || echo "auto-detect" )"
echo " Skip Velox    : $SKIP_VELOX"
echo " Skip Gluten C++ : $SKIP_GLUTEN_CPP"
echo " Rebuild Velox : $REBUILD_VELOX"
echo " Rebuild Gluten C++ : $REBUILD_GLUTEN_CPP"
echo "=============================================="
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Execution helpers
# ══════════════════════════════════════════════════════════════════════════════

# Build the environment preamble shared by both modes.
_env_preamble() {
  cat <<'PREAMBLE_STATIC'
set -eo pipefail
PREAMBLE_STATIC

  # GCC toolset activation (CentOS SCL).
  echo "${GCC_ACTIVATE}"

  cat <<PREAMBLE_DYNAMIC
export CC=\$(which gcc)
export CXX=\$(which g++)
if [ -z "\${JAVA_HOME:-}" ]; then
  for _jd in /usr/lib/jvm/java-*-openjdk* /usr/lib/jvm/java-*/; do
    [ -f "\$_jd/include/jni.h" ] && export JAVA_HOME=\$_jd && break
  done
fi
export GLUTEN_DIR='${GLUTEN_DIR_CTR}'
export VELOX_HOME='${VELOX_DIR_CTR}'
export CUDA_ARCH='${CUDA_ARCH}'
export CUDF_SOURCE='${CUDF_SOURCE}'
export NUM_THREADS=\$(nproc)
export ENABLE_HDFS='${ENABLE_HDFS}'
export ENABLE_S3='${ENABLE_S3}'
export SPARK_VERSION='${SPARK_VERSION}'
export REBUILD_VELOX='${REBUILD_VELOX}'
export REBUILD_GLUTEN_CPP='${REBUILD_GLUTEN_CPP}'
export MAVEN_SETTINGS='${MAVEN_SETTINGS_CTR}'
PREAMBLE_DYNAMIC
}

# Execute a command in the build environment.
_exec() {
  local cmd
  cmd="$(_env_preamble)"$'\n'"$1"

  if [ "$EXEC_MODE" = "docker" ]; then
    docker exec "$CONTAINER_NAME" bash -c "$cmd"
  else
    bash -c "$cmd"
  fi
}

# Read a file from the build environment.
_cat() {
  if [ "$EXEC_MODE" = "docker" ]; then
    docker exec "$CONTAINER_NAME" cat "$1" 2>/dev/null || true
  else
    cat "$1" 2>/dev/null || true
  fi
}

# Check if a file exists in the build environment.
_test_file() {
  if [ "$EXEC_MODE" = "docker" ]; then
    docker exec "$CONTAINER_NAME" test -f "$1"
  else
    test -f "$1"
  fi
}

# List files matching a glob in the build environment.
_ls() {
  if [ "$EXEC_MODE" = "docker" ]; then
    docker exec "$CONTAINER_NAME" bash -c "ls $1 2>/dev/null | head -1" || true
  else
    bash -c "ls $1 2>/dev/null | head -1" || true
  fi
}

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Start container / verify environment
# ══════════════════════════════════════════════════════════════════════════════

# Container-side paths (docker mode uses mount points, direct mode uses real paths).
if [ "$EXEC_MODE" = "docker" ]; then
  GLUTEN_DIR_CTR="/opt/gluten"
  VELOX_DIR_CTR="/opt/velox"
  SCRIPTS_DIR_CTR="/opt/builder/scripts"
  # Maven settings mounted at this path in the container (if provided).
  if [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "$MAVEN_SETTINGS" ]; then
    MAVEN_SETTINGS_CTR="/opt/maven-settings/settings.xml"
  else
    MAVEN_SETTINGS_CTR=""
  fi

  if [ "$CONTAINER_MODE" = "fresh" ]; then
    if ! docker image inspect "$PREBUILD_IMAGE" &>/dev/null; then
      echo "[1/7] Pulling image $PREBUILD_IMAGE..."
      docker pull "$PREBUILD_IMAGE"
    fi
    if docker inspect "$CONTAINER_NAME" &>/dev/null; then
      echo "[1/7] Removing stale container '$CONTAINER_NAME'..."
      docker rm -f "$CONTAINER_NAME"
    fi
    echo "[1/7] Starting fresh container..."
    DOCKER_RUN_OPTS=(
      --name "$CONTAINER_NAME"
      --gpus all
      -v "${GLUTEN_DIR}:/opt/gluten"
      -v "${VELOX_DIR}:/opt/velox"
      -v "${SCRIPT_DIR}:/opt/builder/scripts"
    )
    # Mount Maven settings if available (used by mvn -s or mvns wrapper).
    if [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "$MAVEN_SETTINGS" ]; then
      DOCKER_RUN_OPTS+=(-v "${MAVEN_SETTINGS}:/opt/maven-settings/settings.xml:ro")
    fi
    docker run "${DOCKER_RUN_OPTS[@]}" \
      -itd \
      "$PREBUILD_IMAGE" \
      /bin/bash
    echo "      Container started."
  else
    if ! docker inspect "$CONTAINER_NAME" &>/dev/null; then
      echo "ERROR: Container '$CONTAINER_NAME' not found."
      exit 1
    fi
    STATE=$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME")
    if [ "$STATE" != "running" ]; then
      echo "ERROR: Container '$CONTAINER_NAME' is not running (state: $STATE)."
      exit 1
    fi
    echo "[1/7] Reusing existing container '$CONTAINER_NAME'."
  fi
else
  # Direct mode — paths are real host paths.
  GLUTEN_DIR_CTR="$GLUTEN_DIR"
  VELOX_DIR_CTR="$VELOX_DIR"
  SCRIPTS_DIR_CTR="$SCRIPT_DIR"
  # Direct mode — MAVEN_SETTINGS path is used as-is.
  MAVEN_SETTINGS_CTR="${MAVEN_SETTINGS:-}"
  echo "[1/7] Direct mode — running in current environment."
fi

# ── Detect GCC toolset ───────────────────────────────────────────────────────
GCC_ACTIVATE=""
for ts in gcc-toolset-14 gcc-toolset-13; do
  if _test_file "/opt/rh/${ts}/enable" 2>/dev/null; then
    GCC_ACTIVATE="source /opt/rh/${ts}/enable &&"
    echo "      Activating ${ts}."
    break
  fi
done

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Verify GPU
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "[2/7] Verifying GPU access..."
if [ "$EXEC_MODE" = "docker" ]; then
  if ! docker exec "$CONTAINER_NAME" nvidia-smi --query-gpu=name,driver_version,memory.total \
      --format=csv,noheader 2>/dev/null; then
    echo "ERROR: nvidia-smi failed inside container."
    exit 1
  fi
else
  if ! nvidia-smi --query-gpu=name,driver_version,memory.total \
      --format=csv,noheader 2>/dev/null; then
    echo "WARNING: nvidia-smi failed — GPU may not be available."
  fi
fi

# ── Step 2.5: Verify cudf commit consistency ─────────────────────────────────
if [ "$CUDF_SOURCE" = "SYSTEM" ]; then
  echo ""
  echo "[2.5/7] Verifying cudf commit consistency..."

  IMAGE_CUDF_INFO=$(_cat /home/cudf-version-info)
  IMAGE_CUDF_COMMIT=$(echo "$IMAGE_CUDF_INFO" | grep '^VELOX_cudf_COMMIT=' | cut -d= -f2 || true)
  IMAGE_CUDF_VERSION=$(echo "$IMAGE_CUDF_INFO" | grep '^VELOX_cudf_VERSION=' | cut -d= -f2 || true)

  CUDF_CMAKE="${VELOX_DIR}/CMake/resolve_dependency_modules/cudf.cmake"
  REPO_CUDF_COMMIT=""
  REPO_CUDF_VERSION=""
  if [ -f "$CUDF_CMAKE" ]; then
    REPO_CUDF_COMMIT=$(grep '^set(VELOX_cudf_COMMIT ' "$CUDF_CMAKE" \
      | sed 's/set(VELOX_cudf_COMMIT \([^ )]*\).*/\1/' || true)
    REPO_CUDF_VERSION=$(grep '^set(VELOX_cudf_VERSION ' "$CUDF_CMAKE" \
      | sed 's/set(VELOX_cudf_VERSION \([^ )]*\).*/\1/' || true)
  fi

  echo "      Image  /home/cudf-version-info:"
  if [ -n "$IMAGE_CUDF_INFO" ]; then
    echo "$IMAGE_CUDF_INFO" | sed 's/^/        /'
  else
    echo "        (not found — image has no pre-built cudf)"
  fi
  echo "      Velox  ${CUDF_CMAKE##*/}:"
  if [ -n "$REPO_CUDF_COMMIT" ]; then
    echo "        VELOX_cudf_VERSION=${REPO_CUDF_VERSION}"
    echo "        VELOX_cudf_COMMIT=${REPO_CUDF_COMMIT}"
  else
    echo "        (not found)"
  fi

  VERSION_CHECK_OK=false
  VERSION_CHECK_MSG=""

  if [ -z "$IMAGE_CUDF_COMMIT" ]; then
    VERSION_CHECK_MSG="No /home/cudf-version-info — cannot verify pre-built cudf."
  elif [ -z "$REPO_CUDF_COMMIT" ]; then
    VERSION_CHECK_MSG="${CUDF_CMAKE} not found or missing VELOX_cudf_COMMIT — cannot verify."
  elif [ "$IMAGE_CUDF_COMMIT" != "$REPO_CUDF_COMMIT" ]; then
    VERSION_CHECK_MSG="cudf commit mismatch!"
  else
    VERSION_CHECK_OK=true
    echo "      => cudf commit matches."
  fi

  if [ "$VERSION_CHECK_OK" = false ]; then
    echo ""
    echo "ERROR: ${VERSION_CHECK_MSG}"
    echo ""
    echo "  Options:"
    echo "    1) Rebuild the prebuild image with the current Velox checkout"
    echo "    2) Use --build_cudf to build cudf from source (slower)"
    echo "    3) Check out the matching Velox revision"
    echo "    4) Use --ignore_version_check to skip this check (at your own risk)"
    if [ "$IGNORE_VERSION_CHECK" = true ]; then
      echo ""
      echo "  --ignore_version_check is set — continuing anyway."
    else
      exit 1
    fi
  fi
fi

# ── Build log setup ──────────────────────────────────────────────────────────
BUILD_LOG_CTR="${GLUTEN_DIR_CTR}/package/build.log"
if [ "$EXEC_MODE" = "docker" ]; then
  docker exec "$CONTAINER_NAME" mkdir -p "$(dirname "$BUILD_LOG_CTR")"
else
  mkdir -p "$(dirname "$BUILD_LOG_CTR")"
fi
: > "$BUILD_LOG"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Build Arrow
# ══════════════════════════════════════════════════════════════════════════════
echo ""

if [ "$SKIP_ARROW" = "auto" ]; then
  ARROW_CPP_OK=$(_ls '/usr/local/lib64/libarrow.a /usr/local/lib/libarrow.a')
  ARROW_JAVA_OK=$(_ls '/root/.m2/repository/org/apache/arrow/arrow-vector/*/arrow-vector-*.jar')

  if [ -n "$ARROW_CPP_OK" ] && [ -n "$ARROW_JAVA_OK" ]; then
    SKIP_ARROW=true
    echo "[3/7] Arrow pre-installed — skipping build."
    echo "      C++:  $ARROW_CPP_OK"
    echo "      Java: $ARROW_JAVA_OK"
  else
    SKIP_ARROW=false
    echo "[3/7] Arrow not found — will build from source."
    [ -z "$ARROW_CPP_OK" ]  && echo "      Missing: libarrow.a"
    [ -z "$ARROW_JAVA_OK" ] && echo "      Missing: arrow-vector JAR in ~/.m2"
  fi
fi

if [ "$SKIP_ARROW" = false ]; then
  echo "[3/7] Building Arrow..."
  if ! _exec "source ${SCRIPTS_DIR_CTR}/build-arrow.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
    echo ""; echo "ERROR: Arrow build failed. See build.log for details."; exit 1
  fi
else
  [ "$SKIP_ARROW" = true ] && echo "[3/7] Skipped (Arrow pre-installed)."
fi

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Build Velox
# ══════════════════════════════════════════════════════════════════════════════
echo ""
if [ "$SKIP_VELOX" = false ]; then
  echo "[4/7] Building Velox..."
  if ! _exec "bash ${SCRIPTS_DIR_CTR}/build-velox.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
    echo ""; echo "ERROR: Velox build failed. See build.log for details."; exit 1
  fi
else
  echo "[4/7] Skipped (--skip_velox)."
fi

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Build Gluten C++
# ══════════════════════════════════════════════════════════════════════════════
echo ""
if [ "$SKIP_GLUTEN_CPP" = false ]; then
  echo "[5/7] Building Gluten C++..."
  if ! _exec "bash ${SCRIPTS_DIR_CTR}/build-gluten-cpp.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
    echo ""; echo "ERROR: Gluten C++ build failed. See build.log for details."; exit 1
  fi
else
  echo "[5/7] Skipped (--skip_gluten_cpp)."
fi

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Build Gluten Maven
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "[6/7] Building Gluten Maven (Spark ${SPARK_VERSION})..."
if ! _exec "bash ${SCRIPTS_DIR_CTR}/build-gluten-jvm.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
  echo ""; echo "ERROR: Gluten Maven build failed. See build.log for details."; exit 1
fi

# ══════════════════════════════════════════════════════════════════════════════
# Step 7: Collect deploy libraries
# ══════════════════════════════════════════════════════════════════════════════
echo ""
if [ "$EXEC_MODE" = "docker" ]; then
  DEPLOY_DIR_CTR="/opt/output"
  docker exec "$CONTAINER_NAME" mkdir -p "$DEPLOY_DIR_CTR"
  echo "[7/7] Collecting shared libraries..."
  if ! _exec "export DEPLOY_DIR='${DEPLOY_DIR_CTR}' && bash ${SCRIPTS_DIR_CTR}/collect-deploy-libs.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
    echo ""; echo "ERROR: collect-deploy-libs failed. See build.log for details."; exit 1
  fi
  echo "      Copying artifacts to ${OUTPUT_DIR}..."
  docker cp "${CONTAINER_NAME}:${DEPLOY_DIR_CTR}/." "${OUTPUT_DIR}/"
else
  echo "[7/7] Collecting shared libraries..."
  if ! _exec "export DEPLOY_DIR='${OUTPUT_DIR}' && bash ${SCRIPTS_DIR_CTR}/collect-deploy-libs.sh 2>&1 | tee -a ${BUILD_LOG_CTR}"; then
    echo ""; echo "ERROR: collect-deploy-libs failed. See build.log for details."; exit 1
  fi
fi

# ── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " BUILD COMPLETE"
echo "=============================================="
echo " Build log: $BUILD_LOG"
echo " Deploy directory: $OUTPUT_DIR"
echo ""
echo " Usage:"
echo "   export GPU_LIBS=$OUTPUT_DIR/libs"
echo "   export LD_LIBRARY_PATH=\$GPU_LIBS:\${LD_LIBRARY_PATH:-}"
echo "   spark-submit \\"
echo "     --jars $OUTPUT_DIR/gluten-velox-bundle-*.jar \\"
echo "     --conf spark.executor.extraLibraryPath=\$GPU_LIBS \\"
echo "     --conf spark.driver.extraLibraryPath=\$GPU_LIBS \\"
echo "     --conf spark.plugins=org.apache.gluten.GlutenPlugin \\"
echo "     --conf spark.gluten.sql.columnar.cudf=true \\"
echo "     ..."
echo "=============================================="
