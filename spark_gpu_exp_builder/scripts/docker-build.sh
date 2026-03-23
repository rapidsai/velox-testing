#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Docker image builder for prebuild and runtime images.
#
# Two modes:
#   Print mode (default): prints docker build commands to stdout
#   Run mode (--run):     executes the build directly
#
# Target is inferred from flags:
#   --run_prebuild only                    → build prebuild image
#   --prebuild_image + --build_output=TAG  → build runtime image (prebuild must exist)
#   --target=all     + --build_output=TAG  → build prebuild then runtime image
#
# Reads GLUTEN_DIR, VELOX_DIR, MVN_SET from environment. In interactive mode,
# shows detected env vars and asks for confirmation.
#
# Usage:
#   # Prebuild only:
#   ggbuild build --mode=run --target=prebuild --prebuild_image=gluten:prebuild
#
#   # Full pipeline (prebuild → runtime image):
#   ggbuild build --mode=run --target=all \
#     --prebuild_image=gluten:prebuild --build_output=gluten:runtime
#
#   # Build libs directly (no runtime image):
#   ggbuild build --mode=run --target=runtime \
#     --prebuild_image=gluten:prebuild --build_output=/tmp/my-libs

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROG_NAME="${GGBUILD_CMD:-$(basename "$0")}"

# Source config definitions (single source of truth for defaults & mappings).
# shellcheck source=config_def.sh
source "${SCRIPT_DIR}/config_def.sh"

# Resolve CONFIG_FILE from CFG_FILE env alias (needed before arg parsing).
if [ -z "${CONFIG_FILE:-}" ] && [ -n "${CFG_FILE:-}" ]; then
  CONFIG_FILE="$CFG_FILE"
fi

# Array flags (not in config_def table).
EXTRA_ARGS=()

# ── Argument parsing ─────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: ${PROG_NAME} [options]

Generate and optionally execute 'docker build' command(s).
Reads GLUTEN_DIR, VELOX_DIR, MVN_SET from environment if set.

Required:
  --prebuild_image=TAG        Prebuild image tag              [env: BUILD_IMG]

Target control:
  --target=prebuild|runtime|all  What to build (default: runtime)
                              prebuild = prebuild docker image only
                              runtime  = see --build_output (docker image or libs)
                              all      = prebuild + runtime/libs
  --build_output=TAG|PATH     Output for runtime/all target:
                                docker tag (e.g. gluten:runtime) → build runtime image
                                path      (e.g. /tmp/libs)       → build libs to that path
                              If unset, libs are written to target/libs_<epoch>

Essential (prompted if missing, reads env vars):
  --gluten_dir=PATH           Gluten source tree              [env: GLUTEN_DIR]
  --velox_dir=PATH            Velox source tree                [env: VELOX_DIR]

Options:
  --config=PATH               XML config file                          [env: CFG_FILE]
  --maven_settings=PATH       Custom Maven settings.xml        [env: MVN_SET]
  --base_image=IMAGE          Base image for prebuild
                              (default: apache/gluten:centos-9-jdk8-cudf)
  --spark_version=VER         Spark major.minor (default: 3.5)
  --spark_full_version=VER    Full Spark version (default: 3.5.5)
  --arrow_version=VER         Arrow version (default: 15.0.0)
  --cuda_arch=ARCH            CUDA architectures (default: prompted)
  --enable_hdfs=ON|OFF        HDFS connector (default: ON)
  --enable_s3=ON|OFF          S3 connector (default: OFF)
  --container=NAME            Reuse existing running container (libs direct mode)
  --run_mode=direct|docker    Libs execution mode (default: direct)
                              direct = spin up PREBUILD_IMAGE via docker run/exec
                              docker = run builder.sh directly (already in container)
  --no_cache                  Docker --no-cache
  --mode=run|print            run   = execute the build (default)
                              print = show docker build commands
  --log_file=PATH             Build log file (implies --mode=run)
  --extra=ARG                 Extra docker build arg (repeatable)
  -h, --help                  Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --gluten_dir=*)          GLUTEN_DIR="${arg#*=}"; _GLUTEN_DIR_FROM_ENV="" ;;
    --velox_dir=*)           VELOX_DIR="${arg#*=}"; _VELOX_DIR_FROM_ENV="" ;;
    --config=*)              CONFIG_FILE="${arg#*=}" ;;
    --prebuild_image=*)      PREBUILD_IMAGE="${arg#*=}"; _PREBUILD_IMAGE_FROM_ENV="" ;;
    --build_output=*)        BUILD_OUTPUT="${arg#*=}" ;;
    --target=*)              BUILD_TARGET="${arg#*=}" ;;
    --run_mode=*)            LIBS_RUN_MODE="${arg#*=}" ;;
    --container=*)           CONTAINER_NAME="${arg#*=}" ;;
    --base_image=*)          PREBUILD_BASE_IMAGE="${arg#*=}" ;;
    --spark_version=*)       SPARK_VERSION="${arg#*=}" ;;
    --spark_full_version=*)  SPARK_FULL_VERSION="${arg#*=}" ;;
    --arrow_version=*)       ARROW_VERSION="${arg#*=}" ;;
    --cuda_arch=*)           CUDA_ARCH="${arg#*=}" ;;
    --enable_hdfs=*)         ENABLE_HDFS="${arg#*=}" ;;
    --enable_s3=*)           ENABLE_S3="${arg#*=}" ;;
    --maven_settings=*)      MAVEN_SETTINGS="${arg#*=}"; _MAVEN_SETTINGS_FROM_ENV="" ;;
    --no_cache)              NO_CACHE=true ;;
    --mode=*)                DOCKER_BUILD_MODE="${arg#*=}" ;;
    --run)                   DOCKER_BUILD_MODE=run ;;
    --log_file=*)            LOG_FILE="${arg#*=}"; DOCKER_BUILD_MODE=run ;;
    --extra=*)               EXTRA_ARGS+=("${arg#*=}") ;;
    -h|--help)               usage; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; usage >&2; exit 1 ;;
  esac
done

# ── Config mode: file vs env ──────────────────────────────────────────────────
# When a config file is present, it is the sole source of truth — env aliases
# are ignored. Without a config file, env aliases are applied for interactive use.
_INTERACTIVE=true
if [ -n "${CONFIG_FILE:-}" ] && [ -f "${CONFIG_FILE}" ]; then
  echo "  Config file: ${CONFIG_FILE}" >&2
  eval "$(python3 "${SCRIPT_DIR}/parse-config.py" read "$CONFIG_FILE")"
  _INTERACTIVE=false
else
  # No config file — apply env aliases (GLUTEN_DIR, BUILD_IMG, MVN_SET, etc.).
  apply_env_aliases
  [ "${_GLUTEN_DIR_FROM_ENV:-}" = "env" ]       && echo "  GLUTEN_DIR from env: ${GLUTEN_DIR}" >&2
  [ "${_VELOX_DIR_FROM_ENV:-}" = "env" ]        && echo "  VELOX_DIR from env: ${VELOX_DIR}" >&2
  [ "${_PREBUILD_IMAGE_FROM_ENV:-}" = "env" ]   && echo "  PREBUILD_IMAGE from env (BUILD_IMG): ${PREBUILD_IMAGE}" >&2
  [ "${_MAVEN_SETTINGS_FROM_ENV:-}" = "env" ]   && echo "  MAVEN_SETTINGS from env (MVN_SET): ${MAVEN_SETTINGS}" >&2
fi

# ── Resolve BUILD_OUTPUT ─────────────────────────────────────────────────────
# BUILD_OUTPUT is a unified field: a filesystem path means "build libs here";
# anything else is treated as a docker image tag for the runtime image.
RUNTIME_IMAGE=""
if [[ "${BUILD_OUTPUT:-}" == /* || "${BUILD_OUTPUT:-}" == ./* || "${BUILD_OUTPUT:-}" == ../* ]]; then
  LIBS_OUTPUT_DIR="$BUILD_OUTPUT"
elif [ -n "${BUILD_OUTPUT:-}" ]; then
  RUNTIME_IMAGE="$BUILD_OUTPUT"
fi

# ── Interactive helpers ──────────────────────────────────────────────────────
_HAS_TTY=false
if (echo '' >/dev/null </dev/tty) 2>/dev/null; then
  _HAS_TTY=true
fi

_REPLY=""
_read_input() {
  local prompt="$1"
  printf '%s' "$prompt" >&2
  _REPLY=""
  if [ "$_HAS_TTY" = true ]; then
    read -r _REPLY </dev/tty
  else
    read -r _REPLY
  fi
}

# Prompt for a variable.
# - If set by CLI: use as-is, no confirmation.
# - If set by env var: show value, ask to confirm interactively.
# - If not set: prompt for value.
# Args: VAR_NAME "description" "ENV_VAR_NAME" "from_env_flag_var"
_ask() {
  local var_name="$1" prompt="$2" env_hint="${3:-}" from_env_var="${4:-}"
  local current_val="${!var_name:-}"
  local from_env="${!from_env_var:-}"

  if [ -n "$current_val" ]; then
    if [ "$from_env" = "env" ]; then
      # Set from env var — confirm interactively.
      echo "  ${prompt} (from ${env_hint}): ${current_val}" >&2
      _read_input "  Confirm? [Y/n]: "
      case "${_REPLY:-y}" in
        [nN]*)
          _read_input "  New value: "
          eval "$var_name=\"\$_REPLY\""
          ;;
      esac
    fi
    # Set by CLI or confirmed — done.
    return
  fi

  # Not set — prompt.
  while [ -z "${!var_name:-}" ]; do
    local hint=""
    [ -n "$env_hint" ] && hint=" [env: ${env_hint}]"
    _read_input "${prompt}${hint}: "
    eval "$var_name=\"\$_REPLY\""
    [ -z "${!var_name:-}" ] && echo "  (required)" >&2
  done || true
}

# ── Interactive prompts ───────────────────────────────────────────────────────
if [ "$_INTERACTIVE" = true ]; then
  # Prompt for build target if not set.
  if [ -z "${BUILD_TARGET:-}" ]; then
    echo "  Build target [prebuild/runtime/all] (default: runtime):" >&2
    _read_input "  Target: "
    BUILD_TARGET="${_REPLY:-runtime}"
  fi

  # Prebuild image: required for prebuild/all targets, and for runtime target
  # when LIBS_RUN_MODE=direct (image is used as the build container).
  _needs_prebuild_image() {
    [[ "$BUILD_TARGET" == "prebuild" || "$BUILD_TARGET" == "all" ]] && return 0
    [[ "$BUILD_TARGET" == "runtime" ]] && [ -z "$RUNTIME_IMAGE" ] \
      && [ "${LIBS_RUN_MODE:-direct}" = "direct" ] && return 0
    return 1
  }
  if _needs_prebuild_image; then
    if [ -n "${PREBUILD_IMAGE:-}" ] && [ "${_PREBUILD_IMAGE_FROM_ENV:-}" = "env" ]; then
      echo "  Prebuild image (from BUILD_IMG): ${PREBUILD_IMAGE}" >&2
      _read_input "  Confirm? [Y/n]: "
      case "${_REPLY:-y}" in
        [nN]*) _read_input "  New prebuild image tag: "; PREBUILD_IMAGE="$_REPLY" ;;
      esac
    elif [ -z "${PREBUILD_IMAGE:-}" ]; then
      while [ -z "${PREBUILD_IMAGE:-}" ]; do
        _read_input "Prebuild image tag (required) [env: BUILD_IMG]: "
        PREBUILD_IMAGE="$_REPLY"
        [ -z "${PREBUILD_IMAGE:-}" ] && echo "  (required)" >&2
      done || true
    fi
  fi

  # BUILD_OUTPUT: optional for runtime/all — docker tag or path; omit to build libs to default dir.
  if [[ "$BUILD_TARGET" == "runtime" || "$BUILD_TARGET" == "all" ]]; then
    if [ -z "${BUILD_OUTPUT:-}" ]; then
      _read_input "Build output (docker tag or /path/to/libs, Enter for default libs dir): "
      BUILD_OUTPUT="$_REPLY"
      # Re-resolve after interactive input.
      if [[ "$BUILD_OUTPUT" == /* || "$BUILD_OUTPUT" == ./* || "$BUILD_OUTPUT" == ../* ]]; then
        LIBS_OUTPUT_DIR="$BUILD_OUTPUT"
      elif [ -n "$BUILD_OUTPUT" ]; then
        RUNTIME_IMAGE="$BUILD_OUTPUT"
      fi
    fi
  fi
fi

# ── Infer target ─────────────────────────────────────────────────────────────
DO_PREBUILD=false
DO_RUNTIME=false
DO_LIBS=false

case "${BUILD_TARGET:-runtime}" in
  prebuild)
    DO_PREBUILD=true
    ;;
  runtime)
    if [ -n "${RUNTIME_IMAGE:-}" ]; then
      DO_RUNTIME=true
    else
      DO_LIBS=true
    fi
    ;;
  all)
    DO_PREBUILD=true
    if [ -n "${RUNTIME_IMAGE:-}" ]; then
      DO_RUNTIME=true
    else
      DO_LIBS=true
    fi
    ;;
  *)
    echo "ERROR: --target must be prebuild|runtime|all, got: ${BUILD_TARGET}" >&2
    exit 1
    ;;
esac

echo "" >&2
if   [ "$DO_PREBUILD" = true ] && [ "$DO_RUNTIME" = true ]; then echo "── Build: prebuild → runtime ──" >&2
elif [ "$DO_PREBUILD" = true ] && [ "$DO_LIBS" = true ];    then echo "── Build: prebuild + libs (${LIBS_RUN_MODE:-direct}) ──" >&2
elif [ "$DO_PREBUILD" = true ]; then echo "── Build: prebuild only ──" >&2
elif [ "$DO_RUNTIME"  = true ]; then echo "── Build: runtime only ──" >&2
else                                 echo "── Build: libs (${LIBS_RUN_MODE:-direct}) ──" >&2
fi
[ "$DO_PREBUILD" = true ] || [ "$DO_RUNTIME" = true ] && echo "  Prebuild: ${PREBUILD_IMAGE:-}" >&2
[ "$DO_RUNTIME"  = true ] && echo "  Runtime:  $RUNTIME_IMAGE" >&2
[ "$DO_LIBS"     = true ] && echo "  Libs run_mode: ${LIBS_RUN_MODE:-direct}" >&2
echo "" >&2

# ── Validate PREBUILD_IMAGE ───────────────────────────────────────────────────
# Required when: building prebuild/all image targets, or any libs build
# (direct mode uses it for docker run; docker mode passes it to builder.sh --image).
if [ "$DO_PREBUILD" = true ] || [ "$DO_LIBS" = true ]; then
  if [ -z "${PREBUILD_IMAGE:-}" ]; then
    echo "ERROR: PREBUILD_IMAGE is required for target '${BUILD_TARGET}'." \
         "Set --prebuild_image or BUILD_IMG env." >&2
    exit 1
  fi
fi

# ── Prompt for essential arguments ───────────────────────────────────────────
if [ "$_INTERACTIVE" = true ]; then
  _ask GLUTEN_DIR "Gluten source directory" "GLUTEN_DIR" "_GLUTEN_DIR_FROM_ENV"
  _ask VELOX_DIR  "Velox source directory"  "VELOX_DIR"  "_VELOX_DIR_FROM_ENV"

  # Maven settings — confirm if from env.
  if [ -n "${MAVEN_SETTINGS:-}" ] && [ "${_MAVEN_SETTINGS_FROM_ENV:-}" = "env" ]; then
    echo "  Maven settings (from MVN_SET): ${MAVEN_SETTINGS}" >&2
    _read_input "  Confirm? [Y/n]: "
    case "${_REPLY:-y}" in
      [nN]*)
        _read_input "  New Maven settings path: "
        MAVEN_SETTINGS="$_REPLY"
        ;;
    esac
  fi
fi

# Validate required paths.
if [ -z "${GLUTEN_DIR:-}" ]; then
  echo "ERROR: GLUTEN_DIR is required. Set --gluten_dir, GLUTEN_DIR env, or configure in the XML." >&2
  exit 1
fi
if [ -z "${VELOX_DIR:-}" ]; then
  echo "ERROR: VELOX_DIR is required. Set --velox_dir, VELOX_DIR env, or configure in the XML." >&2
  exit 1
fi

# Resolve to absolute paths.
[ -d "$GLUTEN_DIR" ] && GLUTEN_DIR=$(realpath "$GLUTEN_DIR") \
  || echo "WARNING: Gluten directory not found: $GLUTEN_DIR (using as-is)" >&2
[ -d "$VELOX_DIR" ] && VELOX_DIR=$(realpath "$VELOX_DIR") \
  || echo "WARNING: Velox directory not found: $VELOX_DIR (using as-is)" >&2

# ── Prompt for CUDA arch ─────────────────────────────────────────────────────
if [ -z "${CUDA_ARCH:-}" ]; then
  if [ "$_INTERACTIVE" = true ]; then
    DETECTED_CAP=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null \
      | head -1 | tr -d '.' || true)
    DETECTED_GPU=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 || true)

    echo "" >&2
    echo "Select CUDA target architecture:" >&2
    echo "  1) all-major  — 75;80;86;89;90 (portable) [default]" >&2
    [ -n "$DETECTED_CAP" ] && echo "  2) ${DETECTED_CAP}        — detected ${DETECTED_GPU}" >&2
    echo "  *) custom     — enter semicolon-separated list" >&2
    echo "" >&2
    _read_input "Choice [1]: "
    arch_choice="${_REPLY:-1}"
    case "$arch_choice" in
      1|"")  CUDA_ARCH="75;80;86;89;90" ;;
      2)     CUDA_ARCH="${DETECTED_CAP:-75;80;86;89;90}" ;;
      *)
        if [[ "$arch_choice" =~ ^[0-9]+([\;,][0-9]+)*$ ]]; then
          CUDA_ARCH="${arch_choice//,/;}"
        else
          _read_input "Enter CUDA arch (e.g. 80;86): "
          CUDA_ARCH="$_REPLY"
          CUDA_ARCH="${CUDA_ARCH//,/;}"
        fi
        ;;
    esac
    echo "  Using CUDA arch: ${CUDA_ARCH}" >&2
  else
    if [ "$DO_LIBS" = true ]; then
      # Libs build: leave CUDA_ARCH unset — builder.sh will detect the local GPU.
      true
    else
      # Non-interactive image build fallback: use portable all-major arch set.
      CUDA_ARCH="75;80;86;89;90"
      echo "  CUDA_ARCH not set — using default: ${CUDA_ARCH}" >&2
    fi
  fi
fi

# ── Apply remaining defaults ─────────────────────────────────────────────────
apply_defaults

# ══════════════════════════════════════════════════════════════════════════════
# Command generation (print mode)
# ══════════════════════════════════════════════════════════════════════════════

_print_cmd() {
  local -a cmd=("$@")
  echo "${cmd[0]} ${cmd[1]} \\"
  for ((i=2; i<${#cmd[@]}-1; i++)); do
    echo "  ${cmd[$i]} \\"
  done
  echo "  ${cmd[${#cmd[@]}-1]}"
}

_gen_prebuild() {
  local -a cmd=("docker" "build")
  [ "$NO_CACHE" = true ] && cmd+=("--no-cache")
  cmd+=("--progress=plain")
  cmd+=("--build-context gluten=${GLUTEN_DIR}")
  cmd+=("--build-context velox=${VELOX_DIR}")
  cmd+=("--build-arg BASE_IMAGE=${PREBUILD_BASE_IMAGE}")
  cmd+=("--build-arg SPARK_VERSION=${SPARK_VERSION}")
  cmd+=("--build-arg ARROW_VERSION=${ARROW_VERSION}")
  cmd+=("--build-arg CUDA_ARCH=\"${CUDA_ARCH}\"")
  cmd+=("-t ${PREBUILD_IMAGE}")
  cmd+=("-f ${MODULE_DIR}/docker/prebuild.dockerfile")
  for extra in "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"; do
    cmd+=("$extra")
  done
  cmd+=("${MODULE_DIR}")
  _print_cmd "${cmd[@]}"
}

_gen_runtime() {
  local -a cmd=("docker" "build")
  [ "$NO_CACHE" = true ] && cmd+=("--no-cache")
  cmd+=("--progress=plain")
  cmd+=("--build-context gluten=${GLUTEN_DIR}")
  cmd+=("--build-context velox=${VELOX_DIR}")
  cmd+=("--build-arg PREBUILD_IMAGE=${PREBUILD_IMAGE}")
  cmd+=("--build-arg CUDA_ARCH=\"${CUDA_ARCH}\"")
  cmd+=("--build-arg SPARK_VERSION=${SPARK_VERSION}")
  cmd+=("--build-arg SPARK_FULL_VERSION=${SPARK_FULL_VERSION}")
  cmd+=("--build-arg ENABLE_HDFS=${ENABLE_HDFS}")
  cmd+=("--build-arg ENABLE_S3=${ENABLE_S3}")
  cmd+=("-t ${RUNTIME_IMAGE}")
  cmd+=("-f ${MODULE_DIR}/docker/runtime.dockerfile")
  for extra in "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"; do
    cmd+=("$extra")
  done
  cmd+=("${MODULE_DIR}")
  _print_cmd "${cmd[@]}"
}

# ── Maven staging ────────────────────────────────────────────────────────────
_stage_maven() {
  MVN_STAGE_DIR="${MODULE_DIR}/.docker-maven-settings"
  mkdir -p "$MVN_STAGE_DIR"
  _MVN_STAGED=false
  if [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "$MAVEN_SETTINGS" ]; then
    cp "$MAVEN_SETTINGS" "$MVN_STAGE_DIR/settings.xml"
    _MVN_STAGED=true
    echo "Staged Maven settings: $MAVEN_SETTINGS" >&2
  fi
}
_unstage_maven() {
  if [ "${_MVN_STAGED:-false}" = true ]; then
    rm -f "${MVN_STAGE_DIR}/settings.xml"
  fi
}
_maven_note() {
  if [ -n "${MAVEN_SETTINGS:-}" ]; then
    echo "# Stage Maven settings before running:"
    echo "cp ${MAVEN_SETTINGS} ${MODULE_DIR}/.docker-maven-settings/settings.xml"
    echo ""
  fi
}

# ── Libs output dir ──────────────────────────────────────────────────────────
# Resolved once here so both print and exec modes use the same value.
if [ "$DO_LIBS" = true ]; then
  LIBS_OUTPUT_DIR="${LIBS_OUTPUT_DIR:-${OUTPUT_DIR:-${MODULE_DIR}/target/libs_$(date +%s)}}"
  LIBS_OUTPUT_DIR=$(realpath -m "$LIBS_OUTPUT_DIR")
  mkdir -p "$LIBS_OUTPUT_DIR"
fi

# ── Dump resolved config to output dir ───────────────────────────────────────
_CFG_DUMP_DIR="${LIBS_OUTPUT_DIR:-${MODULE_DIR}/target}"
mkdir -p "$_CFG_DUMP_DIR"
export GLUTEN_DIR VELOX_DIR PREBUILD_IMAGE BUILD_OUTPUT PREBUILD_BASE_IMAGE
export SPARK_VERSION SPARK_FULL_VERSION ARROW_VERSION
export ENABLE_HDFS ENABLE_S3 MAVEN_SETTINGS
[ -n "${CUDA_ARCH:-}" ] && export CUDA_ARCH
python3 "${SCRIPT_DIR}/parse-config.py" write "${_CFG_DUMP_DIR}/resolved_config.xml" --env
echo "Resolved config: ${_CFG_DUMP_DIR}/resolved_config.xml" >&2

# ── Libs helpers ─────────────────────────────────────────────────────────────
# Build the builder.sh arg list.
# $1 = mode (direct|docker), $2 = gluten path, $3 = velox path, $4 = output path.
_builder_args() {
  local mode="$1" gluten="$2" velox="$3" output="$4"
  local args=(
    "--mode=${mode}"
    "--gluten_dir=${gluten}"
    "--velox_dir=${velox}"
    "--output_dir=${output}"
  )
  [ "$mode" = "docker" ] && [ -n "${PREBUILD_IMAGE:-}" ] && args+=(--image="$PREBUILD_IMAGE")
  [ "$mode" = "docker" ] && [ -n "${CONTAINER_NAME:-}" ] && args+=(--container="$CONTAINER_NAME")
  [ -n "${CUDA_ARCH:-}" ]                       && args+=(--cuda_arch="$CUDA_ARCH")
  [ -n "${SPARK_VERSION:-}" ]                   && args+=(--spark_version="$SPARK_VERSION")
  [ -n "${ENABLE_HDFS:-}" ]                     && args+=(--enable_hdfs="$ENABLE_HDFS")
  [ -n "${ENABLE_S3:-}" ]                       && args+=(--enable_s3="$ENABLE_S3")
  [ "${SKIP_VELOX:-false}"           = true ]   && args+=(--skip_velox)
  [ "${SKIP_GLUTEN_CPP:-false}"      = true ]   && args+=(--skip_build_native)
  [ "${BUILD_CUDF:-false}"           = true ]   && args+=(--build_cudf)
  [ "${REBUILD_VELOX:-false}"        = true ]   && args+=(--rebuild_velox)
  [ "${REBUILD_GLUTEN_CPP:-false}"   = true ]   && args+=(--rebuild_gluten_cpp)
  [ "${IGNORE_VERSION_CHECK:-false}" = true ]   && args+=(--ignore_version_check)
  printf '%s\n' "${args[@]}"
}

_gen_libs() {
  if [ "$LIBS_RUN_MODE" = "direct" ]; then
    # direct: docker-build.sh creates the docker run/exec; builder.sh runs --mode=direct inside.
    if [ -n "${CONTAINER_NAME:-}" ]; then
      echo "docker exec ${CONTAINER_NAME} \\"
      echo "  bash /opt/spark_experimental/scripts/builder.sh \\"
      while IFS= read -r a; do echo "    ${a} \\"; done \
        < <(_builder_args direct /opt/gluten /opt/velox /opt/output)
    else
      echo "docker run --rm --gpus all \\"
      echo "  -v ${GLUTEN_DIR}:/opt/gluten \\"
      echo "  -v ${VELOX_DIR}:/opt/velox \\"
      echo "  -v ${SCRIPT_DIR}:/opt/spark_experimental/scripts \\"
      echo "  -v ${LIBS_OUTPUT_DIR}:/opt/output \\"
      [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "$MAVEN_SETTINGS" ] && \
        echo "  -v ${MAVEN_SETTINGS}:/opt/maven-settings/settings.xml:ro \\"
      echo "  ${PREBUILD_IMAGE} \\"
      echo "  bash /opt/spark_experimental/scripts/builder.sh \\"
      while IFS= read -r a; do echo "    ${a} \\"; done \
        < <(_builder_args direct /opt/gluten /opt/velox /opt/output)
    fi
  else
    # docker: builder.sh manages its own container via --mode=docker.
    echo "bash ${SCRIPT_DIR}/builder.sh \\"
    while IFS= read -r a; do echo "  ${a} \\"; done \
      < <(_builder_args docker "$GLUTEN_DIR" "$VELOX_DIR" "$LIBS_OUTPUT_DIR")
  fi
  echo ""
  echo "# Output: ${LIBS_OUTPUT_DIR}"
}

# ── Dockerignore setup ───────────────────────────────────────────────────────
_setup_dockerignore() {
  local src="${MODULE_DIR}/docker/gluten.dockerignore"
  local dst="${GLUTEN_DIR}/.dockerignore"
  _DOCKERIGNORE_CLEANUP=false
  if [ -f "$src" ] && [ ! -e "$dst" ]; then
    cp "$src" "$dst"
    _DOCKERIGNORE_CLEANUP=true
  fi
}
_cleanup_dockerignore() {
  if [ "${_DOCKERIGNORE_CLEANUP:-false}" = true ]; then
    rm -f "${GLUTEN_DIR}/.dockerignore"
  fi
}

# ══════════════════════════════════════════════════════════════════════════════
# Output / execution
# ══════════════════════════════════════════════════════════════════════════════

if [ "$DOCKER_BUILD_MODE" = "run" ]; then
  # ── Execute mode ──────────────────────────────────────────────────────
  _stage_maven
  _setup_dockerignore
  trap '_unstage_maven; _cleanup_dockerignore' EXIT

  if [ -n "${LOG_FILE:-}" ]; then
    LOG_FILE=$(realpath -m "$LOG_FILE")
    mkdir -p "$(dirname "$LOG_FILE")"
    : > "$LOG_FILE"
    _run() { "$@" 2>&1 | tee -a "$LOG_FILE"; return "${PIPESTATUS[0]}"; }
  else
    _run() { "$@"; }
  fi

  _exec_prebuild() {
    echo "" >&2
    echo "── Building prebuild → ${PREBUILD_IMAGE} ──" >&2
    _run docker build \
      $( [ "$NO_CACHE" = true ] && echo "--no-cache" ) \
      --progress=plain \
      --build-context "gluten=${GLUTEN_DIR}" \
      --build-context "velox=${VELOX_DIR}" \
      --build-arg "BASE_IMAGE=${PREBUILD_BASE_IMAGE}" \
      --build-arg "SPARK_VERSION=${SPARK_VERSION}" \
      --build-arg "ARROW_VERSION=${ARROW_VERSION}" \
      --build-arg "CUDA_ARCH=${CUDA_ARCH}" \
      "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" \
      -t "${PREBUILD_IMAGE}" \
      -f "${MODULE_DIR}/docker/prebuild.dockerfile" \
      "${MODULE_DIR}"
  }

  _exec_runtime() {
    echo "" >&2
    echo "── Building runtime → ${RUNTIME_IMAGE} ──" >&2
    _run docker build \
      $( [ "$NO_CACHE" = true ] && echo "--no-cache" ) \
      --progress=plain \
      --build-context "gluten=${GLUTEN_DIR}" \
      --build-context "velox=${VELOX_DIR}" \
      --build-arg "PREBUILD_IMAGE=${PREBUILD_IMAGE}" \
      --build-arg "CUDA_ARCH=${CUDA_ARCH}" \
      --build-arg "SPARK_VERSION=${SPARK_VERSION}" \
      --build-arg "SPARK_FULL_VERSION=${SPARK_FULL_VERSION}" \
      --build-arg "ENABLE_HDFS=${ENABLE_HDFS}" \
      --build-arg "ENABLE_S3=${ENABLE_S3}" \
      "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" \
      -t "${RUNTIME_IMAGE}" \
      -f "${MODULE_DIR}/docker/runtime.dockerfile" \
      "${MODULE_DIR}"
  }

  _exec_libs() {
    echo "" >&2
    echo "── Building libs (${LIBS_RUN_MODE} mode) → ${LIBS_OUTPUT_DIR} ──" >&2
    if [ "$LIBS_RUN_MODE" = "direct" ]; then
      # direct: docker-build.sh creates the docker run/exec; builder.sh runs --mode=direct inside.
      if [ -n "${CONTAINER_NAME:-}" ]; then
        _run docker exec "$CONTAINER_NAME" \
          bash /opt/spark_experimental/scripts/builder.sh \
          $(_builder_args direct /opt/gluten /opt/velox /opt/output)
      else
        local docker_opts=(
          --rm --gpus all
          -v "${GLUTEN_DIR}:/opt/gluten"
          -v "${VELOX_DIR}:/opt/velox"
          -v "${SCRIPT_DIR}:/opt/spark_experimental/scripts"
          -v "${LIBS_OUTPUT_DIR}:/opt/output"
        )
        [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "$MAVEN_SETTINGS" ] && \
          docker_opts+=(-v "${MAVEN_SETTINGS}:/opt/maven-settings/settings.xml:ro")
        _run docker run "${docker_opts[@]}" "$PREBUILD_IMAGE" \
          bash /opt/spark_experimental/scripts/builder.sh \
          $(_builder_args direct /opt/gluten /opt/velox /opt/output)
      fi
    else
      # docker: builder.sh manages its own container via --mode=docker.
      _run bash "$SCRIPT_DIR/builder.sh" \
        $(_builder_args docker "$GLUTEN_DIR" "$VELOX_DIR" "$LIBS_OUTPUT_DIR")
    fi
    echo "" >&2
    echo "── Libs ready: ${LIBS_OUTPUT_DIR} ──" >&2
  }

  [ "$DO_PREBUILD" = true ] && _exec_prebuild
  [ "$DO_RUNTIME"  = true ] && _exec_runtime
  [ "$DO_LIBS"     = true ] && _exec_libs

  echo "" >&2
  echo "── BUILD COMPLETE ──" >&2
  [ -n "${LOG_FILE:-}" ] && echo "Log: $LOG_FILE" >&2

else
  # ── Print mode ────────────────────────────────────────────────────────
  echo "" >&2
  _maven_note

  if [ "$DO_LIBS" = true ]; then
    echo "# Build libs (${LIBS_RUN_MODE} mode)"
    _gen_libs
  fi

  if [ "$DO_PREBUILD" = true ]; then
    [ "$DO_LIBS" = true ] && echo ""
    echo "# Build prebuild image"
    _gen_prebuild
    [ "$DO_RUNTIME" = true ] && echo ""
  fi

  if [ "$DO_RUNTIME" = true ]; then
    echo "# Build runtime image (base: ${PREBUILD_IMAGE})"
    _gen_runtime
  fi

  echo "" >&2
  echo "── To execute, add --mode=run ──" >&2
  echo "" >&2
fi
