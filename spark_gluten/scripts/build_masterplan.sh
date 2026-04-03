#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

OUTPUT_DIR="build_artifacts/masterplan"

# Expected branches for reproducibility.
EXPECTED_GLUTEN_BRANCH="master-plan-v1-repro-20260317"
EXPECTED_VELOX_BRANCH="ferd-dev-v1-reconstructed"

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

Builds the "master plan" variant of Gluten+Velox, which collapses Spark's
multi-stage query plan into a single Substrait plan executed inside Velox.

The build is fully self-contained: Arrow Java, Maven, C++, .so injection,
and Arrow JNI are all handled inside a single Docker build.

Prerequisites:
  - Sibling directories must exist:
      ../spark-gluten/   (on branch: ${EXPECTED_GLUTEN_BRANCH})
      ../velox/          (on branch: ${EXPECTED_VELOX_BRANCH})

OPTIONS:
    -h, --help                   Show this help message.
    -o, --output-gluten-jar-dir  Directory for the output JAR (default: "${OUTPUT_DIR}").
    -j, --num-threads            Compilation threads (default: \$(nproc) / 2).
    -n, --no-cache               Wipe BuildKit cache and force full rebuild.
    --cuda-arch ARCHS            CUDA SM architectures (default: "89").
    --skip-branch-check          Skip branch verification.

EXAMPLES:
    $0
    $0 -o my_output_dir -j 16
    $0 --cuda-arch "80;89" --no-cache
    $0 -h

After building, run benchmarks with:
    SPARK_DATA_DIR=/path/to/data \\
    spark_gluten/scripts/run_benchmark.sh \\
      -b tpch -q "1,3" -d tpch_sf10 \\
      --gluten-jar-path ${OUTPUT_DIR}/gluten-*.jar \\
      --spark-config spark_gluten/scripts/masterplan_spark.conf

EOF
}

NUM_THREADS=$(($(nproc) / 2))
NO_CACHE=false
# Auto-detect CUDA architecture from host GPU.
SCRIPT_DIR_INIT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR_INIT}/../../scripts/cuda_helper.sh"
CUDA_ARCH="$(detect_cuda_architecture 2>/dev/null || echo "native")"
SKIP_BRANCH_CHECK=false

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -o|--output-gluten-jar-dir)
        if [[ -n $2 ]]; then
          OUTPUT_DIR=$2
          shift 2
        else
          echo "Error: --output-gluten-jar-dir requires a value"
          exit 1
        fi
        ;;
      -j|--num-threads)
        if [[ -n $2 ]]; then
          NUM_THREADS=$2
          shift 2
        else
          echo "Error: --num-threads requires a value"
          exit 1
        fi
        ;;
      -n|--no-cache)
        NO_CACHE=true
        shift
        ;;
      --cuda-arch)
        if [[ -n $2 ]]; then
          CUDA_ARCH=$2
          shift 2
        else
          echo "Error: --cuda-arch requires a value"
          exit 1
        fi
        ;;
      --skip-branch-check)
        SKIP_BRANCH_CHECK=true
        shift
        ;;
      *)
        echo "Error: Unknown argument $1"
        print_help
        exit 1
        ;;
    esac
  done
}

parse_args "$@"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../.."
WORKSPACE_ROOT="$REPO_ROOT/.."

SPARK_GLUTEN_DIR="$WORKSPACE_ROOT/spark-gluten"
VELOX_DIR="$WORKSPACE_ROOT/velox"
DOCKERFILE="$REPO_ROOT/spark_gluten/docker/masterplan_build.dockerfile"

# --- Validate sibling repos exist ---
for dir in "$SPARK_GLUTEN_DIR" "$VELOX_DIR"; do
  if [[ ! -d "$dir/.git" ]]; then
    echo "Error: required repo not found: $dir"
    echo "Expected workspace layout:"
    echo "  $(readlink -f "$WORKSPACE_ROOT")/"
    echo "    velox-testing/    (this repo)"
    echo "    spark-gluten/     (NVIDIA spark-gluten fork)"
    echo "    velox/            (Velox with cuDF extensions)"
    exit 1
  fi
done

# --- Verify and optionally checkout expected branches ---
if [[ "$SKIP_BRANCH_CHECK" != "true" ]]; then
  check_and_checkout_branch() {
    local repo_dir="$1"
    local expected="$2"
    local name="$3"
    local current
    current=$(git -C "$repo_dir" branch --show-current 2>/dev/null || echo "DETACHED")

    if [[ "$current" == "$expected" ]]; then
      return 0
    fi

    echo "Warning: $name is on branch '$current', expected '$expected'"

    # Check if working tree is clean enough to checkout
    if [[ -n "$(git -C "$repo_dir" status --porcelain 2>/dev/null)" ]]; then
      echo "  $name has uncommitted changes — cannot auto-checkout."
      read -r -p "  Continue on current branch anyway? [y/N] " response
      [[ "$response" =~ ^[Yy]$ ]] || exit 1
      return 0
    fi

    # Check if the expected branch exists locally
    if git -C "$repo_dir" rev-parse --verify "$expected" >/dev/null 2>&1; then
      read -r -p "  Checkout '$expected'? [Y/n] " response
      if [[ ! "$response" =~ ^[Nn]$ ]]; then
        git -C "$repo_dir" checkout "$expected"
      fi
    else
      echo "  Branch '$expected' not found locally."
      read -r -p "  Continue on current branch anyway? [y/N] " response
      [[ "$response" =~ ^[Yy]$ ]] || exit 1
    fi
  }

  check_and_checkout_branch "$SPARK_GLUTEN_DIR" "$EXPECTED_GLUTEN_BRANCH" "spark-gluten"
  check_and_checkout_branch "$VELOX_DIR" "$EXPECTED_VELOX_BRANCH" "velox"

  echo "spark-gluten: $(git -C "$SPARK_GLUTEN_DIR" branch --show-current) ($(git -C "$SPARK_GLUTEN_DIR" log --oneline -1))"
  echo "velox:        $(git -C "$VELOX_DIR" branch --show-current) ($(git -C "$VELOX_DIR" log --oneline -1))"
fi

# --- Docker build ---
BUILD_IMAGE="gluten-masterplan-build:latest"
# Tag for run_benchmark.sh compatibility (expects apache/gluten:*)
COMPAT_IMAGE="apache/gluten:masterplan"

echo ""
echo "Building master plan JAR (${NUM_THREADS} threads, CUDA arch: ${CUDA_ARCH}) ..."
echo ""

BUILD_ARGS=(
  -t "$BUILD_IMAGE"
  -f "$DOCKERFILE"
  --progress=plain
  --build-arg "NUM_THREADS=${NUM_THREADS}"
  --build-arg "CUDA_ARCHITECTURES=${CUDA_ARCH}"
  --build-arg "NO_CACHE=${NO_CACHE}"
)

if [[ "${NO_CACHE}" == "true" ]]; then
  BUILD_ARGS+=(--no-cache)
fi

DOCKER_BUILDKIT=1 docker build "${BUILD_ARGS[@]}" "$WORKSPACE_ROOT" 2>&1 | tee build_masterplan.log

# Tag for run_benchmark.sh compatibility
docker tag "$BUILD_IMAGE" "$COMPAT_IMAGE"

# --- Extract JAR from image ---
mkdir -p "$OUTPUT_DIR"
docker run --rm \
  -v "$(readlink -f "$OUTPUT_DIR"):/output" \
  "$BUILD_IMAGE" \
  bash -c "cp /opt/gluten/jars/* /output/ && chown -R $(id -u):$(id -g) /output"

echo ""
echo "Build complete."
echo "  JAR:   $OUTPUT_DIR/$(ls "$OUTPUT_DIR"/gluten-*.jar 2>/dev/null | head -1 | xargs basename)"
echo "  Image: $COMPAT_IMAGE"
echo ""
echo "To run benchmarks:"
echo "  SPARK_DATA_DIR=/path/to/data \\"
echo "  velox-testing/spark_gluten/scripts/run_benchmark.sh \\"
echo "    -b tpch -q \"1,3\" -d tpch_sf10 \\"
echo "    --image-tag masterplan \\"
echo "    --spark-config velox-testing/spark_gluten/testing/config/masterplan.conf \\"
echo "    --skip-drop-cache"
