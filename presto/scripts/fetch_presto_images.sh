#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Fetches the latest Presto images from GHCR and tags them locally with the
# current user's name:
#
#   presto-coordinator:$USER
#   presto-native-worker-cpu:$USER
#   presto-native-worker-gpu:$USER
#
# By default, images are filtered to those built from a specific Presto branch.
# The presto/velox SHAs are resolved by inspecting the most recent successful
# run of the presto.yml CI pipeline with a matching presto_commit input.
#
# Requires:
#   - gh CLI authenticated with read:packages scope
#   - docker

set -euo pipefail

REGISTRY="ghcr.io"
PACKAGE_REPO="rapidsai"
PACKAGE_NAME="velox-testing-images"
VELOX_TESTING_REPO="rapidsai/velox-testing"
IMAGE_BASE="${REGISTRY}/${PACKAGE_REPO}/${PACKAGE_NAME}"
LOCAL_TAG="${USER:-latest}"
BRANCH="ibm-research-preview-2026-03-31"
CUDA_VERSION=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [--branch BRANCH] [--cuda-version VERSION]

Fetches the latest Presto images built from a given branch and tags them locally.
Queries the presto.yml CI pipeline to resolve which presto/velox commit SHAs
correspond to that branch, then finds matching images in GHCR.

Options:
  --branch BRANCH         Presto branch to filter images by
                          (default: ibm-research-preview-2026-03-31)
  --cuda-version VERSION  CUDA version for the GPU worker image (e.g. 13.1, 12.9)
                          If omitted, the latest available CUDA version is used
  -h, --help              Show this help

Local tags created:
  presto-coordinator:\$USER
  presto-native-worker-cpu:\$USER
  presto-native-worker-gpu:\$USER
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)       BRANCH="$2";       shift 2 ;;
    --cuda-version) CUDA_VERSION="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# --- Step 1: Resolve presto/velox SHAs from the CI pipeline ---

echo "Searching presto.yml CI runs for builds from branch '${BRANCH}'..."

# Find the most recent successful presto.yml run whose display_title contains the branch name.
# Runs are named "presto (<variant_label>)" via the workflow's run-name field, so a run
# dispatched with variant_label=<BRANCH> will have display_title="presto (<BRANCH>)".
# Note: --paginate applies the jq expression per page; we collect all matches then pick
# the newest with a second jq pass.
RUN_JSON=$(gh api \
  --paginate \
  "repos/${VELOX_TESTING_REPO}/actions/workflows/presto.yml/runs?status=success&per_page=100" \
  --jq ".workflow_runs[] | select(.display_title | contains(\"${BRANCH}\"))" \
  | jq -s 'sort_by(.created_at) | reverse | .[0]')

if [[ -z "${RUN_JSON}" || "${RUN_JSON}" == "null" ]]; then
  echo "ERROR: No successful presto.yml run found with presto_commit='${BRANCH}'" >&2
  exit 1
fi

RUN_ID=$(echo "${RUN_JSON}" | jq -r '.id')
RUN_DATE=$(echo "${RUN_JSON}" | jq -r '.created_at')
echo "  Found run #${RUN_ID} (${RUN_DATE})"

# Find the resolve-commits job within this run
JOB_ID=$(gh api \
  "repos/${VELOX_TESTING_REPO}/actions/runs/${RUN_ID}/jobs" \
  --jq '.jobs[] | select(.name == "resolve-commits") | .id')

if [[ -z "${JOB_ID}" ]]; then
  echo "ERROR: resolve-commits job not found in run ${RUN_ID}" >&2
  exit 1
fi

# Download the job logs and parse the SHAs printed by the resolve-commits action:
#   "Presto SHA: <full-sha> (short: <7-char>)"
#   "Velox SHA:  <full-sha> (short: <7-char>)"
JOB_LOGS=$(gh api "repos/${VELOX_TESTING_REPO}/actions/jobs/${JOB_ID}/logs")

PRESTO_SHORT_SHA=$(echo "${JOB_LOGS}" | grep 'Presto SHA:' \
  | grep -oE '\(short: [0-9a-f]+\)' | grep -oE '[0-9a-f]+' | head -n 1)
VELOX_SHORT_SHA=$(echo "${JOB_LOGS}" | grep 'Velox SHA:' \
  | grep -oE '\(short: [0-9a-f]+\)' | grep -oE '[0-9a-f]+' | head -n 1)

if [[ -z "${PRESTO_SHORT_SHA}" || -z "${VELOX_SHORT_SHA}" ]]; then
  echo "ERROR: Could not parse SHAs from resolve-commits job logs (run ${RUN_ID}, job ${JOB_ID})" >&2
  exit 1
fi

echo "  Presto SHA : ${PRESTO_SHORT_SHA}"
echo "  Velox SHA  : ${VELOX_SHORT_SHA}"
echo ""

# --- Step 2: Find matching image tags in GHCR ---

echo "Querying GHCR for latest Presto image tags..."

# Fetch all tags once and cache them, sorted newest-first by date suffix (YYYYMMDD).
# awk extracts $NF (the date field) as a sort key, sort -rn orders by it descending,
# then cut strips the prepended key.
ALL_TAGS=$(gh api \
  --paginate \
  "orgs/${PACKAGE_REPO}/packages/container/${PACKAGE_NAME}/versions" \
  --jq '.[].metadata.container.tags[]' \
  | awk -F'-' '{print $NF"\t"$0}' | sort -rn | cut -f2-)

find_latest_tag() {
  local pattern="$1"
  echo "${ALL_TAGS}" | grep -E "${pattern}" | head -n 1
}

# coordinator: presto-coordinator-<presto-sha>-<YYYYMMDD>
COORDINATOR_TAG=$(find_latest_tag "^presto-coordinator-${PRESTO_SHORT_SHA}-[0-9]{8}$")
if [[ -z "${COORDINATOR_TAG}" ]]; then
  echo "ERROR: No coordinator image found for presto SHA ${PRESTO_SHORT_SHA}" >&2
  exit 1
fi
echo "  coordinator : ${COORDINATOR_TAG}"

# cpu worker: presto-<presto-sha>-velox-<velox-sha>-cpu-<YYYYMMDD>
CPU_TAG=$(find_latest_tag "^presto-${PRESTO_SHORT_SHA}-velox-${VELOX_SHORT_SHA}-cpu-[0-9]{8}$")
if [[ -z "${CPU_TAG}" ]]; then
  echo "ERROR: No CPU worker image found for presto SHA ${PRESTO_SHORT_SHA}, velox SHA ${VELOX_SHORT_SHA}" >&2
  exit 1
fi
echo "  cpu worker  : ${CPU_TAG}"

# gpu worker: presto-<presto-sha>-velox-<velox-sha>-gpu-cuda<ver>-<YYYYMMDD>
# If --cuda-version was given, anchor to that exact version; otherwise match any.
CUDA_PAT="${CUDA_VERSION:-[0-9.]+}"
GPU_TAG=$(find_latest_tag "^presto-${PRESTO_SHORT_SHA}-velox-${VELOX_SHORT_SHA}-gpu-cuda${CUDA_PAT}-[0-9]{8}$")
if [[ -z "${GPU_TAG}" ]]; then
  if [[ -n "${CUDA_VERSION}" ]]; then
    echo "ERROR: No GPU worker image found for presto SHA ${PRESTO_SHORT_SHA}, velox SHA ${VELOX_SHORT_SHA}, CUDA ${CUDA_VERSION}" >&2
  else
    echo "ERROR: No GPU worker image found for presto SHA ${PRESTO_SHORT_SHA}, velox SHA ${VELOX_SHORT_SHA}" >&2
  fi
  exit 1
fi
echo "  gpu worker  : ${GPU_TAG}"

echo ""

# --- Step 3: Pull and retag locally ---

pull_and_tag() {
  local remote_tag="$1"
  local local_name="$2"
  local remote="${IMAGE_BASE}:${remote_tag}"
  local local_image="${local_name}:${LOCAL_TAG}"

  echo "Pulling ${remote}..."
  docker pull "${remote}"

  echo "Tagging as ${local_image}..."
  docker tag "${remote}" "${local_image}"
  echo ""
}

pull_and_tag "${COORDINATOR_TAG}" "presto-coordinator"
pull_and_tag "${CPU_TAG}"         "presto-native-worker-cpu"
pull_and_tag "${GPU_TAG}"         "presto-native-worker-gpu"

echo "Done. Local images:"
echo "  presto-coordinator:${LOCAL_TAG}"
echo "  presto-native-worker-cpu:${LOCAL_TAG}"
echo "  presto-native-worker-gpu:${LOCAL_TAG}"
