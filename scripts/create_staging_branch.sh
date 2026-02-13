#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Staging Branch Creator
# Run with --help for usage and examples.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MERGE_COMMUTE_CMD=(python3 "${PROJECT_ROOT}/scripts/merge_commute.py" --strict-commute)
if [[ -d "${PROJECT_ROOT}/resolutions.d/contents" ]]; then
  MERGE_COMMUTE_CMD+=(--resolutions-dir "${PROJECT_ROOT}/resolutions.d")
fi

BASE_REPO=""
BASE_BRANCH=""
TARGET_REPO=""
TARGET_BRANCH="staging"
TARGET_PATH=""
WORK_DIR=""
AUTO_FETCH_PRS="true"
AUTO_FETCH_PR_LIMIT=200
MANUAL_PR_NUMBERS=""
EXCLUDE_PR_NUMBERS=""
ADDITIONAL_PR_NUMBERS=""
PR_LABELS=""
MANIFEST_TEMPLATE=""
FORCE_PUSH="false"
ADDITIONAL_REPOSITORY=""
ADDITIONAL_BRANCH=""
GH_TOKEN="${GH_TOKEN:-}"
USE_LOCAL_PATH="false"
MODE="local"
STEP_NAME=""
DUMP_CONFLICTS="false"
CONFLICT_REPORT_DIR=""
SKIPPED_PRS=""
RESET_BASE_COMMIT=""
PURGE_UNUSED_RESOLUTIONS="false"
declare -A PR_SHA
RESOLUTION_STATS_DIR=""

log() { echo "$@" >&2; }
die() { log "ERROR: $*"; exit 1; }
STEP=0
step() {
  STEP=$((STEP + 1))
  log "==== [${STEP}] $* ===="
}
divider() {
  log "---- $* ----"
}
normalize_repo_url() {
  local url="$1"
  echo "${url}" | awk -F'[:/]' '{print $(NF-1)"/"$NF}' | sed 's/\.git$//'
}

ensure_conflict_report_dir() {
  if [[ "${DUMP_CONFLICTS}" != "true" ]]; then
    return 1
  fi
  if [[ -z "${CONFLICT_REPORT_DIR}" ]]; then
    CONFLICT_REPORT_DIR="${PROJECT_ROOT}/staging-conflict-report"
    rm -rf "${CONFLICT_REPORT_DIR}"
    mkdir -p "${CONFLICT_REPORT_DIR}"
    log "Conflict report directory: ${CONFLICT_REPORT_DIR}"
  fi
  return 0
}

safe_report_name() {
  local filepath="$1"
  local normalized
  normalized="$(echo "${filepath}" | tr '/' '_')"
  local path_hash
  path_hash="$(printf '%s' "${filepath}" | git hash-object --stdin | cut -c1-12)"
  echo "${normalized}__${path_hash}"
}

write_conflict_context() {
  local report_dir="$1"
  local conflict_kind="$2"
  local source_repo="$3"
  local source_ref="$4"
  local source_url="$5"
  local note="${6:-}"
  local generated_at
  generated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local reset_source_commit="${RESET_BASE_COMMIT:-${BASE_COMMIT:-unknown}}"
  local additional_enabled="false"
  local effective_base_description="${BASE_REPO}/${BASE_BRANCH}"
  local additional_source_url=""
  if [[ -n "${ADDITIONAL_REPOSITORY}" && -n "${ADDITIONAL_BRANCH}" ]]; then
    additional_enabled="true"
    effective_base_description="${effective_base_description} + ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}"
    additional_source_url="https://github.com/${ADDITIONAL_REPOSITORY}/tree/${ADDITIONAL_MERGE_COMMIT:-${ADDITIONAL_BRANCH}}"
  fi

  {
    echo "generated_at_utc=${generated_at}"
    echo "mode=${MODE}"
    echo "reset_source_repository=${BASE_REPO}"
    echo "reset_source_branch=${BASE_BRANCH}"
    echo "reset_source_commit=${reset_source_commit}"
    echo "base_repository=${BASE_REPO}"
    echo "base_branch=${BASE_BRANCH}"
    echo "base_commit=${reset_source_commit}"
    echo "effective_base_description=${effective_base_description}"
    echo "effective_base_commit=${BASE_COMMIT:-unknown}"
    echo "includes_additional_merge=${additional_enabled}"
    echo "additional_repository=${ADDITIONAL_REPOSITORY:-none}"
    echo "additional_branch=${ADDITIONAL_BRANCH:-none}"
    echo "additional_merge_commit=${ADDITIONAL_MERGE_COMMIT:-none}"
    if [[ -n "${additional_source_url}" ]]; then
      echo "additional_source_url=${additional_source_url}"
    fi
    echo "target_repository=${TARGET_REPO:-unknown}"
    echo "target_branch=${TARGET_BRANCH}"
    echo "conflict_kind=${conflict_kind}"
    echo "source_repository=${source_repo}"
    echo "source_ref=${source_ref}"
    echo "source_url=${source_url}"
    if [[ -n "${note}" ]]; then
      echo "note=${note}"
    fi
  } > "${report_dir}/CONFLICT_CONTEXT.txt"
}

ensure_sibling_layout() {
  local target_dir="$1"
  local parent_dir
  parent_dir="$(cd "${target_dir}/.." && pwd)"
  local velox_testing_dir="${parent_dir}/velox-testing"

  if [[ ! -d "${target_dir}" ]]; then
    die "Target directory not found: ${target_dir}"
  fi
  if [[ ! -d "${velox_testing_dir}" ]]; then
    die "Expected velox-testing sibling directory not found: ${velox_testing_dir}"
  fi
}
emit_output() {
  local name="$1"
  local value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "${name}=${value}" >> "${GITHUB_OUTPUT}"
  fi
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "${name}=${value}" >> "${GITHUB_ENV}"
  fi
}

require_env_var() {
  local name="$1"
  local value="${!name:-}"
  [[ -n "${value}" ]] || die "Missing ${name}. Run the appropriate prior step to set it."
}

retry() {
  local attempts=5
  local sleep_s=2
  local n=1
  local cmd=("$@")
  while true; do
    if "${cmd[@]}"; then
      return 0
    fi
    if [[ "${n}" -ge "${attempts}" ]]; then
      return 1
    fi
    log "WARN: command failed (attempt ${n}/${attempts}), retrying in ${sleep_s}s..."
    sleep "${sleep_s}"
    n=$((n + 1))
    sleep_s=$((sleep_s * 2))
  done
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Creates a staging branch by merging PRs from a base repository.

Prerequisites (local mode with --target-path):
  The target repository will be DESTRUCTIVELY modified. The script performs:
    - git checkout -B (switches/creates branch, discards conflicting changes)
    - git reset --hard (discards all uncommitted changes to tracked files)
    - git clean -fd    (removes untracked files and directories)

  Before running locally, ensure you:
    1. Have NO uncommitted changes you want to keep (stash or commit them first)
    2. Have NO untracked files you want to keep in the repo directory
    3. Are NOT on a branch with work-in-progress changes

  In CI mode (--mode ci), the script clones a fresh copy, so this is not a concern.

Required:
  --target-path /path/to/repo      Local path to target repository

Options:
  --base-repository owner/repo     Base repository (default: ${BASE_REPO})
  --base-branch branch             Base branch (default: ${BASE_BRANCH})
  --target-branch branch           Target branch (default: ${TARGET_BRANCH})
  --work-dir path                  Directory to clone target repo (default: ${WORK_DIR})
  --auto-fetch-prs true|false      Auto-fetch non-draft PRs with label (default: ${AUTO_FETCH_PRS})
  --manual-pr-numbers "1,2,3"      Comma-separated PR numbers to merge (disables auto-fetch)
  --exclude-pr-numbers "4,5,6"    Comma-separated PR numbers to exclude from auto-fetch results
  --additional-pr-numbers "7,8"   Comma-separated PR numbers to append to fetched/manual list
  --pr-labels labels               Comma-separated PR labels to auto-fetch (default: ${PR_LABELS})
  --manifest-template path         Manifest template path (default: repo template)
  --force-push true|false          Force push to target branch (default: ${FORCE_PUSH})
  --additional-repository repo     Additional repository to merge from (e.g., rapidsai/cudf)
  --additional-branch branch       Branch from additional repository to merge
  --mode local|ci                  Execution mode (default: ${MODE})
  --step name                      Run a single step (see below)
  --dump-conflicts                 Dump diff3 conflict reports to velox-testing/staging-conflict-report/
  --purge-unused-resolutions       Remove banked resolutions not used in this run
                                   (includes CONFLICT_CONTEXT.txt and FILE_INDEX.tsv per report)
  -h, --help                       Show this help

Environment:
  GH_TOKEN                         GitHub token for cloning/pushing and gh API calls

Steps (for --step option):
  reset, fetch-prs, test-merge, test-pairwise, merge, merge-additional, manifest, push, all

Examples:

  # Using child scripts (recommended):
  ./velox/scripts/create_staging.sh
  ./presto/scripts/create_staging.sh --manual-pr-numbers "27057,27054"

  # Direct usage with Velox:
  ./scripts/create_staging_branch.sh \\
    --target-path ../velox \\
    --base-repository facebookincubator/velox \\
    --base-branch main \\
    --pr-labels "cudf"

  # Manual PR list (auto-fetch disabled automatically):
  ./scripts/create_staging_branch.sh \\
    --target-path ../velox \\
    --base-repository facebookincubator/velox \\
    --manual-pr-numbers "12345,12346,12347"

  # Auto-fetch + explicitly include extra PRs (e.g., draft/unlabeled):
  ./scripts/create_staging_branch.sh \\
    --target-path ../velox \\
    --base-repository facebookincubator/velox \\
    --pr-labels "cudf" \\
    --additional-pr-numbers "17001,17005"

  # Merge with additional repository (e.g., cuDF exchange):
  ./scripts/create_staging_branch.sh \\
    --target-path ../velox \\
    --base-repository facebookincubator/velox \\
    --pr-labels "cudf" \\
    --additional-repository rapidsai/cudf \\
    --additional-branch velox-exchange

  # CI mode (no interactive prompt, pushes to remote):
  GH_TOKEN=... ./scripts/create_staging_branch.sh \\
    --mode ci \\
    --target-path ./velox \\
    --base-repository facebookincubator/velox \\
    --force-push true

Notes:
  - In local mode (default), push to remote is skipped.
  - If --target-path is provided, the script will prompt before resetting.
  - Auto-fetch queries at most ${AUTO_FETCH_PR_LIMIT} open PRs from GitHub.
    If more matching PRs exist, use narrower labels/exclusions or --manual-pr-numbers.
  - Option --additional-pr-numbers appends explicit PRs to the list built from
    auto-fetch or --manual-pr-numbers.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --target-path) TARGET_PATH="$2"; shift 2 ;;
      --base-repository) BASE_REPO="$2"; shift 2 ;;
      --base-branch) BASE_BRANCH="$2"; shift 2 ;;
      --target-branch) TARGET_BRANCH="$2"; shift 2 ;;
      --work-dir) WORK_DIR="$2"; shift 2 ;;
      --auto-fetch-prs) AUTO_FETCH_PRS="$2"; shift 2 ;;
      --manual-pr-numbers) MANUAL_PR_NUMBERS="$2"; AUTO_FETCH_PRS="false"; shift 2 ;;
      --exclude-pr-numbers) EXCLUDE_PR_NUMBERS="$2"; shift 2 ;;
      --additional-pr-numbers) ADDITIONAL_PR_NUMBERS="$2"; shift 2 ;;
      --pr-labels) PR_LABELS="$2"; shift 2 ;;
      --manifest-template) MANIFEST_TEMPLATE="$2"; shift 2 ;;
      --force-push) FORCE_PUSH="$2"; shift 2 ;;
      --additional-repository) ADDITIONAL_REPOSITORY="$2"; shift 2 ;;
      --additional-branch) ADDITIONAL_BRANCH="$2"; shift 2 ;;
      --mode) MODE="$2"; shift 2 ;;
      --step) STEP_NAME="$2"; shift 2 ;;
      --dump-conflicts) DUMP_CONFLICTS="true"; shift ;;
      --purge-unused-resolutions) PURGE_UNUSED_RESOLUTIONS="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option: $1" ;;
    esac
  done
}

ensure_repo_inputs() {
  if [[ -z "${TARGET_PATH}" ]]; then
    die "Missing --target-path (repo should already be cloned)."
  fi
  if [[ "${MODE}" == "local" && -z "${TARGET_PATH}" ]]; then
    die "Local mode requires --target-path (repo should already be cloned locally)."
  fi
  if [[ "${MODE}" == "ci" && -z "${TARGET_PATH}" ]]; then
    die "CI mode requires --target-path. Clone the repo in the workflow step."
  fi
}

setup_git_config() {
  local repo_dir="$1"
  if ! git -C "${repo_dir}" config user.name >/dev/null 2>&1; then
    git -C "${repo_dir}" config user.name "velox-staging-bot"
  fi
  if ! git -C "${repo_dir}" config user.email >/dev/null 2>&1; then
    git -C "${repo_dir}" config user.email "velox-staging-bot@users.noreply.github.com"
  fi
  # diff3 conflict style ensures conflict markers include the base section,
  # matching git merge-file --diff3 used by the resolution bank.
  git -C "${repo_dir}" config merge.conflictstyle diff3
}

init_target_repo() {
  if [[ -z "${TARGET_PATH}" ]]; then
    die "target path not found: provide --target-path (cloning is handled in CI workflow)"
  fi
  [[ -d "${TARGET_PATH}" ]] || die "target path not found: ${TARGET_PATH}"
  WORK_DIR="$(cd "${TARGET_PATH}" && pwd)"
  git -C "${WORK_DIR}" rev-parse --git-dir >/dev/null 2>&1 || die "target path is not a git repo: ${WORK_DIR}"
  if [[ -z "${TARGET_REPO}" ]]; then
    TARGET_REPO="$(normalize_repo_url "$(git -C "${WORK_DIR}" remote get-url origin 2>/dev/null || true)")"
  fi
  USE_LOCAL_PATH="true"
}

setup_remotes() {
  local repo_dir="$1"
  if [[ "${USE_LOCAL_PATH}" == "true" ]]; then
    local upstream_url
    upstream_url="$(git -C "${repo_dir}" remote get-url upstream 2>/dev/null || true)"
    if [[ -z "${upstream_url}" ]]; then
      log "Upstream remote not set. Adding upstream -> https://github.com/${BASE_REPO}.git"
      git -C "${repo_dir}" remote add upstream "https://github.com/${BASE_REPO}.git"
      return 0
    fi
    local normalized
    # Extract owner/repo by splitting on ':' and '/' and trimming trailing .git
    normalized="$(echo "${upstream_url}" | awk -F'[:/]' '{print $(NF-1)"/"$NF}' | sed 's/\.git$//')"
    if [[ "${normalized}" != "${BASE_REPO}" ]]; then
      die "Upstream remote points to ${normalized}. Please add a different remote name for ${BASE_REPO} and update --base-repository accordingly."
    fi
    return 0
  fi
  if ! git -C "${repo_dir}" remote get-url upstream >/dev/null 2>&1; then
    git -C "${repo_dir}" remote add upstream "https://github.com/${BASE_REPO}.git"
  fi
  log "Fetching upstream ${BASE_BRANCH}..."
  retry git -C "${repo_dir}" fetch upstream "${BASE_BRANCH}"
}

set_dated_branch() {
  if [[ "${MODE}" == "local" ]]; then
    DATED_BRANCH=""
    export DATED_BRANCH
    return 0
  fi
  CURRENT_DATE="$(date -u +%m-%d-%Y)"
  DATED_BRANCH="${TARGET_BRANCH}_${CURRENT_DATE}"
  export DATED_BRANCH
  log "Dated branch: ${DATED_BRANCH}"
}

reset_target_branch() {
  local repo_dir="$1"
  if [[ "${USE_LOCAL_PATH}" == "true" ]]; then
    log "Resetting ${TARGET_BRANCH} to ${BASE_REPO}/${BASE_BRANCH} (via direct fetch)..."
    retry git -C "${repo_dir}" fetch "https://github.com/${BASE_REPO}.git" "${BASE_BRANCH}"
    git -C "${repo_dir}" checkout -B "${TARGET_BRANCH}" FETCH_HEAD
  else
    log "Resetting ${TARGET_BRANCH} to upstream/${BASE_BRANCH}..."
    git -C "${repo_dir}" checkout -B "${TARGET_BRANCH}" "upstream/${BASE_BRANCH}"
  fi
  BASE_COMMIT="$(git -C "${repo_dir}" rev-parse HEAD)"
  RESET_BASE_COMMIT="${BASE_COMMIT}"
  export BASE_COMMIT RESET_BASE_COMMIT
  emit_output BASE_COMMIT "${BASE_COMMIT}"
  emit_output RESET_BASE_COMMIT "${RESET_BASE_COMMIT}"
  log "Base commit: ${BASE_COMMIT}"
}

fetch_pr_list() {
  local pr_list=""
  if [[ "${AUTO_FETCH_PRS}" == "true" ]]; then
    step "Auto-fetch PRs with labels: ${PR_LABELS}"
    log "Auto-fetch query limit: ${AUTO_FETCH_PR_LIMIT} PRs"
    local label_args=()
    local labels=()
    IFS=',' read -r -a labels <<< "${PR_LABELS}"
    for label in "${labels[@]}"; do
      label="$(echo "${label}" | xargs)"
      [[ -z "${label}" ]] && continue
      label_args+=(--label "${label}")
    done
    pr_list="$(gh pr list \
      --repo "${BASE_REPO}" \
      "${label_args[@]}" \
      --state open \
      --limit "${AUTO_FETCH_PR_LIMIT}" \
      --json number,isDraft \
      --jq '.[] | select(.isDraft == false) | .number' | tr '\n' ' ' | xargs || true)"
    local fetched_count
    fetched_count="$(echo "${pr_list}" | wc -w | xargs)"
    if [[ "${fetched_count}" -ge "${AUTO_FETCH_PR_LIMIT}" ]]; then
      log "WARN: Auto-fetch returned ${fetched_count} PRs (limit ${AUTO_FETCH_PR_LIMIT})."
      log "WARN: Additional matching PRs may exist but were not included."
      log "WARN: Use narrower labels/exclusions or provide --manual-pr-numbers."
    fi
  else
    pr_list="$(echo "${MANUAL_PR_NUMBERS}" | tr ',' ' ' | xargs || true)"
  fi

  # Append explicitly requested PRs (e.g., drafts or unlabeled PRs).
  if [[ -n "${ADDITIONAL_PR_NUMBERS}" ]]; then
    local additional_list
    additional_list="$(echo "${ADDITIONAL_PR_NUMBERS}" | tr ',' ' ' | xargs || true)"
    for pr in ${additional_list}; do
      [[ "${pr}" =~ ^[0-9]+$ ]] || die "Invalid PR number in --additional-pr-numbers: ${pr}"
      pr_list="${pr_list} ${pr}"
    done
    pr_list="$(echo "${pr_list}" | xargs || true)"
    log "Appended additional PRs: ${additional_list}"
  fi

  # Dedupe while preserving order.
  if [[ -n "${pr_list}" ]]; then
    local deduped=""
    declare -A seen_prs=()
    for pr in ${pr_list}; do
      [[ "${pr}" =~ ^[0-9]+$ ]] || die "Invalid PR number in computed PR list: ${pr}"
      if [[ -z "${seen_prs[$pr]:-}" ]]; then
        seen_prs["$pr"]=1
        deduped="${deduped} ${pr}"
      fi
    done
    pr_list="$(echo "${deduped}" | xargs || true)"
  fi

  # Exclude specified PRs
  if [[ -n "${EXCLUDE_PR_NUMBERS}" ]]; then
    local exclude_list
    exclude_list="$(echo "${EXCLUDE_PR_NUMBERS}" | tr ',' ' ')"
    local filtered=""
    for pr in ${pr_list}; do
      local excluded=false
      for ex in ${exclude_list}; do
        if [[ "${pr}" == "${ex}" ]]; then
          excluded=true
          break
        fi
      done
      if [[ "${excluded}" == "false" ]]; then
        filtered="${filtered} ${pr}"
      else
        log "Excluding PR #${pr}"
      fi
    done
    pr_list="$(echo "${filtered}" | xargs || true)"
  fi

  if [[ -z "${pr_list}" ]]; then
    die "No PRs found to merge."
  fi
  PR_LIST="${pr_list}"
  export PR_LIST
  emit_output PR_LIST "${PR_LIST}"
  emit_output PR_COUNT "$(echo "${PR_LIST}" | wc -w | xargs)"
  log "PRs to process: ${PR_LIST}"
}

fetch_pr_head() {
  local repo_dir="$1"
  local pr_num="$2"
  if [[ -n "${PR_SHA[$pr_num]:-}" ]]; then
    return 0
  fi
  if [[ "${USE_LOCAL_PATH}" == "true" ]]; then
    retry git -C "${repo_dir}" fetch "https://github.com/${BASE_REPO}.git" "pull/${pr_num}/head"
  else
    retry git -C "${repo_dir}" fetch upstream "pull/${pr_num}/head"
  fi
  PR_SHA[$pr_num]="$(git -C "${repo_dir}" rev-parse FETCH_HEAD 2>/dev/null || true)"
  if [[ -z "${PR_SHA[$pr_num]}" ]]; then
    die "Failed to resolve PR #${pr_num} HEAD SHA"
  fi
}

get_pr_source_ref() {
  local pr_num="$1"
  local source_ref
  source_ref="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" \
    --json headRepositoryOwner,headRefName \
    --jq 'if .headRepositoryOwner and .headRepositoryOwner.login and .headRefName then (.headRepositoryOwner.login + ":" + .headRefName) else empty end' \
    2>/dev/null || true)"
  if [[ -n "${source_ref}" ]]; then
    echo "${source_ref}"
  else
    echo "pull/${pr_num}/head"
  fi
}

dump_conflict_report() {
  local repo_dir="$1"
  local report_key="$2"
  local conflict_kind="$3"
  local source_repo="$4"
  local source_ref="$5"
  local source_url="$6"
  local note="${7:-}"

  if ! ensure_conflict_report_dir; then
    return 0
  fi

  local report_dir="${CONFLICT_REPORT_DIR}/${report_key}"
  rm -rf "${report_dir}"
  mkdir -p "${report_dir}"
  write_conflict_context "${report_dir}" "${conflict_kind}" "${source_repo}" "${source_ref}" "${source_url}" "${note}"
  : > "${report_dir}/FILE_INDEX.tsv"

  local unmerged
  unmerged="$(git -C "${repo_dir}" diff --name-only --diff-filter=U 2>/dev/null || true)"
  if [[ -z "${unmerged}" ]]; then
    return 0
  fi

  while IFS= read -r filepath; do
    [[ -z "${filepath}" ]] && continue
    local safe_name
    safe_name="$(safe_report_name "${filepath}")"

    # Working tree with conflict markers (user edits this to resolve)
    local wt_file="${repo_dir}/${filepath}"
    if [[ -f "${wt_file}" ]]; then
      cp "${wt_file}" "${report_dir}/${safe_name}.conflict"
    fi

    # Original filepath (for recording script)
    echo "${filepath}" > "${report_dir}/${safe_name}.filepath"
    printf '%s\t%s\n' "${safe_name}" "${filepath}" >> "${report_dir}/FILE_INDEX.tsv"

    # Stage 1: base, Stage 2: ours, Stage 3: theirs
    git -C "${repo_dir}" show ":1:${filepath}" > "${report_dir}/${safe_name}.base" 2>/dev/null || true
    git -C "${repo_dir}" show ":2:${filepath}" > "${report_dir}/${safe_name}.ours" 2>/dev/null || true
    git -C "${repo_dir}" show ":3:${filepath}" > "${report_dir}/${safe_name}.theirs" 2>/dev/null || true
  done <<< "${unmerged}"

  local file_count
  file_count="$(echo "${unmerged}" | wc -l | xargs)"
  log "  Dumped conflict report for ${report_key}: ${file_count} file(s) -> ${report_dir}"
}

test_merge_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local successful=""
  local conflicts=""

  step "Testing merge compatibility against ${BASE_REPO}/${BASE_BRANCH}"
  for pr_num in ${pr_list}; do
    divider "PR #${pr_num}"
    fetch_pr_head "${repo_dir}" "${pr_num}"
    local resol_log="${RESOLUTION_STATS_DIR}/PR-${pr_num}.tsv"
    : > "${resol_log}"
    if (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge --resolution-log "${resol_log}" "${TARGET_BRANCH}" "${PR_SHA[$pr_num]}"); then
      successful="${successful} ${pr_num}"
    else
      conflicts="${conflicts} ${pr_num}"
      local pr_source_ref
      pr_source_ref="$(get_pr_source_ref "${pr_num}")"
      dump_conflict_report \
        "${repo_dir}" \
        "PR-${pr_num}" \
        "pr" \
        "${BASE_REPO}" \
        "${pr_source_ref}@${PR_SHA[$pr_num]}" \
        "https://github.com/${BASE_REPO}/pull/${pr_num}"
      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    fi
    git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
    git -C "${repo_dir}" clean -fd >/dev/null
  done

  if [[ -n "$(echo "${conflicts}" | xargs)" ]]; then
    log ""
    log "Conflicts detected (skipping) for PRs:${conflicts}"
    for pr_num in ${conflicts}; do
      log "  PR #${pr_num}: https://github.com/${BASE_REPO}/pull/${pr_num}"
    done
    if [[ -n "${CONFLICT_REPORT_DIR}" ]]; then
      log "Conflict reports saved to: ${CONFLICT_REPORT_DIR}"
    fi
    log ""
    successful="$(echo "${successful}" | xargs)"
    if [[ -z "${successful}" ]]; then
      die "No PRs remain after removing conflicts."
    fi
    PR_LIST="${successful}"
    export PR_LIST
    emit_output PR_LIST "${PR_LIST}"
    emit_output PR_COUNT "$(echo "${PR_LIST}" | wc -w | xargs)"
    SKIPPED_PRS="$(echo "${conflicts}" | xargs)"
    export SKIPPED_PRS
    log "Continuing with PRs: ${PR_LIST}"
  else
    log "All PRs can merge cleanly with ${BASE_BRANCH}."
  fi
}

dump_pairwise_conflict_report() {
  local repo_dir="$1"
  local pr1="$2"
  local pr2="$3"
  local which_failed="$4"  # "first" or "second"

  if ! ensure_conflict_report_dir; then
    return 0
  fi

  local pair_dir="${CONFLICT_REPORT_DIR}/pairwise/PR-${pr1}+PR-${pr2}"
  rm -rf "${pair_dir}"
  mkdir -p "${pair_dir}"
  write_conflict_context \
    "${pair_dir}" \
    "pairwise-prs" \
    "${BASE_REPO}" \
    "pull/${pr1}/head@${PR_SHA[$pr1]:-unknown} + pull/${pr2}/head@${PR_SHA[$pr2]:-unknown}" \
    "https://github.com/${BASE_REPO}/pull/${pr1},https://github.com/${BASE_REPO}/pull/${pr2}" \
    "failed_on=${which_failed}_merge"
  : > "${pair_dir}/FILE_INDEX.tsv"

  local unmerged
  unmerged="$(git -C "${repo_dir}" diff --name-only --diff-filter=U 2>/dev/null || true)"
  if [[ -z "${unmerged}" ]]; then
    echo "No unmerged files found (${which_failed} merge failed without conflict markers)" > "${pair_dir}/NOTE.txt"
    return 0
  fi

  while IFS= read -r filepath; do
    [[ -z "${filepath}" ]] && continue
    local safe_name
    safe_name="$(safe_report_name "${filepath}")"

    # Working tree with conflict markers (user edits this to resolve)
    local wt_file="${repo_dir}/${filepath}"
    if [[ -f "${wt_file}" ]]; then
      cp "${wt_file}" "${pair_dir}/${safe_name}.conflict"
    fi

    # Original filepath (for recording script)
    echo "${filepath}" > "${pair_dir}/${safe_name}.filepath"
    printf '%s\t%s\n' "${safe_name}" "${filepath}" >> "${pair_dir}/FILE_INDEX.tsv"

    # Stage 1: base, Stage 2: ours, Stage 3: theirs
    git -C "${repo_dir}" show ":1:${filepath}" > "${pair_dir}/${safe_name}.base" 2>/dev/null || true
    git -C "${repo_dir}" show ":2:${filepath}" > "${pair_dir}/${safe_name}.ours" 2>/dev/null || true
    git -C "${repo_dir}" show ":3:${filepath}" > "${pair_dir}/${safe_name}.theirs" 2>/dev/null || true
  done <<< "${unmerged}"

  local file_count
  file_count="$(echo "${unmerged}" | wc -l | xargs)"
  log "  Dumped pairwise conflict report for PR #${pr1} + PR #${pr2} (${which_failed} merge): ${file_count} file(s) -> ${pair_dir}"
}

test_pairwise_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local pr_array=(${pr_list})
  local pr_count="${#pr_array[@]}"
  local conflict_pairs=""
  declare -A pair_results  # Store results for matrix display
  declare -A conflict_prs  # Track PRs involved in conflicts

  if [[ "${pr_count}" -lt 2 ]]; then
    return 0
  fi

  step "Testing pairwise merge compatibility"
  for ((i=0; i<pr_count; i++)); do
    for ((j=i+1; j<pr_count; j++)); do
      local pr1="${pr_array[$i]}"
      local pr2="${pr_array[$j]}"
      fetch_pr_head "${repo_dir}" "${pr1}"
      fetch_pr_head "${repo_dir}" "${pr2}"
      git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
      git -C "${repo_dir}" clean -fd >/dev/null

      if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --auto-continue "${TARGET_BRANCH}" "${PR_SHA[$pr1]}") >/dev/null 2>&1; then
        dump_pairwise_conflict_report "${repo_dir}" "${pr1}" "${pr2}" "first"
        git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
        git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
        pair_results["${pr1},${pr2}"]="XX"
        conflict_prs["${pr1}"]=1
        conflict_prs["${pr2}"]=1
        continue
      fi

      if (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge "${TARGET_BRANCH}" "${PR_SHA[$pr2]}") >/dev/null 2>&1; then
        pair_results["${pr1},${pr2}"]="OK"
      else
        dump_pairwise_conflict_report "${repo_dir}" "${pr1}" "${pr2}" "second"
        git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
        pair_results["${pr1},${pr2}"]="XX"
        conflict_prs["${pr1}"]=1
        conflict_prs["${pr2}"]=1
      fi

      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
      git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
      git -C "${repo_dir}" clean -fd >/dev/null
    done
  done

  # Display pairwise compatibility matrix
  log ""
  log "Pairwise Compatibility Matrix:"
  log "Legend: OK = Compatible, XX = Conflict"
  log ""

  # Column width based on longest PR number (min 7 for "#XXXXX")
  local col_w=7
  for pr in "${pr_array[@]}"; do
    local len=$(( ${#pr} + 1 ))  # +1 for '#' prefix
    (( len > col_w )) && col_w=$len
  done

  # Helper to build a divider row: +--------+--------+...
  _matrix_divider() {
    local div="+"
    div+="$(printf '%*s' $((col_w + 2)) '' | tr ' ' '-')+"
    for ((d=0; d<pr_count; d++)); do
      div+="$(printf '%*s' $((col_w + 2)) '' | tr ' ' '-')+"
    done
    log "${div}"
  }

  # Print header row
  _matrix_divider
  local header
  header="$(printf "| %-${col_w}s " "PR")"
  for pr in "${pr_array[@]}"; do
    header+="$(printf "| %${col_w}s " "#${pr}")"
  done
  header+="|"
  log "${header}"
  _matrix_divider

  # Print matrix rows with row dividers
  for ((i=0; i<pr_count; i++)); do
    local pr1="${pr_array[$i]}"
    local row
    row="$(printf "| %-${col_w}s " "#${pr1}")"
    for ((j=0; j<pr_count; j++)); do
      local pr2="${pr_array[$j]}"
      local cell
      if [[ $i -eq $j ]]; then
        cell="--"
      elif [[ $i -lt $j ]]; then
        cell="${pair_results[${pr1},${pr2}]:-?}"
      else
        cell="${pair_results[${pr2},${pr1}]:-?}"
      fi
      row+="$(printf "| %${col_w}s " "${cell}")"
    done
    row+="|"
    log "${row}"
    _matrix_divider
  done

  # If conflicts exist, show detailed table with PR authors and links
  if [[ -n "$(echo "${conflict_pairs}" | xargs)" ]]; then
    log ""
    log "PRs Involved in Conflicts:"
    log ""
    log "$(printf "| %-10s | %-20s | %-50s | %-55s |" "PR" "Author" "Title" "URL")"
    log "$(printf "| %-10s | %-20s | %-50s | %-55s |" "----------" "--------------------" "--------------------------------------------------" "-------------------------------------------------------")"

    for pr_num in "${!conflict_prs[@]}"; do
      local pr_author pr_title pr_url
      pr_author="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json author --jq '.author.login' 2>/dev/null || echo "N/A")"
      pr_title="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json title --jq '.title' 2>/dev/null || echo "N/A")"
      pr_url="https://github.com/${BASE_REPO}/pull/${pr_num}"
      # Truncate title if too long
      if [[ ${#pr_title} -gt 47 ]]; then
        pr_title="${pr_title:0:47}..."
      fi
      log "$(printf "| %-10s | %-20s | %-50s | %-55s |" "#${pr_num}" "${pr_author}" "${pr_title}" "${pr_url}")"
    done

    log ""
    log "Conflict Pairs: ${conflict_pairs}"
    exit 1
  fi
  log ""
  log "All PR pairs can merge cleanly together."
}

merge_prs() {
  local repo_dir="$1"
  local pr_list="$2"
  local merged=""
  local count=0

  step "Merging PRs: ${pr_list}"
  for pr_num in ${pr_list}; do
    fetch_pr_head "${repo_dir}" "${pr_num}"
    local pr_title
    pr_title="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json title --jq '.title' 2>/dev/null || echo "PR #${pr_num}")"
    local resol_log="${RESOLUTION_STATS_DIR}/PR-${pr_num}.tsv"
    : > "${resol_log}"
    if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge --resolution-log "${resol_log}" "${TARGET_BRANCH}" "${PR_SHA[$pr_num]}") 2>&1; then
      local pr_source_ref
      pr_source_ref="$(get_pr_source_ref "${pr_num}")"
      dump_conflict_report \
        "${repo_dir}" \
        "PR-${pr_num}" \
        "pr" \
        "${BASE_REPO}" \
        "${pr_source_ref}@${PR_SHA[$pr_num]}" \
        "https://github.com/${BASE_REPO}/pull/${pr_num}"
      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
      log "Merge conflict in PR #${pr_num}. Aborting."
      exit 1
    fi
    if git -C "${repo_dir}" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1; then
      git -C "${repo_dir}" commit -m "Merge PR #${pr_num}: ${pr_title}"
    else
      log "PR #${pr_num} already up to date; no merge commit created."
    fi
    merged="${merged} ${pr_num}"
    count=$((count + 1))
  done

  MERGED_PRS="${merged}"
  MERGED_COUNT="${count}"
  export MERGED_PRS MERGED_COUNT
  emit_output MERGED_PRS "${MERGED_PRS}"
  emit_output MERGED_COUNT "${MERGED_COUNT}"
  log "Merged ${MERGED_COUNT} PRs."
}

merge_additional_repository() {
  local repo_dir="$1"

  if [[ -z "${ADDITIONAL_REPOSITORY}" || -z "${ADDITIONAL_BRANCH}" ]]; then
    log "No additional repository configured, skipping."
    return 0
  fi

  step "Merging additional repository: ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}"

  local additional_remote="additional-merge-source"
  local additional_url="https://github.com/${ADDITIONAL_REPOSITORY}.git"

  # Add remote if missing; if present, ensure it points to the requested repository.
  local existing_additional_url
  existing_additional_url="$(git -C "${repo_dir}" remote get-url "${additional_remote}" 2>/dev/null || true)"
  if [[ -z "${existing_additional_url}" ]]; then
    git -C "${repo_dir}" remote add "${additional_remote}" "${additional_url}"
  else
    local normalized_existing
    normalized_existing="$(normalize_repo_url "${existing_additional_url}")"
    if [[ "${normalized_existing}" != "${ADDITIONAL_REPOSITORY}" ]]; then
      log "Remote ${additional_remote} points to ${normalized_existing}; updating to ${ADDITIONAL_REPOSITORY}."
      git -C "${repo_dir}" remote set-url "${additional_remote}" "${additional_url}"
    fi
  fi

  # Fetch the branch
  log "Fetching ${ADDITIONAL_BRANCH} from ${ADDITIONAL_REPOSITORY}..."
  if ! retry git -C "${repo_dir}" fetch "${additional_remote}" "${ADDITIONAL_BRANCH}" 2>&1; then
    die "Failed to fetch ${ADDITIONAL_BRANCH} from ${ADDITIONAL_REPOSITORY}"
  fi

  # Merge the branch (same resolution approach as PR merges: bank + commuting-merge)
  log "Merging ${additional_remote}/${ADDITIONAL_BRANCH}..."
  local additional_sha
  additional_sha="$(git -C "${repo_dir}" rev-parse "${additional_remote}/${ADDITIONAL_BRANCH}" 2>/dev/null || true)"
  local resol_log="${RESOLUTION_STATS_DIR}/additional.tsv"
  : > "${resol_log}"
  if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge --resolution-log "${resol_log}" "${TARGET_BRANCH}" "${additional_remote}/${ADDITIONAL_BRANCH}") 2>&1; then
    dump_conflict_report \
      "${repo_dir}" \
      "${ADDITIONAL_REPOSITORY//\//_}-${ADDITIONAL_BRANCH//\//_}" \
      "additional-branch" \
      "${ADDITIONAL_REPOSITORY}" \
      "${ADDITIONAL_BRANCH}${additional_sha:+@${additional_sha}}" \
      "https://github.com/${ADDITIONAL_REPOSITORY}/tree/${ADDITIONAL_BRANCH}"
    git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    log "Merge conflict with additional repository. Aborting."
    exit 1
  fi
  if git -C "${repo_dir}" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1; then
    git -C "${repo_dir}" commit -m "Merge ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}"
  else
    log "Additional branch already up to date; no merge commit created."
  fi

  ADDITIONAL_MERGE_COMMIT="$(git -C "${repo_dir}" rev-parse "${additional_remote}/${ADDITIONAL_BRANCH}")"
  export ADDITIONAL_MERGE_COMMIT
  emit_output ADDITIONAL_MERGE_COMMIT "${ADDITIONAL_MERGE_COMMIT}"
  log "Successfully merged ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH} (${ADDITIONAL_MERGE_COMMIT})"

  # Update BASE_COMMIT to current HEAD so subsequent steps preserve the additional merge
  BASE_COMMIT="$(git -C "${repo_dir}" rev-parse HEAD)"
  export BASE_COMMIT
  emit_output BASE_COMMIT "${BASE_COMMIT}"
  log "Updated BASE_COMMIT to include additional merge: ${BASE_COMMIT}"
}

create_manifest() {
  local repo_dir="$1"
  local template_file="${MANIFEST_TEMPLATE:-${PROJECT_ROOT}/.github/templates/staging-manifest.yaml.template}"
  local manifest_file="${repo_dir}/.staging-manifest.yaml"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  step "Create staging manifest"
  [[ -f "${template_file}" ]] || die "template not found: ${template_file}"

  cp "${template_file}" "${manifest_file}"
  sed -i "s|{{TIMESTAMP}}|${timestamp}|g" "${manifest_file}"
  sed -i "s|{{TARGET_REPO}}|${TARGET_REPO}|g" "${manifest_file}"
  sed -i "s|{{TARGET_BRANCH}}|${TARGET_BRANCH}|g" "${manifest_file}"
  sed -i "s|{{DATED_BRANCH}}|${DATED_BRANCH:-N/A}|g" "${manifest_file}"
  sed -i "s|{{BASE_REPO}}|${BASE_REPO}|g" "${manifest_file}"
  sed -i "s|{{BASE_BRANCH}}|${BASE_BRANCH}|g" "${manifest_file}"
  sed -i "s|{{BASE_COMMIT}}|${BASE_COMMIT}|g" "${manifest_file}"

  # Populate additional_merge section
  local additional_section
  if [[ -n "${ADDITIONAL_REPOSITORY}" && -n "${ADDITIONAL_BRANCH}" ]]; then
    additional_section="  repository: \"${ADDITIONAL_REPOSITORY}\"\n  branch: \"${ADDITIONAL_BRANCH}\"\n  commit: \"${ADDITIONAL_MERGE_COMMIT:-unknown}\"\n  url: \"https://github.com/${ADDITIONAL_REPOSITORY}/tree/${ADDITIONAL_MERGE_COMMIT:-${ADDITIONAL_BRANCH}}\""
  else
    additional_section="  null  # No additional repository merged"
  fi
  sed -i "s|{{ADDITIONAL_MERGE_SECTION}}|${additional_section}|g" "${manifest_file}"
  # Convert escaped newlines to actual newlines
  sed -i 's/\\n/\n/g' "${manifest_file}"

  if [[ -n "${MERGED_PRS}" ]]; then
    for pr_num in ${MERGED_PRS}; do
      local pr_commit pr_title pr_author pr_title_escaped
      # Fetch PR details from GitHub API (PR_SHA array may be empty in step-by-step CI mode)
      pr_commit="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json headRefOid --jq '.headRefOid' 2>/dev/null || echo "${PR_SHA[$pr_num]:-unknown}")"
      pr_title="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json title --jq '.title' 2>/dev/null || echo "N/A")"
      pr_author="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json author --jq '.author.login' 2>/dev/null || echo "N/A")"
      pr_title_escaped="$(echo "${pr_title}" | sed 's/"/\\"/g')"

      {
        echo "  - number: ${pr_num}"
        echo "    commit: \"${pr_commit}\""
        echo "    author: \"${pr_author}\""
        echo "    title: \"${pr_title_escaped}\""
        echo "    url: \"https://github.com/${BASE_REPO}/pull/${pr_num}\""
      } >> "${manifest_file}"
    done
  else
    echo "  []  # No PRs merged" >> "${manifest_file}"
  fi

  git -C "${repo_dir}" add "${manifest_file}"
  git -C "${repo_dir}" commit -m "${timestamp}" -m "Staging branch manifest for ${DATED_BRANCH:-${TARGET_BRANCH}}"
  log "Manifest committed."
}

push_branches() {
  local repo_dir="$1"
  local force="${FORCE_PUSH}"

  if [[ "${MODE}" == "local" ]]; then
    log "Local mode: skipping push to remote."
    return 0
  fi

  if ! git -C "${repo_dir}" remote get-url origin >/dev/null 2>&1; then
    die "origin remote not set for ${repo_dir}"
  fi

  if [[ "${force}" == "true" ]]; then
    step "Force push ${TARGET_BRANCH} and ${DATED_BRANCH}"
    log "Force pushing ${TARGET_BRANCH} and ${DATED_BRANCH}..."
    retry git -C "${repo_dir}" push origin "HEAD:${TARGET_BRANCH}" "HEAD:${DATED_BRANCH}" --force
  else
    step "Push ${TARGET_BRANCH} and ${DATED_BRANCH}"
    log "Pushing ${TARGET_BRANCH} and ${DATED_BRANCH}..."
    retry git -C "${repo_dir}" push origin "HEAD:${TARGET_BRANCH}" "HEAD:${DATED_BRANCH}"
  fi
}


# Summarize resolution stats from a TSV log file.
# Outputs: "N hunks (B bank, C commute, U unresolved)" or empty if no conflicts.
format_resolution_stats() {
  local stats_file="$1"
  if [[ ! -s "${stats_file}" ]]; then
    return
  fi
  local total=0 bank=0 commute=0
  while IFS=$'\t' read -r _path file_total file_bank file_commute; do
    total=$((total + file_total))
    bank=$((bank + file_bank))
    commute=$((commute + file_commute))
  done < "${stats_file}"
  local unresolved=$((total - bank - commute))
  local parts=()
  if [[ ${bank} -gt 0 ]]; then
    parts+=("${bank} bank")
  fi
  if [[ ${commute} -gt 0 ]]; then
    parts+=("${commute} commute")
  fi
  if [[ ${unresolved} -gt 0 ]]; then
    parts+=("${unresolved} unresolved")
  fi
  local detail=""
  if [[ ${#parts[@]} -gt 0 ]]; then
    detail="$(IFS=', '; echo "${parts[*]}")"
  fi
  echo "${total} hunks (${detail})"
}

print_final_report() {
  step "Staging Branch Report"
  log ""
  log "Base: ${BASE_REPO}/${BASE_BRANCH} (${BASE_COMMIT})"
  if [[ -n "${ADDITIONAL_REPOSITORY}" && -n "${ADDITIONAL_BRANCH}" ]]; then
    log "Additional: ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH} (${ADDITIONAL_MERGE_COMMIT:-N/A})"
    local additional_stats
    additional_stats="$(format_resolution_stats "${RESOLUTION_STATS_DIR}/additional.tsv")"
    if [[ -n "${additional_stats}" ]]; then
      log "  Conflicts: ${additional_stats}"
    fi
  fi
  log ""

  if [[ -n "${MERGED_PRS}" ]]; then
    log "Merged PRs ($(echo "${MERGED_PRS}" | wc -w | xargs)):"
    for pr_num in ${MERGED_PRS}; do
      local pr_title
      pr_title="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json title --jq '.title' 2>/dev/null || echo "N/A")"
      log "  #${pr_num}: ${pr_title}"
      log "    https://github.com/${BASE_REPO}/pull/${pr_num}"
      local pr_stats
      pr_stats="$(format_resolution_stats "${RESOLUTION_STATS_DIR}/PR-${pr_num}.tsv")"
      if [[ -n "${pr_stats}" ]]; then
        log "    Conflicts: ${pr_stats}"
      else
        log "    Conflicts: none"
      fi
    done
  fi

  if [[ -n "${SKIPPED_PRS}" ]]; then
    log ""
    log "Skipped PRs - conflicting ($(echo "${SKIPPED_PRS}" | wc -w | xargs)):"
    for pr_num in ${SKIPPED_PRS}; do
      local pr_title
      pr_title="$(gh pr view "${pr_num}" --repo "${BASE_REPO}" --json title --jq '.title' 2>/dev/null || echo "N/A")"
      log "  #${pr_num}: ${pr_title}"
      log "    https://github.com/${BASE_REPO}/pull/${pr_num}"
      local pr_stats
      pr_stats="$(format_resolution_stats "${RESOLUTION_STATS_DIR}/PR-${pr_num}.tsv")"
      if [[ -n "${pr_stats}" ]]; then
        log "    Conflicts: ${pr_stats}"
      fi
    done
    if [[ -n "${CONFLICT_REPORT_DIR}" ]]; then
      log "  Conflict reports: ${CONFLICT_REPORT_DIR}"
    fi
  fi

  # Bank usage summary
  if [[ -d "${PROJECT_ROOT}/resolutions.d/contents" ]]; then
    local bank_total used_count unused_count
    bank_total="$(ls "${PROJECT_ROOT}/resolutions.d/contents" 2>/dev/null | wc -l | xargs)"
    if [[ ${bank_total} -gt 0 ]]; then
      # Collect all used keys across every .keys file
      local all_used_keys_file="${RESOLUTION_STATS_DIR}/all_used.keys"
      cat "${RESOLUTION_STATS_DIR}"/*.keys 2>/dev/null | sort -u > "${all_used_keys_file}" || true
      used_count="$(wc -l < "${all_used_keys_file}" | xargs)"
      unused_count=$((bank_total - used_count))
      log ""
      log "Resolution bank: ${used_count} used, ${unused_count} unused, ${bank_total} total"
      if [[ ${unused_count} -gt 0 ]]; then
        # List unused keys (show filepath from .resol metadata if possible)
        local bank_key
        while IFS= read -r bank_key; do
          [[ -z "${bank_key}" ]] && continue
          if ! grep -qFx "${bank_key}" "${all_used_keys_file}" 2>/dev/null; then
            log "  unused: ${bank_key:0:32}..."
          fi
        done < <(ls "${PROJECT_ROOT}/resolutions.d/contents")
      fi
    fi
  fi
  log ""
}

purge_unused_resolutions() {
  if [[ "${PURGE_UNUSED_RESOLUTIONS}" != "true" ]]; then
    return 0
  fi
  local contents_dir="${PROJECT_ROOT}/resolutions.d/contents"
  if [[ ! -d "${contents_dir}" ]]; then
    return 0
  fi
  local all_used_keys_file="${RESOLUTION_STATS_DIR}/all_used.keys"
  if [[ ! -s "${all_used_keys_file}" ]]; then
    log "No used keys recorded — skipping purge (would delete everything)."
    return 0
  fi

  local purged=0
  local bank_key
  for bank_key in "${contents_dir}"/*; do
    [[ -f "${bank_key}" ]] || continue
    local key_name
    key_name="$(basename "${bank_key}")"
    if ! grep -qFx "${key_name}" "${all_used_keys_file}" 2>/dev/null; then
      rm "${bank_key}"
      purged=$((purged + 1))
    fi
  done

  if [[ ${purged} -gt 0 ]]; then
    log "Purged ${purged} unused resolution(s) from bank."
    # Clean up .resol files that reference only purged keys
    local f
    while IFS= read -r f; do
      [[ -z "${f}" ]] && continue
      [[ -f "${f}" ]] || continue
      local has_live="false"
      local key
      while IFS= read -r key; do
        [[ -z "${key}" ]] && continue
        if [[ -f "${contents_dir}/${key}" ]]; then
          has_live="true"
          break
        fi
      done < <(python3 -c "
import json, sys
with open('${f}') as fh:
    data = json.load(fh)
for fi in data.get('files', []):
    for k in fi.get('hunk_keys', []):
        print(k)
" 2>/dev/null || true)
      if [[ "${has_live}" == "false" ]]; then
        log "  Removing empty .resol: $(basename "${f}")"
        rm "${f}"
      fi
    done < <(compgen -G "${PROJECT_ROOT}/resolutions.d/*.resol" || true)
  else
    log "No unused resolutions to purge."
  fi
}

init_repo() {
  init_target_repo
  if [[ -z "${TARGET_REPO}" ]]; then
    die "Could not determine target repository from origin remote."
  fi
  ensure_sibling_layout "${WORK_DIR}"
  setup_git_config "${WORK_DIR}"
  setup_remotes "${WORK_DIR}"
  set_dated_branch
}

maybe_confirm_reset() {
  if [[ "${USE_LOCAL_PATH}" == "true" ]]; then
    if [[ "${MODE}" == "local" ]]; then
      if [[ ! -t 0 ]]; then
        die "Confirmation required to reset ${TARGET_BRANCH} but no TTY available."
      fi

      # Warn about dirty state
      local dirty_files
      dirty_files="$(git -C "${WORK_DIR}" status --porcelain 2>/dev/null || true)"
      if [[ -n "${dirty_files}" ]]; then
        log ""
        log "WARNING: The repository at ${WORK_DIR} has uncommitted changes:"
        log "${dirty_files}"
        log ""
        log "These changes WILL BE LOST. The script performs hard resets and cleans untracked files."
        log "Consider running 'git stash' first to preserve your work."
        log ""
      fi

      log "About to reset ${TARGET_BRANCH} to ${BASE_REPO}/${BASE_BRANCH} in ${WORK_DIR}."
      log "This will DESTRUCTIVELY modify the repository (checkout -B, reset --hard, clean -fd)."
      read -r -p "Continue? [y/N] " confirm
      if [[ ! "${confirm}" =~ ^[Yy]$ ]]; then
        die "Aborted by user."
      fi
    else
      log "CI mode: auto-confirming reset of ${TARGET_BRANCH}."
    fi
  fi
}

main() {
  require_cmd git
  require_cmd gh
  require_cmd python3
  parse_args "$@"
  ensure_repo_inputs
  init_repo

  # Per-merge resolution stats (one file per merge key, containing TSV lines)
  RESOLUTION_STATS_DIR="$(mktemp -d)"
  trap 'rm -rf "${RESOLUTION_STATS_DIR}"' EXIT

  if [[ -n "${STEP_NAME}" && "${STEP_NAME}" != "all" ]]; then
    case "${STEP_NAME}" in
      reset)
        maybe_confirm_reset
        reset_target_branch "${WORK_DIR}"
        ;;
      fetch-prs)
        fetch_pr_list
        ;;
      test-merge)
        require_env_var PR_LIST
        require_env_var BASE_COMMIT
        test_merge_compatibility "${WORK_DIR}" "${PR_LIST}"
        ;;
      test-pairwise)
        require_env_var PR_LIST
        require_env_var BASE_COMMIT
        test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}"
        ;;
      merge)
        require_env_var PR_LIST
        merge_prs "${WORK_DIR}" "${PR_LIST}"
        ;;
      merge-additional)
        merge_additional_repository "${WORK_DIR}"
        ;;
      manifest)
        require_env_var MERGED_PRS
        create_manifest "${WORK_DIR}"
        ;;
      push)
        push_branches "${WORK_DIR}"
        ;;
      *)
        die "Unknown step: ${STEP_NAME}"
        ;;
    esac
    log "Done."
    return 0
  fi

  maybe_confirm_reset
  reset_target_branch "${WORK_DIR}"
  merge_additional_repository "${WORK_DIR}"
  fetch_pr_list
  test_merge_compatibility "${WORK_DIR}" "${PR_LIST}"
  test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}"
  merge_prs "${WORK_DIR}" "${PR_LIST}"
  create_manifest "${WORK_DIR}"
  push_branches "${WORK_DIR}"
  print_final_report
  purge_unused_resolutions
  log "Done."
}

main "$@"
