#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Staging Branch Creator
# Run with --help for usage and examples.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MERGE_COMMUTE_CMD=(python3 "${PROJECT_ROOT}/scripts/merge_commute.py" --strict-commute)

BASE_REPO=""
BASE_BRANCH=""
TARGET_REPO=""
TARGET_BRANCH="staging"
TARGET_PATH=""
WORK_DIR=""
AUTO_FETCH_PRS="true"
MANUAL_PR_NUMBERS=""
EXCLUDE_PR_NUMBERS=""
PR_LABELS=""
MANIFEST_TEMPLATE=""
FORCE_PUSH="false"
ADDITIONAL_REPOSITORY=""
ADDITIONAL_BRANCH=""
GH_TOKEN="${GH_TOKEN:-}"
USE_LOCAL_PATH="false"
MODE="local"
STEP_NAME=""
MERGED_BASES_BRANCH="merged_bases"
declare -A PR_SHA

# Log messages to stderr.
log() { echo "$@" >&2; }
# Print error and exit.
die() { log "ERROR: $*"; exit 1; }
STEP=0
# Emit a numbered step header.
step() {
  STEP=$((STEP + 1))
  log "==== [${STEP}] $* ===="
}
# Emit a section divider.
divider() {
  log "---- $* ----"
}
# Normalize git URL to owner/repo.
normalize_repo_url() {
  local url="$1"
  echo "${url}" | awk -F'[:/]' '{print $(NF-1)"/"$NF}' | sed 's/\.git$//'
}

# Ensure repo layout has velox-testing sibling.
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
# Emit outputs to GitHub workflow env files.
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

# Require an env var to be set.
require_env_var() {
  local name="$1"
  local value="${!name:-}"
  [[ -n "${value}" ]] || die "Missing ${name}. Run the appropriate prior step to set it."
}

# Retry a command with backoff.
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

# Print script usage.
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
  --pr-labels labels               Comma-separated PR labels to auto-fetch (default: ${PR_LABELS})
  --manifest-template path         Manifest template path (default: repo template)
  --force-push true|false          Force push to target branch (default: ${FORCE_PUSH})
  --additional-repository repo     Additional repository to merge from (e.g., rapidsai/cudf)
  --additional-branch branch       Branch from additional repository to merge
  --mode local|ci                  Execution mode (default: ${MODE})
  --step name                      Run a single step (see below)
  -h, --help                       Show this help

Environment:
  GH_TOKEN                         GitHub token for cloning/pushing and gh API calls

Steps (for --step option):
  # reset, fetch-prs, test-bases, test-merge, test-merge-merged-bases, test-pairwise,
  # test-pairwise-merged-bases, merge, merge-additional, manifest, push, all

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
EOF
}

# Ensure a command exists in PATH.
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

# Parse CLI arguments.
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
      --pr-labels) PR_LABELS="$2"; shift 2 ;;
      --manifest-template) MANIFEST_TEMPLATE="$2"; shift 2 ;;
      --force-push) FORCE_PUSH="$2"; shift 2 ;;
      --additional-repository) ADDITIONAL_REPOSITORY="$2"; shift 2 ;;
      --additional-branch) ADDITIONAL_BRANCH="$2"; shift 2 ;;
      --mode) MODE="$2"; shift 2 ;;
      --step) STEP_NAME="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option: $1" ;;
    esac
  done
}

# Validate required repo inputs.
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

# Ensure git user config exists.
setup_git_config() {
  local repo_dir="$1"
  if ! git -C "${repo_dir}" config user.name >/dev/null 2>&1; then
    git -C "${repo_dir}" config user.name "velox-staging-bot"
  fi
  if ! git -C "${repo_dir}" config user.email >/dev/null 2>&1; then
    git -C "${repo_dir}" config user.email "velox-staging-bot@users.noreply.github.com"
  fi
}

# Initialize and validate target repo path.
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

# Configure upstream remotes and fetch.
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

# Set dated branch name for CI mode.
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

# Reset target branch to base branch.
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
  export BASE_COMMIT
  emit_output BASE_COMMIT "${BASE_COMMIT}"
  log "Base commit: ${BASE_COMMIT}"
}

# Build PR list from labels or manual list.
fetch_pr_list() {
  local pr_list=""
  if [[ "${AUTO_FETCH_PRS}" == "true" ]]; then
    step "Auto-fetch PRs with labels: ${PR_LABELS}"
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
      --json number,isDraft \
      --jq '.[] | select(.isDraft == false) | .number' | tr '\n' ' ' | xargs || true)"
  else
    pr_list="$(echo "${MANUAL_PR_NUMBERS}" | tr ',' ' ' | xargs || true)"
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

# Fetch PR head SHA for a PR number.
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

# List files with merge conflicts.
list_conflict_files() {
  local repo_dir="$1"
  git -C "${repo_dir}" diff --name-only --diff-filter=U | xargs || true
}

# Create merged bases branch if possible.
ensure_merged_bases_branch() {
  local repo_dir="$1"
  step "Testing base branch compatibility"

  if [[ -z "${ADDITIONAL_REPOSITORY}" || -z "${ADDITIONAL_BRANCH}" ]]; then
    log "No additional base branch configured; ${MERGED_BASES_BRANCH} matches ${TARGET_BRANCH}."
    git -C "${repo_dir}" checkout -B "${MERGED_BASES_BRANCH}" "${TARGET_BRANCH}" >/dev/null 2>&1
    git -C "${repo_dir}" checkout "${TARGET_BRANCH}" >/dev/null 2>&1
    MERGED_BASES_READY="false"
    export MERGED_BASES_READY
    emit_output MERGED_BASES_READY "${MERGED_BASES_READY}"
    return 0
  fi

  local additional_remote="additional-merge-source"
  local additional_url="https://github.com/${ADDITIONAL_REPOSITORY}.git"
  if ! git -C "${repo_dir}" remote get-url "${additional_remote}" >/dev/null 2>&1; then
    git -C "${repo_dir}" remote add "${additional_remote}" "${additional_url}"
  fi
  log "Fetching ${ADDITIONAL_BRANCH} from ${ADDITIONAL_REPOSITORY} for merged bases..."
  if ! git -C "${repo_dir}" fetch "${additional_remote}" "${ADDITIONAL_BRANCH}" >/dev/null 2>&1; then
    log "Failed to fetch ${ADDITIONAL_BRANCH} for merged bases."
    return 1
  fi

  git -C "${repo_dir}" checkout -B "${MERGED_BASES_BRANCH}" "${TARGET_BRANCH}" >/dev/null 2>&1
  log "Merging base branches into ${MERGED_BASES_BRANCH}..."
  if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge "${MERGED_BASES_BRANCH}" "${additional_remote}/${ADDITIONAL_BRANCH}") >/dev/null 2>&1; then
    local conflict_files
    conflict_files="$(list_conflict_files "${repo_dir}")"
    if [[ -n "${conflict_files}" ]]; then
      log "Conflicting files for base branches: ${conflict_files}"
    fi
    git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    git -C "${repo_dir}" reset --hard "${TARGET_BRANCH}" >/dev/null 2>&1
    git -C "${repo_dir}" checkout "${TARGET_BRANCH}" >/dev/null 2>&1
    log "Cannot create ${MERGED_BASES_BRANCH} branch."
    MERGED_BASES_READY="false"
    export MERGED_BASES_READY
    emit_output MERGED_BASES_READY "${MERGED_BASES_READY}"
    return 1
  fi

  if git -C "${repo_dir}" rev-parse -q --verify MERGE_HEAD >/dev/null 2>&1; then
    git -C "${repo_dir}" commit -m "Merge base branches: ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}" >/dev/null 2>&1
  fi
  git -C "${repo_dir}" checkout "${TARGET_BRANCH}" >/dev/null 2>&1
  log "Created ${MERGED_BASES_BRANCH} branch."
  MERGED_BASES_READY="true"
  export MERGED_BASES_READY
  emit_output MERGED_BASES_READY "${MERGED_BASES_READY}"
  return 0
}


# Test PR merge compatibility against a base ref.
test_merge_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local base_ref="$3"
  local label="$4"
  local base_commit
  base_commit="$(git -C "${repo_dir}" rev-parse "${base_ref}")"
  local successful=""
  local conflicts=""

  step "Testing merge compatibility against ${label}"
  for pr_num in ${pr_list}; do
    divider "PR #${pr_num}"
    fetch_pr_head "${repo_dir}" "${pr_num}"
    if (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge "${base_ref}" "${PR_SHA[$pr_num]}") >/dev/null 2>&1; then
      successful="${successful} ${pr_num}"
    else
      local conflict_files
      conflict_files="$(list_conflict_files "${repo_dir}")"
      if [[ -n "${conflict_files}" ]]; then
        log "Conflicting files for PR #${pr_num}: ${conflict_files}"
      fi
      conflicts="${conflicts} ${pr_num}"
    fi
    git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    git -C "${repo_dir}" reset --hard "${base_commit}" >/dev/null
    git -C "${repo_dir}" clean -fd >/dev/null
  done

  if [[ -n "$(echo "${conflicts}" | xargs)" ]]; then
    log "Conflicts detected for PRs:${conflicts}"
    for pr_num in ${conflicts}; do
      log "  PR #${pr_num}: https://github.com/${BASE_REPO}/pull/${pr_num}"
    done
    exit 1
  fi
  log "All PRs can merge cleanly with ${label}."
}

# Build pairwise compatibility matrix against a base.
test_pairwise_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local base_ref="$3"
  local label="$4"
  local base_commit
  base_commit="$(git -C "${repo_dir}" rev-parse "${base_ref}")"
  local pr_array=(${pr_list})
  local pr_count="${#pr_array[@]}"
  local conflict_pairs=""
  declare -A pair_results  # Store results for matrix display
  declare -A conflict_prs  # Track PRs involved in conflicts

  if [[ "${pr_count}" -lt 2 ]]; then
    return 0
  fi

  step "Testing pairwise merge compatibility against ${label}"
  for ((i=0; i<pr_count; i++)); do
    for ((j=i+1; j<pr_count; j++)); do
      local pr1="${pr_array[$i]}"
      local pr2="${pr_array[$j]}"
      fetch_pr_head "${repo_dir}" "${pr1}"
      fetch_pr_head "${repo_dir}" "${pr2}"
      git -C "${repo_dir}" reset --hard "${base_commit}" >/dev/null
      git -C "${repo_dir}" clean -fd >/dev/null

      if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --auto-continue --keep-merge "${base_ref}" "${PR_SHA[$pr1]}") >/dev/null 2>&1; then
        local conflict_files
        conflict_files="$(list_conflict_files "${repo_dir}")"
        if [[ -n "${conflict_files}" ]]; then
          log "Conflicting files for PR #${pr1}: ${conflict_files}"
        fi
        git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
        git -C "${repo_dir}" reset --hard "${base_commit}" >/dev/null
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
        pair_results["${pr1},${pr2}"]="XX"
        conflict_prs["${pr1}"]=1
        conflict_prs["${pr2}"]=1
        continue
      fi

      if (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" --keep-merge "${base_ref}" "${PR_SHA[$pr2]}") >/dev/null 2>&1; then
        pair_results["${pr1},${pr2}"]="OK"
      else
        local conflict_files
        conflict_files="$(list_conflict_files "${repo_dir}")"
        if [[ -n "${conflict_files}" ]]; then
          log "Conflicting files for PR #${pr1} + #${pr2}: ${conflict_files}"
        fi
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
        pair_results["${pr1},${pr2}"]="XX"
        conflict_prs["${pr1}"]=1
        conflict_prs["${pr2}"]=1
      fi

      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
      git -C "${repo_dir}" reset --hard "${base_commit}" >/dev/null
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

# Merge PRs into target branch.
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
    if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" "${TARGET_BRANCH}" "${PR_SHA[$pr_num]}") 2>&1; then
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

# Merge additional repository branch into target.
merge_additional_repository() {
  local repo_dir="$1"

  if [[ -z "${ADDITIONAL_REPOSITORY}" || -z "${ADDITIONAL_BRANCH}" ]]; then
    log "No additional repository configured, skipping."
    return 0
  fi

  step "Merging additional repository: ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}"

  local additional_remote="additional-merge-source"
  local additional_url="https://github.com/${ADDITIONAL_REPOSITORY}.git"

  # Add remote if not exists
  if ! git -C "${repo_dir}" remote get-url "${additional_remote}" >/dev/null 2>&1; then
    git -C "${repo_dir}" remote add "${additional_remote}" "${additional_url}"
  fi

  # Fetch the branch
  log "Fetching ${ADDITIONAL_BRANCH} from ${ADDITIONAL_REPOSITORY}..."
  if ! git -C "${repo_dir}" fetch "${additional_remote}" "${ADDITIONAL_BRANCH}" 2>&1; then
    die "Failed to fetch ${ADDITIONAL_BRANCH} from ${ADDITIONAL_REPOSITORY}"
  fi

  # Merge the branch
  log "Merging ${additional_remote}/${ADDITIONAL_BRANCH}..."
  if ! (cd "${repo_dir}" && "${MERGE_COMMUTE_CMD[@]}" "${TARGET_BRANCH}" "${additional_remote}/${ADDITIONAL_BRANCH}") 2>&1; then
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

# Create and commit staging manifest.
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

# Push target and dated branches.
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


# Initialize repo, remotes, and dated branch.
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

# Confirm destructive reset in local mode.
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

# Orchestrate the full staging flow.
main() {
  require_cmd git
  require_cmd gh
  require_cmd python3
  parse_args "$@"
  ensure_repo_inputs
  init_repo

  if [[ -n "${STEP_NAME}" && "${STEP_NAME}" != "all" ]]; then
    case "${STEP_NAME}" in
      reset)
        maybe_confirm_reset
        reset_target_branch "${WORK_DIR}"
        ;;
      fetch-prs)
        fetch_pr_list
        ;;
      test-bases)
        ensure_merged_bases_branch "${WORK_DIR}" || true
        ;;
      test-merge)
        require_env_var PR_LIST
        require_env_var BASE_COMMIT
        test_merge_compatibility "${WORK_DIR}" "${PR_LIST}" "${TARGET_BRANCH}" "${BASE_REPO}/${BASE_BRANCH}"
        ;;
      test-merge-merged-bases)
        require_env_var PR_LIST
        require_env_var MERGED_BASES_READY
        if [[ "${MERGED_BASES_READY}" == "true" ]]; then
          test_merge_compatibility "${WORK_DIR}" "${PR_LIST}" "${MERGED_BASES_BRANCH}" "${MERGED_BASES_BRANCH}"
        else
          log "Skipping ${MERGED_BASES_BRANCH} PR merge tests: cannot create ${MERGED_BASES_BRANCH} branch."
        fi
        ;;
      test-pairwise)
        require_env_var PR_LIST
        require_env_var BASE_COMMIT
        test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}" "${TARGET_BRANCH}" "${BASE_REPO}/${BASE_BRANCH}"
        ;;
      test-pairwise-merged-bases)
        require_env_var PR_LIST
        require_env_var MERGED_BASES_READY
        if [[ "${MERGED_BASES_READY}" == "true" ]]; then
          test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}" "${MERGED_BASES_BRANCH}" "${MERGED_BASES_BRANCH}"
        else
          log "Skipping ${MERGED_BASES_BRANCH} pairwise tests: cannot create ${MERGED_BASES_BRANCH} branch."
        fi
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
  ensure_merged_bases_branch "${WORK_DIR}" || true
  require_env_var MERGED_BASES_READY
  fetch_pr_list
  test_merge_compatibility "${WORK_DIR}" "${PR_LIST}" "${TARGET_BRANCH}" "${BASE_REPO}/${BASE_BRANCH}"
  if [[ "${MERGED_BASES_READY}" == "true" ]]; then
    test_merge_compatibility "${WORK_DIR}" "${PR_LIST}" "${MERGED_BASES_BRANCH}" "${MERGED_BASES_BRANCH}"
  else
    log "Skipping ${MERGED_BASES_BRANCH} PR merge tests: cannot create ${MERGED_BASES_BRANCH} branch."
  fi
  test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}" "${TARGET_BRANCH}" "${BASE_REPO}/${BASE_BRANCH}"
  if [[ "${MERGED_BASES_READY}" == "true" ]]; then
    test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}" "${MERGED_BASES_BRANCH}" "${MERGED_BASES_BRANCH}"
  else
    log "Skipping ${MERGED_BASES_BRANCH} pairwise tests: cannot create ${MERGED_BASES_BRANCH} branch."
  fi
  merge_additional_repository "${WORK_DIR}"
  merge_prs "${WORK_DIR}" "${PR_LIST}"
  create_manifest "${WORK_DIR}"
  push_branches "${WORK_DIR}"
  log "Done."
}

main "$@"
