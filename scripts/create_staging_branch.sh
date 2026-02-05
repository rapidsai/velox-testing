#!/usr/bin/env bash
set -euo pipefail

# Staging Branch Creator
# Run with --help for usage and examples.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BASE_REPO="facebookincubator/velox"
BASE_BRANCH="main"
TARGET_REPO=""
TARGET_BRANCH="staging"
TARGET_PATH=""
WORK_DIR="velox"
AUTO_FETCH_PRS="true"
MANUAL_PR_NUMBERS=""
PR_LABELS="cudf"
MANIFEST_TEMPLATE=""
FORCE_PUSH="false"
ADDITIONAL_REPOSITORY=""
ADDITIONAL_BRANCH=""
GH_TOKEN="${GH_TOKEN:-}"
USE_LOCAL_PATH="false"
MODE="local"
STEP_NAME=""
declare -A PR_SHA

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

Required:
  --target-path /path/to/repo      Local path to target repository

Options:
  --base-repository owner/repo     Base repository (default: ${BASE_REPO})
  --base-branch branch             Base branch (default: ${BASE_BRANCH})
  --target-branch branch           Target branch (default: ${TARGET_BRANCH})
  --work-dir path                  Directory to clone target repo (default: ${WORK_DIR})
  --auto-fetch-prs true|false      Auto-fetch non-draft PRs with label (default: ${AUTO_FETCH_PRS})
  --manual-pr-numbers "1,2,3"      Comma-separated PR numbers to merge (disables auto-fetch)
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
  export BASE_COMMIT
  emit_output BASE_COMMIT "${BASE_COMMIT}"
  log "Base commit: ${BASE_COMMIT}"
}

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

test_merge_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local successful=""
  local conflicts=""

  step "Testing merge compatibility against ${BASE_REPO}/${BASE_BRANCH}"
  for pr_num in ${pr_list}; do
    divider "PR #${pr_num}"
    fetch_pr_head "${repo_dir}" "${pr_num}"
    if git -C "${repo_dir}" merge --no-commit --no-ff "${PR_SHA[$pr_num]}" >/dev/null 2>&1; then
      successful="${successful} ${pr_num}"
    else
      conflicts="${conflicts} ${pr_num}"
    fi
    git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
    git -C "${repo_dir}" clean -fd >/dev/null
  done

  if [[ -n "$(echo "${conflicts}" | xargs)" ]]; then
    log "Conflicts detected for PRs:${conflicts}"
    for pr_num in ${conflicts}; do
      log "  PR #${pr_num}: https://github.com/${BASE_REPO}/pull/${pr_num}"
    done
    exit 1
  fi
  log "All PRs can merge cleanly with ${BASE_BRANCH}."
}

test_pairwise_compatibility() {
  local repo_dir="$1"
  local pr_list="$2"
  local pr_array=(${pr_list})
  local pr_count="${#pr_array[@]}"
  local successful_pairs=""
  local conflict_pairs=""

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

      if ! git -C "${repo_dir}" merge --no-edit "${PR_SHA[$pr1]}" >/dev/null 2>&1; then
        git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
        git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
        continue
      fi

      if git -C "${repo_dir}" merge --no-commit --no-ff "${PR_SHA[$pr2]}" >/dev/null 2>&1; then
        successful_pairs="${successful_pairs} ${pr1}+${pr2}"
      else
        conflict_pairs="${conflict_pairs} ${pr1}+${pr2}"
      fi

      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
      git -C "${repo_dir}" reset --hard "${BASE_COMMIT}" >/dev/null
      git -C "${repo_dir}" clean -fd >/dev/null
    done
  done

  if [[ -n "$(echo "${conflict_pairs}" | xargs)" ]]; then
    log "Pairwise conflicts detected:${conflict_pairs}"
    exit 1
  fi
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
    if ! git -C "${repo_dir}" merge "${PR_SHA[$pr_num]}" --log -m "Merge PR #${pr_num}: ${pr_title}" 2>&1; then
      log "Merge conflict in PR #${pr_num}. Aborting."
      git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
      exit 1
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
  if ! git -C "${repo_dir}" merge "${additional_remote}/${ADDITIONAL_BRANCH}" --log -m "Merge ${ADDITIONAL_REPOSITORY}/${ADDITIONAL_BRANCH}" 2>&1; then
    log "Merge conflict with additional repository. Aborting."
    git -C "${repo_dir}" merge --abort >/dev/null 2>&1 || true
    exit 1
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
  sed -i "s|{{DATED_BRANCH}}|${DATED_BRANCH}|g" "${manifest_file}"
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
  git -C "${repo_dir}" commit -m "${timestamp}" -m "Staging branch manifest for ${DATED_BRANCH}"
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
      log "About to reset ${TARGET_BRANCH} to ${BASE_REPO}/${BASE_BRANCH} in ${WORK_DIR}."
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
  log "Done."
}

main "$@"
