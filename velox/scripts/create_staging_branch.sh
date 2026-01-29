#!/usr/bin/env bash
set -euo pipefail

# Examples:
#   # Local fork path
#   ./velox/scripts/create_staging_branch.sh \
#     --target-path ../velox \
#     --base-repository facebookincubator/velox \
#     --base-branch main \
#     --target-branch staging
#
#   # Manual PR list (no auto-fetch)
#   ./velox/scripts/create_staging_branch.sh \
#     --target-repository rapidsai/velox \
#     --auto-fetch-prs false \
#     --manual-pr-numbers "12345 12346 12347"
#
#   # CI mode (no prompt)
#   GH_TOKEN=... ./velox/scripts/create_staging_branch.sh \
#     --mode ci \
#     --target-repository rapidsai/velox \
#     --base-repository facebookincubator/velox \
#     --base-branch main \
#     --target-branch staging
#
#   # Force push
#   ./velox/scripts/create_staging_branch.sh \
#     --target-repository rapidsai/velox \
#     --force-push true
#
#   # Build CPU only
#   ./velox/scripts/create_staging_branch.sh \
#     --target-repository rapidsai/velox \
#     --build-target cpu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BASE_REPO="facebookincubator/velox"
BASE_BRANCH="main"
TARGET_REPO=""
TARGET_BRANCH="staging"
TARGET_PATH=""
WORK_DIR="velox"
AUTO_FETCH_PRS="true"
MANUAL_PR_NUMBERS=""
BUILD_AND_RUN_TESTS="false"
BUILD_TARGET="gpu"
FORCE_PUSH="false"
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
  local velox_dir="$1"
  local parent_dir
  parent_dir="$(cd "${velox_dir}/.." && pwd)"
  local velox_testing_dir="${parent_dir}/velox-testing"

  if [[ ! -d "${velox_dir}" ]]; then
    die "Velox directory not found: ${velox_dir}"
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

Required (one of):
  --target-repository owner/repo   Target GitHub repository (e.g. rapidsai/velox)
  --target-path /path/to/velox     Local path to a Velox fork

Options:
  --base-repository owner/repo     Base repository (default: ${BASE_REPO})
  --base-branch branch             Base branch (default: ${BASE_BRANCH})
  --target-branch branch           Target branch (default: ${TARGET_BRANCH})
  --work-dir path                  Directory to clone target repo (default: ${WORK_DIR})
  --auto-fetch-prs true|false      Auto-fetch non-draft PRs with "cudf" label (default: ${AUTO_FETCH_PRS})
  --manual-pr-numbers "1 2 3"      PR numbers to merge (used when auto-fetch is false)
  --build-and-run-tests true|false Build and run tests after update (default: ${BUILD_AND_RUN_TESTS})
  --build-target all|cpu|gpu       Test build target (default: ${BUILD_TARGET})
  --force-push true|false          Force push to target branch (default: ${FORCE_PUSH})
  --mode local|ci                  Execution mode (default: ${MODE})
  --step name                      Run a single step (see below)
  -h, --help                       Show this help

Env:
  GH_TOKEN                         GitHub token for cloning/pushing and gh API calls
Notes:
  - If --target-path is provided, the script will prompt before resetting
    ${TARGET_BRANCH} to ${BASE_REPO}/${BASE_BRANCH}.
  - Steps: clone, reset, fetch-prs, test-merge, test-pairwise, merge, manifest, push, build, test, all
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --target-repository) TARGET_REPO="$2"; shift 2 ;;
      --target-path) TARGET_PATH="$2"; shift 2 ;;
      --base-repository) BASE_REPO="$2"; shift 2 ;;
      --base-branch) BASE_BRANCH="$2"; shift 2 ;;
      --target-branch) TARGET_BRANCH="$2"; shift 2 ;;
      --work-dir) WORK_DIR="$2"; shift 2 ;;
      --auto-fetch-prs) AUTO_FETCH_PRS="$2"; shift 2 ;;
      --manual-pr-numbers) MANUAL_PR_NUMBERS="$2"; AUTO_FETCH_PRS="false"; shift 2 ;;
      --build-and-run-tests) BUILD_AND_RUN_TESTS="$2"; shift 2 ;;
      --build-target) BUILD_TARGET="$2"; shift 2 ;;
      --force-push) FORCE_PUSH="$2"; shift 2 ;;
      --mode) MODE="$2"; shift 2 ;;
      --step) STEP_NAME="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown option: $1" ;;
    esac
  done
}

ensure_repo_inputs() {
  if [[ -z "${TARGET_REPO}" && -z "${TARGET_PATH}" ]]; then
    die "Must specify --target-repository or --target-path"
  fi
  if [[ -n "${TARGET_REPO}" && -n "${TARGET_PATH}" ]]; then
    die "Specify only one of --target-repository or --target-path"
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
  emit_output base_commit "${BASE_COMMIT}"
  log "Base commit: ${BASE_COMMIT}"
}

fetch_pr_list() {
  local pr_list=""
  if [[ "${AUTO_FETCH_PRS}" == "true" ]]; then
    step "Auto-fetch PRs with 'cudf' label"
    pr_list="$(gh pr list \
      --repo "${BASE_REPO}" \
      --label "cudf" \
      --state open \
      --json number,isDraft \
      --jq '.[] | select(.isDraft == false) | .number' | tr '\n' ' ' | xargs || true)"
  else
    pr_list="$(echo "${MANUAL_PR_NUMBERS}" | xargs || true)"
  fi

  if [[ -z "${pr_list}" ]]; then
    die "No PRs found to merge."
  fi
  PR_LIST="${pr_list}"
  export PR_LIST
  emit_output pr_list "${PR_LIST}"
  emit_output pr_count "$(echo "${PR_LIST}" | wc -w | xargs)"
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
    if ! git -C "${repo_dir}" merge "${PR_SHA[$pr_num]}" --no-edit >/dev/null 2>&1; then
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
  emit_output merged_prs "${MERGED_PRS}"
  emit_output merged_count "${MERGED_COUNT}"
  log "Merged ${MERGED_COUNT} PRs."
}

create_manifest() {
  local repo_dir="$1"
  local template_file="${PROJECT_ROOT}/.github/templates/staging-manifest.yaml.template"
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

  if [[ -n "${MERGED_PRS}" ]]; then
    for pr_num in ${MERGED_PRS}; do
      local pr_commit pr_title pr_author pr_title_escaped
      pr_commit="${PR_SHA[$pr_num]:-unknown}"
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

build_velox() {
  if [[ "${BUILD_AND_RUN_TESTS}" != "true" ]]; then
    log "Skipping build."
    return 0
  fi

  local scripts_dir="${PROJECT_ROOT}/velox/scripts"
  if [[ ! -d "${scripts_dir}" ]]; then
    die "velox scripts directory not found: ${scripts_dir}"
  fi

  step "Build Velox (${BUILD_TARGET})"
  case "${BUILD_TARGET}" in
    cpu)
      (cd "${scripts_dir}" && TREAT_WARNINGS_AS_ERRORS=0 ./build_velox.sh --cpu --log build_velox_cpu.log)
      ;;
    gpu)
      (cd "${scripts_dir}" && TREAT_WARNINGS_AS_ERRORS=0 ./build_velox.sh --gpu --log build_velox_gpu.log)
      ;;
    all)
      (cd "${scripts_dir}" && TREAT_WARNINGS_AS_ERRORS=0 ./build_velox.sh --cpu --log build_velox_cpu.log)
      (cd "${scripts_dir}" && TREAT_WARNINGS_AS_ERRORS=0 ./build_velox.sh --gpu --log build_velox_gpu.log)
      ;;
    *)
      die "invalid build target: ${BUILD_TARGET} (expected: all|cpu|gpu)"
      ;;
  esac
}

run_tests() {
  if [[ "${BUILD_AND_RUN_TESTS}" != "true" ]]; then
    log "Skipping tests."
    return 0
  fi

  local scripts_dir="${PROJECT_ROOT}/velox/scripts"
  if [[ ! -d "${scripts_dir}" ]]; then
    die "velox scripts directory not found: ${scripts_dir}"
  fi

  step "Run Velox tests (${BUILD_TARGET})"
  case "${BUILD_TARGET}" in
    cpu)
      (cd "${scripts_dir}" && ./test_velox.sh --device-type cpu)
      ;;
    gpu)
      (cd "${scripts_dir}" && ./test_velox.sh --device-type gpu)
      ;;
    all)
      (cd "${scripts_dir}" && ./test_velox.sh --device-type cpu)
      (cd "${scripts_dir}" && ./test_velox.sh --device-type gpu)
      ;;
    *)
      die "invalid build target: ${BUILD_TARGET} (expected: all|cpu|gpu)"
      ;;
  esac
}

init_repo() {
  init_target_repo
  if [[ -z "${TARGET_REPO}" ]]; then
    die "Could not determine target repository. Provide --target-repository."
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
      manifest)
        require_env_var MERGED_PRS
        create_manifest "${WORK_DIR}"
        ;;
      push)
        push_branches "${WORK_DIR}"
        ;;
      build)
        build_velox
        ;;
      test)
        run_tests
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
  fetch_pr_list
  test_merge_compatibility "${WORK_DIR}" "${PR_LIST}"
  test_pairwise_compatibility "${WORK_DIR}" "${PR_LIST}"
  merge_prs "${WORK_DIR}" "${PR_LIST}"
  create_manifest "${WORK_DIR}"
  push_branches "${WORK_DIR}"
  build_velox
  run_tests
  log "Done."
}

main "$@"
