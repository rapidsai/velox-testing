#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Separator used to delimit individual failure blocks extracted from CI logs.
BLOCK_SEP="__VELOX_FAILURE_BLOCK__"

# Nightly workflow status helper.
#
# Requirements:
# - gh (GitHub CLI) authenticated (e.g. `gh auth login`)
# - jq
#
# Usage:
#   ./scripts/check_nightly_status.sh [OPTIONS]
#
# Options:
#   --print-logs    Print failed log tails for each failure (default: disabled)
#   --slack         Output in Slack-formatted style with mrkdwn
#   --cause         Use AI to analyze logs and determine failure cause (default: enabled)
#   --no-cause      Disable AI cause analysis
#   --fix           Use AI to suggest a fix, implies --cause (default: enabled)
#   --no-fix        Disable AI fix suggestions
#   --claude        Use Claude CLI for analysis (default: enabled)
#   --no-claude     Use NVIDIA LLM instead of Claude for analysis
#   --date YYYY-MM-DD  Fetch nightly status for a specific date (default: today UTC)
#   -h, --help      Show this help message
#
# If your network is slow, run:
#   GH_HTTP_TIMEOUT=180 GH_RETRIES=8 ./scripts/check_nightly_status.sh
#
# Optional env:
#   REPO=owner/repo              (default: current gh repo)
#   TODAY_UTC=YYYY-MM-DD         (default: today's UTC date)
#   LOG_TAIL_LINES=N             (default: 150) - number of log lines to print
#   STATUS_FILE=path/to/file     (default: status.txt) - output file for status report
#
# AI-powered cause/fix analysis (requires --cause or --fix):
#   LLM_API_KEY or NVIDIA_API_KEY  (required for AI analysis)
#   LLM_API_URL                    (default: https://integrate.api.nvidia.com/v1/chat/completions)
#   LLM_MODEL                      (default: nvdev/nvidia/llama-3.3-nemotron-super-49b-v1)
#   LLM_TIMEOUT                    (default: 30) - timeout in seconds for each LLM API call
#
# Claude AI analysis (requires --cause or --fix with --claude):
#   CLAUDE_BIN                     (default: claude) - path to Claude Code CLI
#   CLAUDE_MODEL                   (default: opus)

# --- Argument parsing ---
PRINT_LOGS="false"
SLACK_FORMAT="true"
ANALYZE_CAUSE="true"
ANALYZE_FIX="true"
USE_CLAUDE="true"

show_help() {
  sed -n '3,/^$/p' "$0" | grep -E '^#' | sed 's/^# \?//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --print-logs)
      PRINT_LOGS="true"
      shift
      ;;
    --slack)
      SLACK_FORMAT="true"
      shift
      ;;
    --cause)
      ANALYZE_CAUSE="true"
      shift
      ;;
    --no-cause)
      ANALYZE_CAUSE="false"
      ANALYZE_FIX="false"
      shift
      ;;
    --fix)
      ANALYZE_FIX="true"
      ANALYZE_CAUSE="true"  # --fix implies --cause
      shift
      ;;
    --no-fix)
      ANALYZE_FIX="false"
      shift
      ;;
    --claude)
      USE_CLAUDE="true"
      shift
      ;;
    --no-claude)
      USE_CLAUDE="false"
      shift
      ;;
    --date)
      TODAY_UTC="$2"
      shift 2
      ;;
    -h|--help)
      show_help
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information." >&2
      exit 1
      ;;
  esac
done
# --- End argument parsing ---

maybe_init_conda_for_tools() {
  # If tools like `gh` are only available in a conda env, allow sourcing our helper.
  # Requires MINIFORGE_HOME to be set (as expected by init_conda).
  local helper_script
  helper_script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/py_env_functions.sh"
  if [[ -f "${helper_script}" && -n "${MINIFORGE_HOME:-}" ]]; then
    # shellcheck disable=SC1090
    source "${helper_script}"
    # init_conda comes from py_env_functions.sh
    init_conda >/dev/null
  fi
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: missing required command: ${cmd}" >&2
    exit 2
  fi
}

# Always initialize conda (gh/jq are expected to be available in that environment).
PY_ENV_HELPER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/py_env_functions.sh"
if [[ ! -f "${PY_ENV_HELPER}" ]]; then
  echo "ERROR: expected helper script not found: ${PY_ENV_HELPER}" >&2
  exit 2
fi
if [[ -z "${MINIFORGE_HOME:-}" ]]; then
  echo "ERROR: MINIFORGE_HOME must be set to initialize conda via scripts/py_env_functions.sh" >&2
  echo "Example: export MINIFORGE_HOME=\"$HOME/miniforge3\"" >&2
  exit 2
fi
# shellcheck disable=SC1090
source "${PY_ENV_HELPER}"
init_conda >/dev/null

# Activate a conda env for this script (default: base).
# You can override:
#   CONDA_ENV_NAME=myenv ./scripts/check_nightly_status.sh
CONDA_ENV_NAME="${CONDA_ENV_NAME:-base}"
if ! command -v conda >/dev/null 2>&1; then
  echo "ERROR: conda is not available even after init_conda." >&2
  exit 2
fi
conda activate "${CONDA_ENV_NAME}" >/dev/null 2>&1 || {
  echo "ERROR: failed to activate conda env: ${CONDA_ENV_NAME}" >&2
  echo "Hint: list envs with: conda env list" >&2
  exit 2
}

# If tools are missing, optionally install them into the active env.
AUTO_INSTALL_CONDA_TOOLS="${AUTO_INSTALL_CONDA_TOOLS:-true}"
ensure_tool() {
  local cmd="$1"
  local pkg="$2"
  if command -v "${cmd}" >/dev/null 2>&1; then
    return 0
  fi
  if [[ "${AUTO_INSTALL_CONDA_TOOLS}" != "true" ]]; then
    return 1
  fi
  echo "Installing missing tool '${cmd}' into conda env '${CONDA_ENV_NAME}'..." >&2
  conda install -y -q -c conda-forge "${pkg}" >/dev/null
}

ensure_tool gh gh || true
ensure_tool jq jq || true

require_cmd gh
require_cmd jq

REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"
if [[ -z "${REPO}" ]]; then
  echo "ERROR: could not determine repo. Set REPO=owner/repo or run within a gh-authenticated repo." >&2
  exit 2
fi

# Get the date from the last Velox Build Upstream nightly job run
# This ensures we report on the actual nightly run date, not today's date
get_last_nightly_run_date() {
  local workflow_file="velox-nightly-upstream.yml"
  local run_date
  run_date=$(gh run list -R "${REPO}" \
    --workflow "${workflow_file}" \
    --limit 1 \
    --json createdAt \
    --jq '.[0].createdAt // empty' 2>/dev/null | cut -d'T' -f1)

  if [[ -n "${run_date}" ]]; then
    echo "${run_date}"
  else
    # Fallback to today if no run found
    date -u +%Y-%m-%d
  fi
}

# Use the date from the last nightly run, or override with TODAY_UTC env var
TODAY_UTC="${TODAY_UTC:-$(get_last_nightly_run_date)}"
DISPLAY_DATE="$(date -d "${TODAY_UTC}" +%d-%m-%Y 2>/dev/null || echo "${TODAY_UTC}")"

# Retry wrapper for flaky GitHub API/network issues (e.g. TLS handshake timeout).
# Controls:
#   GH_RETRIES (default 5)
#   GH_RETRY_SLEEP_SECONDS (default 2) (exponential backoff)
# Also, gh honors GH_HTTP_TIMEOUT for API calls (seconds) in many environments.
GH_RETRIES="${GH_RETRIES:-5}"
GH_RETRY_SLEEP_SECONDS="${GH_RETRY_SLEEP_SECONDS:-2}"
export GH_HTTP_TIMEOUT="${GH_HTTP_TIMEOUT:-60}"

gh_retry() {
  local attempt=1
  local sleep_s="${GH_RETRY_SLEEP_SECONDS}"
  local out rc

  while true; do
    out="$("$@" 2>&1)" && { printf "%s" "${out}"; return 0; }
    rc=$?

    # Retry on common transient network errors.
    if echo "${out}" | grep -qiE "TLS handshake timeout|i/o timeout|timeout|temporarily unavailable|connection reset|EOF"; then
      if [[ "${attempt}" -lt "${GH_RETRIES}" ]]; then
        echo "WARN: transient GitHub API error (attempt ${attempt}/${GH_RETRIES}), retrying in ${sleep_s}s..." >&2
        echo "      ${out}" >&2
        sleep "${sleep_s}"
        attempt=$((attempt + 1))
        sleep_s=$((sleep_s * 2))
        continue
      fi
    fi

    # Non-retryable or out of retries.
    echo "${out}" >&2
    return "${rc}"
  done
}

# Return JSON of the latest run created today (UTC) for a workflow file, or "null".
get_today_run_json() {
  local workflow_file="$1"
  # Note: Some nightlies may be created late (or rerun) and still be "today" for you even if
  # createdAt is yesterday UTC. We include startedAt/updatedAt in the match.
  gh_retry gh run list -R "${REPO}" \
    --workflow "${workflow_file}" \
    --limit 100 \
    --json databaseId,attempt,createdAt,startedAt,updatedAt,conclusion,status,url,workflowName,displayTitle \
    | jq -c --arg today "${TODAY_UTC}" '
        [ .[]
          | select(
              ((.createdAt // "") | startswith($today)) or
              ((.startedAt // "") | startswith($today)) or
              ((.updatedAt // "") | startswith($today))
            )
        ]
        | sort_by(.createdAt, (.attempt // 1))
        | (last // null)
      '
}

get_latest_run_json() {
  local workflow_file="$1"
  gh_retry gh run list -R "${REPO}" \
    --workflow "${workflow_file}" \
    --limit 100 \
    --json databaseId,attempt,createdAt,startedAt,updatedAt,conclusion,status,url,workflowName,displayTitle,number \
    | jq -c '
        [ .[] ] | sort_by(.createdAt, (.attempt // 1)) | (last // null)
      '
}

get_first_attempt_for_run_number_json() {
  local workflow_file="$1"
  local run_number="$2"
  gh_retry gh run list -R "${REPO}" \
    --workflow "${workflow_file}" \
    --limit 200 \
    --json databaseId,attempt,createdAt,startedAt,updatedAt,conclusion,status,url,workflowName,displayTitle,number \
    | jq -c --argjson n "${run_number}" '
        [ .[] | select(.number == $n and (.attempt // 1) == 1) ]
        | sort_by(.createdAt)
        | (last // null)
      '
}

cell_for_run() {
  # stdin: run JSON or "null"
  # Emoji definitions using printf with octal/hex codes for reliable encoding
  local EMOJI_DASH=$'\xe2\x9e\x96'      # ➖ heavy minus sign
  local EMOJI_HOURGLASS=$'\xe2\x8f\xb3' # ⏳ hourglass
  local EMOJI_CHECK=$'\xe2\x9c\x85'     # ✅ check mark
  local EMOJI_CROSS=$'\xe2\x9d\x8c'     # ❌ cross mark

  local run_json
  run_json="$(cat)"
  if [[ "${run_json}" == "null" ]]; then
    echo "${EMOJI_DASH}"
    return 0
  fi
  local status conclusion
  status="$(jq -r '.status // ""' <<<"${run_json}")"
  conclusion="$(jq -r '.conclusion // ""' <<<"${run_json}")"

  if [[ "${status}" != "completed" ]]; then
    echo "${EMOJI_HOURGLASS}"
    return 0
  fi

  case "${conclusion}" in
    success) echo "${EMOJI_CHECK}" ;;
    failure|cancelled|timed_out|action_required|startup_failure|skipped) echo "${EMOJI_CROSS}" ;;
    *) echo "${EMOJI_CROSS}" ;;
  esac
}

# Cache for jobs JSON to avoid redundant API calls
declare -A JOBS_CACHE

# Get status cell for a specific job filter within a workflow run
# Args: run_json job_filter
# Returns emoji based on filtered jobs' status
cell_for_filtered_jobs() {
  local run_json="$1"
  local job_filter="$2"

  # Emoji definitions
  local EMOJI_DASH=$'\xe2\x9e\x96'      # ➖ heavy minus sign
  local EMOJI_HOURGLASS=$'\xe2\x8f\xb3' # ⏳ hourglass
  local EMOJI_CHECK=$'\xe2\x9c\x85'     # ✅ check mark
  local EMOJI_CROSS=$'\xe2\x9d\x8c'     # ❌ cross mark

  if [[ "${run_json}" == "null" || -z "${run_json}" ]]; then
    echo "${EMOJI_DASH}"
    return 0
  fi

  local run_id
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"

  # Fetch jobs for this run (use cache if available)
  local jobs_json
  if [[ -n "${JOBS_CACHE[${run_id}]:-}" ]]; then
    jobs_json="${JOBS_CACHE[${run_id}]}"
  else
    jobs_json="$(gh_retry gh run view -R "${REPO}" "${run_id}" --json jobs 2>/dev/null)" || {
      echo "${EMOJI_DASH}"
      return 0
    }
    JOBS_CACHE[${run_id}]="${jobs_json}"
  fi

  # Filter jobs by pattern (case-insensitive) and get their statuses
  local filtered_status
  filtered_status=$(jq -r --arg filter "${job_filter}" '
    .jobs[]
    | select(.name | ascii_downcase | contains($filter | ascii_downcase))
    | {status: .status, conclusion: .conclusion}
  ' <<<"${jobs_json}")

  if [[ -z "${filtered_status}" ]]; then
    # No matching jobs found
    echo "${EMOJI_DASH}"
    return 0
  fi

  # Check if any filtered job is still in progress
  if echo "${filtered_status}" | jq -e 'select(.status != "completed")' >/dev/null 2>&1; then
    echo "${EMOJI_HOURGLASS}"
    return 0
  fi

  # Check if any filtered job failed
  if echo "${filtered_status}" | jq -e 'select(.conclusion != "success" and .conclusion != "skipped")' >/dev/null 2>&1; then
    echo "${EMOJI_CROSS}"
    return 0
  fi

  echo "${EMOJI_CHECK}"
}

print_table_header() {
  printf "%-4s | %-18s | %-8s | %-8s | %-8s\n" "SNO" "Job Name" "Staging" "Upstream" "Stable"
  printf "%-4s-|-%-18s-|-%-8s-|-%-8s-|-%-8s\n" "----" "------------------" "--------" "--------" "--------"
}

upstream_repo_for_label() {
  local label="$1"
  if echo "${label}" | grep -qi "velox"; then
    echo "facebookincubator/velox"
  elif echo "${label}" | grep -qi "presto"; then
    echo "prestodb/presto"
  else
    echo ""
  fi
}

extract_search_query() {
  local text="$1"
  local line
  # Prefer a clean GTest test name ("ClassName.testMethod") from a FAILED line.
  line="$(printf '%s\n' "${text}" | sed '/^$/d' \
    | grep -oP '(?<=\[  FAILED  \] )[A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*' \
    2>/dev/null | head -1)" || true
  # Fallback: first non-empty line, stripping GTest bracket prefixes and quotes.
  if [[ -z "${line}" ]]; then
    line="$(printf '%s\n' "${text}" | sed '/^$/d' | head -n 1 | tr -d '\r' \
      | sed -E 's/^\[[^]]*\][[:space:]]*//' \
      | sed 's/["'\'']//g')"
  fi
  line="${line:0:120}"
  # GitHub search treats ':' as starting qualifiers; colons in the phrase break the query.
  echo "${line//:/ }"
}

# Extract GTest failing test names ("ClassName.testMethod") from a stacktrace.
extract_gtest_test_names() {
  local text="$1"
  printf '%s\n' "${text}" \
    | grep -oP '(?<=\[  FAILED  \] )[A-Z][A-Za-z0-9_]*\.[A-Za-z][A-Za-z0-9_]*' \
    2>/dev/null | sort -u | head -5
}

# Search GitHub for related issues AND PRs using multiple search terms derived
# from the failure stacktrace. Each term is searched separately (no quoted-phrase
# lock-in) so results like PR titles with slightly different capitalisation are
# still found. Results are deduplicated by URL.
# Args: repo, stacktrace_text, since_date, line_prefix
find_related_github_items() {
  local repo="$1"
  local stacktrace="$2"
  local since_date="$3"
  local prefix="${4:-    - }"

  [[ -z "${repo}" ]] && return 0

  # Build a list of search terms from multiple sources so no single approach
  # is a single point of failure.
  local search_terms=()

  # 1. GTest test names: "ClassName.testMethod" + bare "testMethod" alone.
  #    Searching just the method name catches PRs whose title omits the class.
  local gtest_names
  gtest_names="$(extract_gtest_test_names "${stacktrace}")"
  if [[ -n "${gtest_names}" ]]; then
    while IFS= read -r name; do
      [[ -z "${name}" ]] && continue
      search_terms+=("${name}")
      local method="${name#*.}"
      [[ -n "${method}" && "${method}" != "${name}" ]] && search_terms+=("${method}")
    done <<< "${gtest_names}"
  fi

  # 2. C++/Java identifiers extracted from error lines (::, . , filenames).
  local identifiers
  identifiers="$(extract_error_identifiers "${stacktrace}")"
  if [[ -n "${identifiers}" ]]; then
    while IFS= read -r id; do
      [[ -z "${id}" ]] && continue
      search_terms+=("${id//::/ }")
    done <<< "${identifiers}"
  fi

  # 3. Fallback first-line query when no structured terms were found.
  if [[ "${#search_terms[@]}" -eq 0 ]]; then
    local first_q
    first_q="$(extract_search_query "${stacktrace}")"
    [[ -n "${first_q}" ]] && search_terms+=("${first_q}")
  fi

  # Cap to 3 most specific terms to keep API calls bounded (3 terms × 2 kinds
  # = 6 calls max per failure instead of potentially 40+).
  local capped_terms=()
  for term in "${search_terms[@]}"; do
    [[ "${#capped_terms[@]}" -ge 3 ]] && break
    [[ -z "${term}" ]] && continue
    capped_terms+=("${term}")
  done

  # Search each term against both issues and PRs; deduplicate by URL.
  local seen_urls="" all_items=""
  for term in "${capped_terms[@]}"; do
    [[ -z "${term}" ]] && continue
    for kind in issues prs; do
      local results
      results=$(gh_retry gh search "${kind}" --repo "${repo}" --limit 3 \
        --json title,url,number \
        "${term} created:>=${since_date}" 2>/dev/null \
        | jq -r '.[] | "#\(.number) \(.title) (\(.url))"' 2>/dev/null) || true
      while IFS= read -r item; do
        [[ -z "${item}" ]] && continue
        local item_url
        item_url=$(printf '%s\n' "${item}" | grep -oP 'https://[^ )]+' || true)
        if [[ -n "${item_url}" && "${seen_urls}" != *"${item_url}"* ]]; then
          seen_urls+=" ${item_url}"
          all_items+="${prefix}${item}"$'\n'
        fi
      done <<< "${results}"
    done
  done

  if [[ -n "${all_items}" ]]; then
    printf '%s' "${all_items}" | head -15
  fi
}

print_failure_details() {
  local label="$1"  # e.g. "Velox Build / Upstream"
  local run_json="$2"
  local job_filter="${3:-}"  # optional: filter jobs by name pattern (e.g. "cpu", "gpu")

  local run_id run_url wf_name title conclusion
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"
  run_url="$(jq -r '.url' <<<"${run_json}")"
  wf_name="$(jq -r '.workflowName' <<<"${run_json}")"
  title="$(jq -r '.displayTitle' <<<"${run_json}")"
  conclusion="$(jq -r '.conclusion' <<<"${run_json}")"

  echo ""
  echo "### ${label}"
  echo "- Workflow: ${wf_name}"
  echo "- Run: ${run_url}"
  echo "- Title: ${title}"
  echo "- Conclusion: ${conclusion}"

  # Fetch logs if needed for printing or AI analysis
  local log_out=""
  LOG_TAIL_LINES="${LOG_TAIL_LINES:-150}"
  local since_date
  since_date="$(date -u -d "7 days ago" +%Y-%m-%d 2>/dev/null || date -u +%Y-%m-%d)"

  if [[ "${PRINT_LOGS}" == "true" || "${ANALYZE_CAUSE}" == "true" ]]; then
    log_out="$(gh_retry gh run view -R "${REPO}" "${run_id}" --log-failed 2>/dev/null)" || true
  fi

  # Get failed jobs and process each one
  local jobs_json
  if jobs_json="$(gh_retry gh run view -R "${REPO}" "${run_id}" --json jobs)"; then
    # Get list of failed job names (filtered by job_filter if provided)
    local failed_jobs
    if [[ -n "${job_filter}" ]]; then
      failed_jobs=$(jq -r --arg filter "${job_filter}" '
            .jobs[]
            | select((.name | ascii_downcase | contains($filter | ascii_downcase)))
            | select((.conclusion // "") | IN("success", "skipped") | not)
            | .name
          ' <<<"${jobs_json}")
    else
      failed_jobs=$(jq -r '
            .jobs[]
            | select((.conclusion // "") | IN("success", "skipped") | not)
            | .name
          ' <<<"${jobs_json}")
    fi

    # Process each failed job
    while IFS= read -r job_name; do
      [[ -z "${job_name}" ]] && continue

      # Get job conclusion and failed steps
      local job_conclusion failed_steps
      job_conclusion=$(jq -r --arg jn "${job_name}" '
            .jobs[] | select(.name == $jn) | .conclusion // "unknown"
          ' <<<"${jobs_json}")
      failed_steps=$(jq -r --arg jn "${job_name}" '
            .jobs[]
            | select(.name == $jn)
            | (.steps // [])
            | map(select((.conclusion // "") as $c | $c != "success" and $c != "skipped"))
            | if length == 0 then "    - (no failing step detail)"
              else map("    - Step: \(.name) (\(.conclusion // "unknown"))") | join("\n")
              end
          ' <<<"${jobs_json}")

      echo "  - Job: ${job_name} (${job_conclusion})"
      if [[ -n "${failed_steps}" ]]; then
        echo "${failed_steps}"
      fi

      # Extract job-specific logs (log format: job-name<TAB>step-name<TAB>timestamp<TAB>content)
      local job_log_content="" raw_job_log=""
      if [[ -n "${log_out}" ]]; then
        # Filter logs by job name (first field before tab) - escape special chars in job name
        local escaped_job_name
        escaped_job_name=$(printf '%s' "${job_name}" | sed 's/[[\.*^$()+?{|]/\\&/g')
        raw_job_log=$(echo "${log_out}" | grep -E "^${escaped_job_name}"$'\t' | \
          awk '{print} /Post job cleanup\./ {exit}' || true)
        # For AI analysis: extract complete GTest failure blocks so early
        # failures are not silently lost by a blind tail.
        job_log_content=$(extract_relevant_failures "${raw_job_log}")
      fi

      # Analyze this job's logs with AI (only if --cause flag is set)
      # Pre-fetch related GitHub items if AI analysis is enabled
      local repo="" related_items=""
      if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
        repo="$(upstream_repo_for_label "${label}")"
        related_items="$(find_related_github_items "${repo}" "${job_log_content}" "${since_date}" "    - ")"
      fi

      # Print every failure block with its stacktrace (regex-extracted) and AI cause/fix
      print_blocks_with_analysis "${job_log_content}" "${job_name}" "${wf_name}" "${related_items}" "false"

      if [[ -n "${related_items}" ]]; then
        echo ""
        echo "  Related issues/PRs (last 7 days):"
        printf '%s\n' "${related_items}"
      fi

    done <<< "${failed_jobs}"
  else
    echo "  - WARN: failed to fetch job/step details (network/API timeout). See run link above." >&2
  fi

  # Print all logs if enabled (combined for all jobs)
  if [[ "${PRINT_LOGS}" == "true" ]]; then
    echo ""
    echo "  Failed test stacktraces:"
    if [[ -n "${log_out}" ]]; then
      local log_content
      log_content="$(extract_relevant_failures "${log_out}")"
      echo "${log_content}" | sed 's/^/    /'
    else
      echo "    (WARN) Unable to fetch logs (network/API timeout). Open the run link above for full logs."
    fi
  fi
}

print_in_progress_details() {
  local label="$1"
  local workflow_file="$2"
  local run_json="$3"

  local run_id run_url wf_name title status attempt run_number
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"
  run_url="$(jq -r '.url' <<<"${run_json}")"
  wf_name="$(jq -r '.workflowName' <<<"${run_json}")"
  title="$(jq -r '.displayTitle' <<<"${run_json}")"
  status="$(jq -r '.status' <<<"${run_json}")"
  attempt="$(jq -r '.attempt // 1' <<<"${run_json}")"
  run_number="$(jq -r '.number // empty' <<<"${run_json}")"

  echo ""
  echo "### ${label} (in progress)"
  echo "- Workflow: ${wf_name}"
  echo "- Run: ${run_url}"
  echo "- Title: ${title}"
  echo "- Status: ${status} (attempt: ${attempt})"

  # If this is a rerun attempt (>1), show attempt=1 status/log tail to help debug.
  if [[ "${attempt}" -gt 1 && -n "${run_number}" ]]; then
    first_json="$(get_first_attempt_for_run_number_json "${workflow_file}" "${run_number}" || echo "null")"
    if [[ "${first_json}" != "null" ]]; then
      local first_status first_conclusion first_url first_id
      first_status="$(jq -r '.status' <<<"${first_json}")"
      first_conclusion="$(jq -r '.conclusion // ""' <<<"${first_json}")"
      first_url="$(jq -r '.url' <<<"${first_json}")"
      first_id="$(jq -r '.databaseId' <<<"${first_json}")"
      echo ""
      echo "  First attempt details:"
      echo "  - Run: ${first_url}"
      echo "  - Status: ${first_status}"
      echo "  - Conclusion: ${first_conclusion:-n/a}"
      if [[ "${first_status}" == "completed" && "${first_conclusion}" != "success" && "${PRINT_LOGS}" == "true" ]]; then
        echo ""
        echo "  First attempt failed test stacktraces:"
        if log_out="$(gh_retry gh run view -R "${REPO}" "${first_id}" --log-failed 2>/dev/null)"; then
          extract_relevant_failures "${log_out}" | sed 's/^/    /'
        else
          echo "    (WARN) Unable to fetch logs. Open the run link above for full logs."
        fi
      fi
    fi
  fi
}

# --- Log extraction helpers ---

# Extract relevant failure content from GH Actions logs for AI analysis.
# For Google Test (GTest) logs: extracts complete failing test blocks
#   (from "[ RUN      ] TestName" through "[ FAILED  ] TestName") and the
#   CTest summary, so early failures are never missed by a blind tail.
# For other logs (compilation, etc.): strips the GH Actions
#   job/step/timestamp prefix and returns a generous tail.
# Input: raw GH Actions log lines (JOBNAME<TAB>STEPNAME<TAB>TIMESTAMP<TAB>CONTENT)
extract_relevant_failures() {
  local raw_log="$1"
  local max_fallback_lines="${2:-500}"

  # Strip the GitHub Actions log prefix (4-field tab-separated header) to
  # obtain just the actual test/build output lines.
  local content
  content=$(printf '%s\n' "${raw_log}" | awk -F'\t' '{
    if (NF >= 4) {
      out = $4
      for (i=5; i<=NF; i++) out = out "\t" $i
      print out
    } else if (NF >= 3) {
      sub(/^[0-9]+-[0-9]+-[0-9]+T[0-9:.]+Z /, "", $3)
      print $3
    } else {
      print $0
    }
  }')

  # Detect Google Test output by looking for GTest markers.
  if printf '%s\n' "${content}" | grep -qE '^\[  FAILED  \]|\[ RUN[[:space:]]+\]'; then
    local gtest_blocks
    gtest_blocks=$(printf '%s\n' "${content}" | awk -v sep="${BLOCK_SEP}" '
      # Start accumulating a test block when we see "[ RUN      ]"
      /^\[ RUN[[:space:]]/ {
        in_block = 1
        block = $0
        next
      }
      in_block {
        block = block "\n" $0
        # Failing test – emit the entire accumulated block followed by separator
        if (/^\[  FAILED  \]/) {
          # Track test name so standalone summary lines for this test are skipped
          tname = $0
          sub(/^\[  FAILED  \] /, "", tname)
          sub(/ \([0-9]+ ms\).*$/, "", tname)
          seen_tests[tname] = 1
          print block
          print sep
          in_block = 0
          block = ""
          next
        }
        # Passing test – discard the block silently
        if (/^\[       OK \]/ || /^\[ DISABLED \]/ || /^\[ SKIPPED \]/) {
          in_block = 0
          block = ""
          next
        }
        next
      }
      # Standalone [  FAILED  ] summary lines (outside any block)
      # Skip if the test already appeared in a full RUN...FAILED block above.
      /^\[  FAILED  \]/ {
        tname = $0
        sub(/^\[  FAILED  \] /, "", tname)
        sub(/ \([0-9]+ ms\).*$/, "", tname)
        if (tname in seen_tests) next
        print; print sep; next
      }
      # CTest percentage summary  (e.g. "99% tests passed, 1 tests failed out of 467")
      /^[0-9]+%.*tests passed/ { print; next }
      # CTest failing-test list
      /^The following tests FAILED:/ { in_ctest = 1 }
      in_ctest { print; next }
    ')
    if [[ -n "${gtest_blocks}" ]]; then
      printf '%s\n' "${gtest_blocks}"
      return
    fi
  fi

  # Fallback for non-GTest logs (compilation errors, etc.): extract only
  # error-relevant lines with 2 lines of leading context instead of a blind tail.
  local error_lines
  error_lines=$(printf '%s\n' "${content}" | awk '
    {
      buf[NR] = $0
    }
    END {
      # Pass 1: mark lines matching error patterns and Docker error blocks
      for (i = 1; i <= NR; i++) {
        line = buf[i]
        if (line ~ /[Ee]rror[: []/ ||
            line ~ /FAILED/ ||
            line ~ /[Ff]atal/ ||
            line ~ /ninja: build stopped/ ||
            line ~ /make.*\*\*\*/ ||
            line ~ /^failed to solve:/ ||
            line ~ /##\[error\]/ ||
            line ~ /^------$/ ||
            line ~ /^--------------------$/ ||
            line ~ /\.dockerfile:[0-9]+/ ||
            line ~ /^[[:space:]]*[0-9]+ \|/ ||
            line ~ /^[[:space:]]*>>>/ ||
            line ~ /undefined reference/ ||
            line ~ /cannot find -l/ ||
            line ~ /[Cc]onfiguring incomplete/) {
          mark[i] = 1
          # Include 2 lines of context before
          if (i-2 > 0 && !mark[i-2]) mark[i-2] = 1
          if (i-1 > 0 && !mark[i-1]) mark[i-1] = 1
        }
      }
      # Also always include the last 5 lines (final error summary)
      for (i = NR-4; i <= NR; i++) {
        if (i > 0) mark[i] = 1
      }
      # Pass 2: print marked lines, inserting "..." for skipped regions
      prev_printed = 0
      for (i = 1; i <= NR; i++) {
        if (mark[i]) {
          if (prev_printed && i - prev_printed > 1) print "..."
          print buf[i]
          prev_printed = i
        }
      }
    }
  ')

  if [[ -n "${error_lines}" ]]; then
    printf '%s\n' "${error_lines}"
  else
    # Ultimate fallback: if no error patterns matched, show the last few lines
    printf '%s\n' "${content}" | tail -n 20
  fi
}

# --- Slack format functions ---

# Analyze logs using AI to determine cause and fix
# Requires: LLM_API_KEY or NVIDIA_API_KEY environment variable
# Optional: LLM_API_URL (defaults to NVIDIA), LLM_MODEL (defaults to llama-3.3-nemotron)
analyze_logs_with_ai() {
  local log_content="$1"
  local job_name="$2"
  local workflow_name="$3"

  # Check for API key
  local api_key="${LLM_API_KEY:-${NVIDIA_API_KEY:-}}"
  if [[ -z "${api_key}" ]]; then
    echo "STACKTRACE:Unable to extract - API key not set"
    echo "CAUSE:Unable to analyze - LLM_API_KEY or NVIDIA_API_KEY not set"
    echo "FIX:Set API key to enable AI-powered log analysis"
    return
  fi

  # API configuration (defaults to NVIDIA's API)
  local api_url="${LLM_API_URL:-https://integrate.api.nvidia.com/v1/chat/completions}"
  local model="${LLM_MODEL:-nvdev/nvidia/llama-3.3-nemotron-super-49b-v1}"

  # Truncate logs if too long (keep last ~30000 chars to capture full error context)
  local truncated_logs
  if [[ ${#log_content} -gt 30000 ]]; then
    truncated_logs="[...truncated...]\n$(echo "${log_content}" | tail -c 30000)"
  else
    truncated_logs="${log_content}"
  fi

  # Escape special characters for JSON
  local escaped_logs
  escaped_logs=$(echo "${truncated_logs}" | jq -Rs '.')

  # Build the prompt with detailed instructions for deep analysis
  local prompt="You are analyzing a CI/CD build failure log. Your task is to find the ROOT CAUSE of the failure, not just the final symptom.

IMPORTANT ANALYSIS RULES:
- For compilation failures: Look for the FIRST 'error:' message with actual error details (type mismatches, undefined references, missing includes, etc.)
- DO NOT report generic messages like 'make failed', 'ninja: build stopped', 'exit code 1' as the cause - these are symptoms, not causes
- Look for specific error patterns: type conversion errors, missing symbols, API mismatches, test assertion failures
- Include the specific file name, class name, or function name involved in the error
- For type errors, mention what type was expected vs what was provided
- For TEST FAILURES: Always include the EXACT test case name(s) that failed (e.g., 'TestClassName.testMethodName', 'test_function_name')
- For test failures, mention the assertion that failed or the error message from the test

Job: ${job_name}
Workflow: ${workflow_name}

Log output:
${truncated_logs}

Based on your analysis, provide:
1. STACKTRACE: Extract the relevant error stacktrace or error messages from the log (the actual error output, compiler errors, test failures, or exception traces - NOT the entire log, just the key error portion)
2. CAUSE: The specific root cause (mention file/class/function names, exact error like type mismatch, missing symbol, failed test names, etc.)
3. FIX: A concrete suggested fix or investigation step

Respond in exactly this format (no markdown except for STACKTRACE which can be multiline):
STACKTRACE:<the relevant error stacktrace or error messages, can span multiple lines, end with END_STACKTRACE on its own line>
END_STACKTRACE
CAUSE:<your specific root cause description - single line>
FIX:<your fix suggestion - single line>"

  local escaped_prompt
  escaped_prompt=$(echo "${prompt}" | jq -Rs '.')

  # Make API request (using NVIDIA API parameters)
  local llm_timeout="${LLM_TIMEOUT:-30}"
  local response
  response=$(curl -s --max-time "${llm_timeout}" "${api_url}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${api_key}" \
    -d "{
      \"model\": \"${model}\",
      \"messages\": [{\"role\": \"user\", \"content\": ${escaped_prompt}}],
      \"max_tokens\": 1024,
      \"temperature\": 0.2,
      \"top_p\": 0.7
    }" 2>/dev/null)

  if [[ -z "${response}" ]]; then
    echo "STACKTRACE:Unable to extract - API request failed or timed out (${llm_timeout}s)"
    echo "CAUSE:Unable to analyze - API request failed"
    echo "FIX:Check network connectivity and API key, or increase LLM_TIMEOUT"
    return
  fi

  # Extract the content from response
  local content
  content=$(echo "${response}" | jq -r '.choices[0].message.content // empty' 2>/dev/null)

  if [[ -z "${content}" ]]; then
    # Check for error message
    local error_msg
    error_msg=$(echo "${response}" | jq -r '.error.message // empty' 2>/dev/null)
    if [[ -n "${error_msg}" ]]; then
      echo "STACKTRACE:Unable to extract - API error"
      echo "CAUSE:API error - ${error_msg}"
      echo "FIX:Check API key and quota"
    else
      echo "STACKTRACE:Unable to extract - API response parse error"
      echo "CAUSE:Unable to parse API response"
      echo "FIX:Check API configuration"
    fi
    return
  fi

  # Return the AI response (should contain CAUSE: and FIX: lines)
  echo "${content}"
}

# Extract C++/Java error identifiers (Class::method, Class.method, filenames) from log content.
# Returns unique identifiers suitable for GitHub search, one per line.
extract_error_identifiers() {
  local log_content="$1"
  local identifiers=""

  # C++ qualified names on lines containing error indicators
  local cpp_ids
  cpp_ids=$(echo "${log_content}" | grep -iE 'error|undefined|unresolved|FAILED|fatal' \
    | grep -oP '[A-Z][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)+' 2>/dev/null | head -5) || true
  identifiers+="${cpp_ids}"$'\n'

  # Java/test method names: ClassName.methodName (from test failure lines)
  local java_ids
  java_ids=$(echo "${log_content}" | grep -iE 'FAILED|FAIL|ERROR|assert' \
    | grep -oP '[A-Z][A-Za-z0-9_]*\.[a-z][A-Za-z0-9_]*' 2>/dev/null | head -5) || true
  identifiers+="${java_ids}"$'\n'

  # Source filenames from error/undefined lines (e.g., HiveIndexReader.cpp)
  local file_ids
  file_ids=$(echo "${log_content}" | grep -iE 'error|undefined|FAILED' \
    | grep -oP '[A-Z][A-Za-z0-9_]+\.(?:cpp|h|cu|cuh|java)' 2>/dev/null | head -5) || true
  identifiers+="${file_ids}"$'\n'

  echo "${identifiers}" | sed '/^$/d' | sort -u | head -8
}

# Compute a stable signature for a CI failure to detect cross-build duplicates.
# Uses job+step names from JOBS_CACHE (already populated by the status table loop).
# Returns a string like "job-name|step-name+step-name:..." stable across builds.
# Args: run_id job_filter
compute_failure_signature() {
  local run_id="$1"
  local job_filter="${2:-}"

  local jobs_json="${JOBS_CACHE[${run_id}]:-}"
  if [[ -z "${jobs_json}" ]]; then
    jobs_json="$(gh_retry gh run view -R "${REPO}" "${run_id}" --json jobs 2>/dev/null)" || jobs_json='{"jobs":[]}'
    JOBS_CACHE[${run_id}]="${jobs_json}"
  fi

  # Signature: sorted "job_name|sorted_failed_step_names" for each failed (filtered) job.
  jq -r --arg filter "${job_filter}" '
    .jobs[]
    | (if ($filter == "") then . else select(.name | ascii_downcase | contains($filter | ascii_downcase)) end)
    | select((.conclusion // "") | IN("success","skipped") | not)
    | (.name + "|" +
        ((.steps // [])
          | map(select((.conclusion // "") != "success" and (.conclusion // "") != "skipped"))
          | map(.name) | sort | join("+")))
  ' <<<"${jobs_json}" 2>/dev/null | sort | tr '\n' ':' | sed 's/:$//'
}

# Print every failure block with its stacktrace, cause, and fix.
# Splits job_log_content on BLOCK_SEP into individual failure blocks extracted
# by regex (e.g. GTest [ RUN ] → [ FAILED ] blocks). Each block is printed
# directly as the stacktrace; AI is called per-block (if ANALYZE_CAUSE=true)
# for cause+fix only.
# Args: job_log_content, job_name, wf_name, related_items, slack (true/false)
print_blocks_with_analysis() {
  local job_log_content="$1"
  local job_name="$2"
  local wf_name="$3"
  local related_items="$4"
  local slack="${5:-false}"

  local st_label cause_label fix_label
  if [[ "${slack}" == "true" ]]; then
    st_label="*Stacktrace"
    cause_label="*Cause:*"
    fix_label="*Fix:*"
  else
    st_label="- Stacktrace"
    cause_label="- Cause:"
    fix_label="- Fix:"
  fi

  if [[ -z "${job_log_content}" ]]; then
    echo "    ${st_label}: _Unavailable_"
    if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
      echo "    ${cause_label} _Unable to fetch logs for this job_"
      [[ "${ANALYZE_FIX}" == "true" ]] && echo "    ${fix_label} _Check the run link above for details_"
    fi
    return
  fi

  # Split content into individual failure blocks on BLOCK_SEP
  local blocks=()
  local current_block=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" == "${BLOCK_SEP}" ]]; then
      if [[ -n "$(printf '%s\n' "${current_block}" | sed '/^[[:space:]]*$/d')" ]]; then
        blocks+=("${current_block}")
      fi
      current_block=""
    else
      current_block+="${line}"$'\n'
    fi
  done <<< "${job_log_content}"
  # Include last block (non-GTest fallback has no trailing separator)
  if [[ -n "$(printf '%s\n' "${current_block}" | sed '/^[[:space:]]*$/d')" ]]; then
    blocks+=("${current_block}")
  fi

  local n_blocks="${#blocks[@]}"
  local idx=0
  for block in "${blocks[@]}"; do
    idx=$((idx + 1))
    local label_suffix=""
    [[ "${n_blocks}" -gt 1 ]] && label_suffix=" ${idx}/${n_blocks}"

    # Print the regex-extracted stacktrace directly — no AI needed for this part
    if [[ "${slack}" == "true" ]]; then
      echo "    ${st_label}${label_suffix}:*"
    else
      echo "    ${st_label}${label_suffix}:"
    fi
    echo '```'
    if [[ "${slack}" == "true" ]]; then
      local cleaned_block
      cleaned_block="$(printf '%s\n' "${block}" | sed '/^[[:space:]]*$/d')"
      printf '%s\n' "${cleaned_block}"
    else
      printf '%s\n' "${block}" | sed '/^[[:space:]]*$/d'
    fi
    echo '```'

    # AI analysis for cause + fix of this specific failure block
    if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
      local ai_response cause fix
      if [[ "${USE_CLAUDE}" == "true" ]]; then
        ai_response="$(analyze_logs_with_claude_ai "${block}" "${job_name}" "${wf_name}" "${related_items}")"
      else
        ai_response="$(analyze_logs_with_ai "${block}" "${job_name}" "${wf_name}")"
      fi
      cause="$(printf '%s\n' "${ai_response}" | grep -i "^CAUSE:" | sed 's/^CAUSE:[[:space:]]*//' | head -1 || true)"
      fix="$(printf '%s\n' "${ai_response}" | grep -i "^FIX:" | sed 's/^FIX:[[:space:]]*//' | head -1 || true)"
      echo "    ${cause_label} _${cause:-Unable to determine cause}_"
      [[ "${ANALYZE_FIX}" == "true" ]] && echo "    ${fix_label} _${fix:-Pending investigation}_"
    fi
  done
}

# Search velox and presto GitHub repos for issues AND PRs related to given
# identifiers (used to build Claude's context).  Also includes bare GTest
# method names extracted from the identifiers so PR titles that omit the
# class prefix are still found.
search_related_github_prs() {
  local identifiers="$1"
  local since_date
  since_date="$(date -u -d "30 days ago" +%Y-%m-%d 2>/dev/null || date -u +%Y-%m-%d)"

  if [[ -z "${identifiers}" ]]; then
    return
  fi

  # Build search terms: the original identifiers + bare method names for
  # ClassName.methodName style identifiers (covers PRs that skip the class).
  local search_terms=()
  while IFS= read -r id; do
    [[ -z "${id}" ]] && continue
    local term="${id//::/ }"
    search_terms+=("${term}")
    # If "ClassName.methodName", also search just "methodName"
    if [[ "${term}" == *.* ]]; then
      local method="${term##*.}"
      [[ -n "${method}" ]] && search_terms+=("${method}")
    fi
  done <<< "${identifiers}"

  local all_results=""
  local repos=("facebookincubator/velox" "prestodb/presto")
  local seen_urls=""

  for search_term in "${search_terms[@]}"; do
    [[ -z "${search_term}" ]] && continue
    for repo in "${repos[@]}"; do
      for kind in issues prs; do
        local results
        results=$(gh_retry gh search "${kind}" --repo "${repo}" --limit 3 \
          --json title,url,number,state \
          "${search_term} created:>=${since_date}" 2>/dev/null \
          | jq -r '.[] | "#\(.number) [\(.state)] \(.title) (\(.url))"' 2>/dev/null) || true
        while IFS= read -r result_line; do
          [[ -z "${result_line}" ]] && continue
          local result_url
          result_url=$(printf '%s\n' "${result_line}" | grep -oP 'https://[^ )]+' || true)
          if [[ -n "${result_url}" && "${seen_urls}" != *"${result_url}"* ]]; then
            seen_urls+=" ${result_url}"
            all_results+="  - ${result_line}"$'\n'
          fi
        done <<< "${results}"
      done
    done
  done

  if [[ -n "${all_results}" ]]; then
    printf '%s' "${all_results}" | head -20
  fi
}

# Analyze logs using Claude Code CLI to determine cause and fix
# Requires: claude CLI installed (e.g., ~/.local/bin/claude)
# Optional: CLAUDE_MODEL (defaults to opus)
analyze_logs_with_claude_ai() {
  local log_content="$1"
  local job_name="$2"
  local workflow_name="$3"
  # Optional: pre-fetched related items string; when set, skips internal GitHub search
  local prefetched_items="${4:-}"

  local claude_bin="${CLAUDE_BIN:-claude}"
  if ! command -v "${claude_bin}" &>/dev/null; then
    echo "STACKTRACE:Unable to extract - claude CLI not found"
    echo "CAUSE:Unable to analyze - claude CLI not installed or not in PATH"
    echo "FIX:Install Claude Code CLI or set CLAUDE_BIN to the correct path"
    return
  fi

  local model="${CLAUDE_MODEL:-opus}"

  local truncated_logs
  if [[ ${#log_content} -gt 30000 ]]; then
    truncated_logs="[...truncated...]
$(echo "${log_content}" | tail -c 30000)"
  else
    truncated_logs="${log_content}"
  fi

  # Use pre-fetched related items if provided; otherwise search GitHub internally
  local related_items="" related_items_section=""
  if [[ -n "${prefetched_items}" ]]; then
    related_items="${prefetched_items}"
  else
    local identifiers
    identifiers="$(extract_error_identifiers "${truncated_logs}")"
    local gtest_names
    gtest_names="$(extract_gtest_test_names "${truncated_logs}")"
    if [[ -n "${gtest_names}" ]]; then
      identifiers="$(printf '%s\n%s\n' "${identifiers}" "${gtest_names}" | sed '/^$/d' | sort -u)"
    fi
    if [[ -n "${identifiers}" ]]; then
      related_items="$(search_related_github_prs "${identifiers}")"
    fi
  fi
  if [[ -n "${related_items}" ]]; then
    related_items_section="

RELATED GITHUB ISSUES AND PRs (found by searching error identifiers in velox/presto repos):
${related_items}

When suggesting a FIX, reference any relevant issue or PR from above that may have introduced or fixed the issue. Include the number and URL."
  fi

  local prompt="You are analyzing a CI/CD build failure log. Your task is to find the ROOT CAUSE of the failure, not just the final symptom.

IMPORTANT ANALYSIS RULES:
- For compilation failures: Look for the FIRST 'error:' message with actual error details (type mismatches, undefined references, missing includes, etc.)
- DO NOT report generic messages like 'make failed', 'ninja: build stopped', 'exit code 1' as the cause - these are symptoms, not causes
- Look for specific error patterns: type conversion errors, missing symbols, API mismatches, test assertion failures
- Include the specific file name, class name, or function name involved in the error
- For type errors, mention what type was expected vs what was provided
- For TEST FAILURES: Always include the EXACT test case name(s) that failed (e.g., 'TestClassName.testMethodName', 'test_function_name')
- For test failures, mention the assertion that failed or the error message from the test

Job: ${job_name}
Workflow: ${workflow_name}

Log output:
${truncated_logs}${related_items_section}

Based on your analysis, provide:
1. STACKTRACE: Extract the relevant error stacktrace or error messages from the log (the actual error output, compiler errors, test failures, or exception traces - NOT the entire log, just the key error portion)
2. CAUSE: The specific root cause (mention file/class/function names, exact error like type mismatch, missing symbol, failed test names, etc.)
3. FIX: A concrete suggested fix or investigation step. If any of the RELATED GITHUB ISSUES AND PRs above are relevant to the failure, mention them with their number and URL.

Respond in exactly this format (no markdown except for STACKTRACE which can be multiline):
STACKTRACE:<the relevant error stacktrace or error messages, can span multiple lines, end with END_STACKTRACE on its own line>
END_STACKTRACE
CAUSE:<your specific root cause description - single line>
FIX:<your fix suggestion - single line, include relevant PR links if applicable>"

  local content
  content=$(echo "${prompt}" | "${claude_bin}" --print \
    --model "${model}" \
    --no-session-persistence \
    --allowedTools "" \
    2>/dev/null) || true

  if [[ -z "${content}" ]]; then
    echo "STACKTRACE:Unable to extract - Claude CLI returned no output"
    echo "CAUSE:Unable to analyze - Claude CLI failed (check authentication with 'claude --print \"hello\"')"
    echo "FIX:Run 'claude' interactively once to authenticate, or check ANTHROPIC_API_KEY"
    return
  fi

  echo "${content}"
}

print_slack_header() {
  # Format date as "Month DD, YYYY" (e.g., January 21, 2026)
  local formatted_date
  formatted_date="$(date -d "${TODAY_UTC}" '+%B %d, %Y' 2>/dev/null || echo "${TODAY_UTC}")"
  echo "*🌙 Nightly Jobs Status - ${formatted_date}*"
  echo ""
  echo "*Status Summary:*"
  echo ""
}

print_slack_table_header() {
  echo "| *NO* | *Job Name*         | *Staging* | *Upstream* | *Stable* |"
  echo "|------|--------------------| --------- | ---------- | -------- |"
}

print_slack_failure_details() {
  local idx="$1"        # e.g., "1"
  local label="$2"      # e.g., "Velox Build / Upstream"
  local run_json="$3"
  local job_filter="${4:-}"  # optional: filter jobs by name pattern (e.g. "cpu", "gpu")
  local extra_affects="${5:-}"  # optional: newline-sep "label\turl" for grouped duplicates

  local run_id run_url wf_name conclusion
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"
  run_url="$(jq -r '.url' <<<"${run_json}")"
  wf_name="$(jq -r '.workflowName' <<<"${run_json}")"
  conclusion="$(jq -r '.conclusion' <<<"${run_json}")"
  local since_date
  since_date="$(date -u -d "7 days ago" +%Y-%m-%d 2>/dev/null || date -u +%Y-%m-%d)"

  echo ""
  echo "*${idx}. ${label}*"
  echo "• *Workflow:* ${wf_name}"
  echo "• *Run:* ${run_url}"
  # If the same failure was detected in other builds, list them here
  if [[ -n "${extra_affects}" ]]; then
    local also_str=""
    while IFS=$'\t' read -r al_label al_url; do
      [[ -z "${al_label}" ]] && continue
      also_str+="${al_label} (${al_url}), "
    done <<< "${extra_affects}"
    echo "• *Also fails in:* ${also_str%, }"
  fi
  echo "• *Conclusion:* ${conclusion}"

  # Fetch logs if needed for printing or AI analysis
  local log_out=""
  LOG_TAIL_LINES="${LOG_TAIL_LINES:-150}"

  if [[ "${PRINT_LOGS}" == "true" || "${ANALYZE_CAUSE}" == "true" ]]; then
    log_out="$(gh_retry gh run view -R "${REPO}" "${run_id}" --log-failed 2>/dev/null)" || true
  fi

  # Get failed jobs and process each one
  local jobs_json
  if jobs_json="$(gh_retry gh run view -R "${REPO}" "${run_id}" --json jobs)"; then
    # Get list of failed job names (filtered by job_filter if provided)
    local failed_jobs
    if [[ -n "${job_filter}" ]]; then
      failed_jobs=$(jq -r --arg filter "${job_filter}" '
            .jobs[]
            | select((.name | ascii_downcase | contains($filter | ascii_downcase)))
            | select((.conclusion // "") | IN("success", "skipped") | not)
            | .name
          ' <<<"${jobs_json}")
    else
      failed_jobs=$(jq -r '
            .jobs[]
            | select((.conclusion // "") | IN("success", "skipped") | not)
            | .name
          ' <<<"${jobs_json}")
    fi

    # Process each failed job
    while IFS= read -r job_name; do
      [[ -z "${job_name}" ]] && continue

      # Get job conclusion and failed steps
      local job_conclusion failed_steps
      job_conclusion=$(jq -r --arg jn "${job_name}" '
            .jobs[] | select(.name == $jn) | .conclusion // "unknown"
          ' <<<"${jobs_json}")
      failed_steps=$(jq -r --arg jn "${job_name}" '
            .jobs[]
            | select(.name == $jn)
            | (.steps // [])
            | map(select((.conclusion // "") as $c | $c != "success" and $c != "skipped"))
            | map("    ▪︎ Step: \(.name) (\(.conclusion // "unknown"))")
            | join("\n")
          ' <<<"${jobs_json}")

      echo "  ◦ Job: \`${job_name}\` (${job_conclusion})"
      if [[ -n "${failed_steps}" ]]; then
        echo "${failed_steps}"
      fi

      # Extract job-specific logs (log format: job-name<TAB>step-name<TAB>timestamp<TAB>content)
      local job_log_content="" raw_job_log=""
      if [[ -n "${log_out}" ]]; then
        # Filter logs by job name (first field before tab) - escape special chars in job name
        local escaped_job_name
        escaped_job_name=$(printf '%s' "${job_name}" | sed 's/[[\.*^$()+?{|]/\\&/g')
        raw_job_log=$(echo "${log_out}" | grep -E "^${escaped_job_name}"$'\t' | \
          awk '{print} /Post job cleanup\./ {exit}' || true)
        # For AI analysis: extract complete GTest failure blocks so early
        # failures are not silently lost by a blind tail.
        job_log_content=$(extract_relevant_failures "${raw_job_log}")
      fi

      # Analyze this job's logs with AI (only if --cause flag is set)
      # Pre-fetch related GitHub items if AI analysis is enabled
      local repo="" related_items=""
      if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
        repo="$(upstream_repo_for_label "${label}")"
        related_items="$(find_related_github_items "${repo}" "${job_log_content}" "${since_date}" "    • ")"
      fi

      # Print every failure block with its stacktrace (regex-extracted) and AI cause/fix
      print_blocks_with_analysis "${job_log_content}" "${job_name}" "${wf_name}" "${related_items}" "true"

      if [[ -n "${related_items}" ]]; then
        echo ""
        echo "    *Related issues/PRs (last 7 days):*"
        printf '%s\n' "${related_items}"
      fi

    done <<< "${failed_jobs}"
  else
    echo "  ◦ _WARN: failed to fetch job/step details (network/API timeout). See run link above._" >&2
  fi

  # Print all logs if enabled (combined for all jobs)
  if [[ "${PRINT_LOGS}" == "true" ]]; then
    echo ""
    echo "*Failed test stacktraces:*"
    if [[ -n "${log_out}" ]]; then
      local log_content
      log_content="$(extract_relevant_failures "${log_out}" | sed '/^[[:space:]]*$/d')"
      echo '```'
      printf '%s\n' "${log_content}"
      echo '```'
    else
      echo "_Unable to fetch logs (network/API timeout). Open the run link above for full logs._"
    fi
  fi
}

print_slack_in_progress_details() {
  local idx="$1"
  local label="$2"
  local workflow_file="$3"
  local run_json="$4"

  local run_id run_url wf_name status attempt run_number
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"
  run_url="$(jq -r '.url' <<<"${run_json}")"
  wf_name="$(jq -r '.workflowName' <<<"${run_json}")"
  status="$(jq -r '.status' <<<"${run_json}")"
  attempt="$(jq -r '.attempt // 1' <<<"${run_json}")"
  run_number="$(jq -r '.number // empty' <<<"${run_json}")"

  echo ""
  echo "*${idx}. ${label} (in progress)*"
  echo "• *Workflow:* ${wf_name}"
  echo "• *Run:* ${run_url}"
  echo "• *Status:* ${status} (attempt: ${attempt})"

  # If this is a rerun attempt (>1), show attempt=1 status/log tail.
  if [[ "${attempt}" -gt 1 && -n "${run_number}" ]]; then
    first_json="$(get_first_attempt_for_run_number_json "${workflow_file}" "${run_number}" || echo "null")"
    if [[ "${first_json}" != "null" ]]; then
      local first_status first_conclusion first_url first_id
      first_status="$(jq -r '.status' <<<"${first_json}")"
      first_conclusion="$(jq -r '.conclusion // ""' <<<"${first_json}")"
      first_url="$(jq -r '.url' <<<"${first_json}")"
      first_id="$(jq -r '.databaseId' <<<"${first_json}")"
      echo ""
      echo "  _First attempt details:_"
      echo "  • *Run:* ${first_url}"
      echo "  • *Status:* ${first_status}"
      echo "  • *Conclusion:* ${first_conclusion:-n/a}"

      # Print first attempt failed logs if enabled.
      if [[ "${first_status}" == "completed" && "${first_conclusion}" != "success" && "${PRINT_LOGS}" == "true" ]]; then
        echo ""
        echo "*First attempt failed test stacktraces:*"
        if log_out="$(gh_retry gh run view -R "${REPO}" "${first_id}" --log-failed 2>/dev/null)"; then
          echo '```'
          extract_relevant_failures "${log_out}" | sed '/^[[:space:]]*$/d' || true
          echo '```'
        else
          echo "_Unable to fetch logs. Open the run link above for full logs._"
        fi
      fi
    fi
  fi
}
# --- End Slack format functions ---

# Convert DISPLAY_DATE (YYYY-MM-DD) to MM-DD-YYYY for display
DISPLAY_DATE_MMDDYYYY="$(date -d "${DISPLAY_DATE}" '+%m-%d-%Y' 2>/dev/null || echo "${DISPLAY_DATE}")"

# Output file for status report (write to both stdout and file)
STATUS_FILE="${STATUS_FILE:-status.txt}"
exec > >(tee "${STATUS_FILE}") 2>&1

if [[ "${SLACK_FORMAT}" == "true" ]]; then
  print_slack_header
  print_slack_table_header
else
  echo "1. Nightly Jobs Status Table - current date (${DISPLAY_DATE_MMDDYYYY})"
  echo ""
  print_table_header
fi

# Row definitions (workflow files live under .github/workflows/)
# "Stable" for Presto refers to presto-nightly-pinned.yml per request.
# Note: velox-nightly-*.yml workflows contain both CPU and GPU jobs
declare -A WF_UPSTREAM WF_STAGING WF_STABLE JOB_FILTER
WF_UPSTREAM["Velox Build CPU"]="velox-nightly-upstream.yml"
WF_STAGING["Velox Build CPU"]="velox-nightly-staging.yml"
WF_STABLE["Velox Build CPU"]=""  # no stable variant defined
JOB_FILTER["Velox Build CPU"]="cpu"  # filter jobs containing "cpu" (case-insensitive)

WF_UPSTREAM["Velox Build GPU"]="velox-nightly-upstream.yml"
WF_STAGING["Velox Build GPU"]="velox-nightly-staging.yml"
WF_STABLE["Velox Build GPU"]=""  # no stable variant defined
JOB_FILTER["Velox Build GPU"]="gpu"  # filter jobs containing "gpu" (case-insensitive)

WF_UPSTREAM["Velox Benchmark"]=""  # none
WF_STAGING["Velox Benchmark"]="velox-benchmark-nightly-staging.yml"
WF_STABLE["Velox Benchmark"]=""    # none

WF_UPSTREAM["Presto Java"]="presto-nightly-upstream.yml"
WF_STAGING["Presto Java"]="presto-nightly-staging.yml"
WF_STABLE["Presto Java"]="presto-nightly-pinned.yml"
JOB_FILTER["Presto Java"]="java"  # filter jobs containing "java" (case-insensitive)

WF_UPSTREAM["Presto CPU"]="presto-nightly-upstream.yml"
WF_STAGING["Presto CPU"]="presto-nightly-staging.yml"
WF_STABLE["Presto CPU"]="presto-nightly-pinned.yml"
JOB_FILTER["Presto CPU"]="native-cpu"  # filter jobs containing "native-cpu"

WF_UPSTREAM["Presto GPU"]="presto-nightly-upstream.yml"
WF_STAGING["Presto GPU"]="presto-nightly-staging.yml"
WF_STABLE["Presto GPU"]="presto-nightly-pinned.yml"
JOB_FILTER["Presto GPU"]="native-gpu"  # filter jobs containing "native-gpu"

rows=("Velox Build CPU" "Velox Build GPU" "Velox Benchmark" "Presto Java" "Presto CPU" "Presto GPU")

# Display names for the table (with parentheses for better formatting)
declare -A DISPLAY_NAME
DISPLAY_NAME["Velox Build CPU"]="Velox Build (CPU)"
DISPLAY_NAME["Velox Build GPU"]="Velox Build (GPU)"
DISPLAY_NAME["Presto Java"]="Presto (Java)"
DISPLAY_NAME["Presto CPU"]="Presto (CPU)"
DISPLAY_NAME["Presto GPU"]="Presto (GPU)"

fail_labels=()
fail_runs=()
fail_filters=()
inprog_labels=()
inprog_wfs=()
inprog_runs=()
inprog_filters=()

row_no=1
for row in "${rows[@]}"; do
  up_wf="${WF_UPSTREAM[$row]}"
  st_wf="${WF_STAGING[$row]}"
  sb_wf="${WF_STABLE[$row]}"
  job_filter="${JOB_FILTER[$row]:-}"

  up_run="null"
  st_run="null"
  sb_run="null"

  if [[ -n "${up_wf}" ]]; then up_run="$(get_today_run_json "${up_wf}")"; fi
  if [[ -n "${st_wf}" ]]; then st_run="$(get_today_run_json "${st_wf}")"; fi
  if [[ -n "${sb_wf}" ]]; then sb_run="$(get_today_run_json "${sb_wf}")"; fi

  # Fallback: if pinned/stable (or any column) has no "today" run, still show latest run
  # so users can see in-progress nightlies that started near midnight UTC.
  if [[ "${up_run}" == "null" && -n "${up_wf}" ]]; then up_run="$(get_latest_run_json "${up_wf}")"; fi
  if [[ "${st_run}" == "null" && -n "${st_wf}" ]]; then st_run="$(get_latest_run_json "${st_wf}")"; fi
  if [[ "${sb_run}" == "null" && -n "${sb_wf}" ]]; then sb_run="$(get_latest_run_json "${sb_wf}")"; fi

  # Get status cell - use job-level filtering if JOB_FILTER is defined
  if [[ -n "${job_filter}" ]]; then
    up_cell="$(cell_for_filtered_jobs "${up_run}" "${job_filter}")"
    st_cell="$(cell_for_filtered_jobs "${st_run}" "${job_filter}")"
    sb_cell="$(cell_for_filtered_jobs "${sb_run}" "${job_filter}")"
  else
    up_cell="$(cell_for_run <<<"${up_run}")"
    st_cell="$(cell_for_run <<<"${st_run}")"
    sb_cell="$(cell_for_run <<<"${sb_run}")"
  fi

  # Use display name if available, otherwise use row name
  display_row="${DISPLAY_NAME[$row]:-$row}"

  if [[ "${SLACK_FORMAT}" == "true" ]]; then
    printf "| %-4s | %-18s | %-9s | %-10s | %-8s |\n" "${row_no}" "${display_row}" "${st_cell}" "${up_cell}" "${sb_cell}"
  else
    printf "%-4s | %-18s | %-8s | %-8s | %-8s\n" "${row_no}" "${display_row}" "${st_cell}" "${up_cell}" "${sb_cell}"
  fi

  # Collect failure details (completed + non-success).
  # Use the cell status which already accounts for job filtering
  EMOJI_CROSS=$'\xe2\x9d\x8c'     # ❌ cross mark
  EMOJI_HOURGLASS=$'\xe2\x8f\xb3' # ⏳ hourglass
  for col in staging upstream stable; do
    case "${col}" in
      upstream) wf="${up_wf}"; run="${up_run}"; cell="${up_cell}";;
      staging)  wf="${st_wf}"; run="${st_run}"; cell="${st_cell}";;
      stable)   wf="${sb_wf}"; run="${sb_run}"; cell="${sb_cell}";;
    esac
    if [[ "${run}" != "null" && -n "${wf}" ]]; then
      if [[ "${cell}" == "${EMOJI_CROSS}" ]]; then
        fail_labels+=("${row} / ${col^}")
        fail_runs+=("${run}")
        fail_filters+=("${job_filter}")
      elif [[ "${cell}" == "${EMOJI_HOURGLASS}" ]]; then
        inprog_labels+=("${row} / ${col^}")
        inprog_wfs+=("${wf}")
        inprog_runs+=("${run}")
        inprog_filters+=("${job_filter}")
      fi
    fi
  done

  row_no=$((row_no + 1))
done

if [[ "${SLACK_FORMAT}" == "true" ]]; then
  echo ""
  echo "---"
  echo ""
  echo "*🔴 Failure Details:*"

  if [[ "${#fail_runs[@]}" -eq 0 ]]; then
    echo ""
    echo "_No failures detected for ${TODAY_UTC}._"
    exit 0
  fi

  # Phase 1: Pre-compute a step-based signature for every failure so we can
  # group identical errors that appear in multiple builds (Staging + Upstream, etc.).
  declare -a fail_sig=()
  for i in "${!fail_runs[@]}"; do
    sig_run_id="$(jq -r '.databaseId' <<<"${fail_runs[$i]}")"
    sig_result="$(compute_failure_signature "${sig_run_id}" "${fail_filters[$i]}")"
    fail_sig+=("${sig_result:-unique_${i}}")
  done

  # Phase 2: Print failures grouped by signature — each unique error shown once.
  declare -A dedup_seen_sigs=()
  fail_idx=1
  fail_first_printed=true
  for i in "${!fail_runs[@]}"; do
    dedup_sig="${fail_sig[$i]}"
    [[ -n "${dedup_seen_sigs[${dedup_sig}]:-}" ]] && continue
    dedup_seen_sigs["${dedup_sig}"]=1

    # Collect all other failures sharing this signature (same error, different build)
    dedup_extra_affects=""
    for j in "${!fail_runs[@]}"; do
      [[ "$j" == "$i" ]] && continue
      [[ "${fail_sig[$j]}" != "${dedup_sig}" ]] && continue
      dedup_extra_affects+="${fail_labels[$j]}"$'\t'"$(jq -r '.url' <<<"${fail_runs[$j]}")"$'\n'
    done

    if [[ "${fail_first_printed}" != "true" ]]; then
      echo ""
      echo "---"
    fi
    fail_first_printed=false

    print_slack_failure_details "${fail_idx}" "${fail_labels[$i]}" "${fail_runs[$i]}" "${fail_filters[$i]}" "${dedup_extra_affects}"
    fail_idx=$((fail_idx + 1))
  done

  if [[ "${#inprog_runs[@]}" -gt 0 ]]; then
    echo ""
    echo "---"
    echo ""
    echo "*⏳ In-Progress Details:*"
    inprog_idx=1
    for i in "${!inprog_runs[@]}"; do
      print_slack_in_progress_details "${inprog_idx}" "${inprog_labels[$i]}" "${inprog_wfs[$i]}" "${inprog_runs[$i]}"
      inprog_idx=$((inprog_idx + 1))
    done
  fi
else
  echo ""
  echo "2. Failure Details:"

  if [[ "${#fail_runs[@]}" -eq 0 ]]; then
    echo ""
    echo "(No failures detected for ${TODAY_UTC}.)"
    exit 0
  fi

  for i in "${!fail_runs[@]}"; do
    print_failure_details "${fail_labels[$i]}" "${fail_runs[$i]}" "${fail_filters[$i]}"
  done

  if [[ "${#inprog_runs[@]}" -gt 0 ]]; then
    echo ""
    echo "3. In-progress Details:"
    for i in "${!inprog_runs[@]}"; do
      print_in_progress_details "${inprog_labels[$i]}" "${inprog_wfs[$i]}" "${inprog_runs[$i]}"
    done
  fi
fi
