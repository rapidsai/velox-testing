#!/usr/bin/env bash
set -euo pipefail

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
#   --cause         Use AI to analyze logs and determine failure cause
#   --fix           Use AI to suggest a fix (implies --cause)
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

# --- Argument parsing ---
PRINT_LOGS="false"
SLACK_FORMAT="true"
ANALYZE_CAUSE="false"
ANALYZE_FIX="false"

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
    --fix)
      ANALYZE_FIX="true"
      ANALYZE_CAUSE="true"  # --fix implies --cause
      shift
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
      local job_log_content=""
      if [[ -n "${log_out}" ]]; then
        # Filter logs by job name (first field before tab) - escape special chars in job name
        local escaped_job_name
        escaped_job_name=$(printf '%s' "${job_name}" | sed 's/[[\.*^$()+?{|]/\\&/g')
        job_log_content=$(echo "${log_out}" | grep -E "^${escaped_job_name}"$'\t' | \
          awk '{print} /Post job cleanup\./ {exit}' | tail -n "${LOG_TAIL_LINES}" || true)
      fi
      
      # Analyze this job's logs with AI (only if --cause flag is set)
      if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
        local ai_response cause fix stacktrace
        if [[ -n "${job_log_content}" ]]; then
          ai_response="$(analyze_logs_with_ai "${job_log_content}" "${job_name}" "${wf_name}")"
        else
          ai_response="STACKTRACE:Unable to fetch logs for this job
END_STACKTRACE
CAUSE:Unable to fetch logs for this job
FIX:Check the run link above for details"
        fi
        
        # Parse STACKTRACE (multiline, between STACKTRACE: and END_STACKTRACE)
        stacktrace="$(echo "${ai_response}" | sed -n '/^STACKTRACE:/,/^END_STACKTRACE/p' | sed '1s/^STACKTRACE:[[:space:]]*//' | sed '/^END_STACKTRACE/d')"
        # Parse CAUSE and FIX from response (guard against no matches)
        cause="$(echo "${ai_response}" | grep -i "^CAUSE:" | sed 's/^CAUSE:[[:space:]]*//' | head -1 || true)"
        fix="$(echo "${ai_response}" | grep -i "^FIX:" | sed 's/^FIX:[[:space:]]*//' | head -1 || true)"
        
        # Fallback if parsing failed
        if [[ -z "${stacktrace}" ]]; then
          stacktrace="Unable to extract stacktrace"
        fi
        if [[ -z "${cause}" ]]; then
          cause="Unable to determine cause"
        fi
        if [[ -z "${fix}" ]]; then
          fix="Pending investigation"
        fi

        # Print stacktrace (LLM-extracted relevant error portion)
        # Strip any backticks the LLM may have included
        local clean_stacktrace
        clean_stacktrace="$(echo "${stacktrace}" | sed '/^```/d; /```$/d' | sed '/^$/d')"
        if [[ -n "${clean_stacktrace}" ]]; then
          echo "    - Stacktrace:"
          echo '```'
          echo "${clean_stacktrace}"
          echo '```'
        else
          echo "    - Stacktrace: _Unavailable_"
        fi
        echo "    - Cause: _${cause}_"
        if [[ "${ANALYZE_FIX}" == "true" ]]; then
          echo "    - Fix: _${fix}_"
        fi
      fi
      
    done <<< "${failed_jobs}"
  else
    echo "  - WARN: failed to fetch job/step details (network/API timeout). See run link above." >&2
  fi

  # Print all logs if enabled (combined for all jobs)
  if [[ "${PRINT_LOGS}" == "true" ]]; then
    echo ""
    echo "  Failed log tail (last ${LOG_TAIL_LINES} lines):"
    if [[ -n "${log_out}" ]]; then
      local log_content
      log_content="$(echo "${log_out}" | awk '{print} /Post job cleanup\./ {exit}' | tail -n "${LOG_TAIL_LINES}" || true)"
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
        LOG_TAIL_LINES="${LOG_TAIL_LINES:-150}"
        echo ""
        echo "  First attempt failed log tail (last ${LOG_TAIL_LINES} lines):"
        if log_out="$(gh_retry gh run view -R "${REPO}" "${first_id}" --log-failed 2>/dev/null)"; then
          echo "${log_out}" \
            | awk '{print} /Post job cleanup\./ {exit}' \
            | tail -n "${LOG_TAIL_LINES}" \
            | sed 's/^/    /'
        else
          echo "    (WARN) Unable to fetch logs. Open the run link above for full logs."
        fi
      fi
    fi
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
  
  # Truncate logs if too long (keep last ~12000 chars to capture full error context)
  local truncated_logs
  if [[ ${#log_content} -gt 12000 ]]; then
    truncated_logs="[...truncated...]\n$(echo "${log_content}" | tail -c 12000)"
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

  local run_id run_url wf_name conclusion
  run_id="$(jq -r '.databaseId' <<<"${run_json}")"
  run_url="$(jq -r '.url' <<<"${run_json}")"
  wf_name="$(jq -r '.workflowName' <<<"${run_json}")"
  conclusion="$(jq -r '.conclusion' <<<"${run_json}")"

  echo ""
  echo "*${idx}. ${label}*"
  echo "• *Workflow:* ${wf_name}"
  echo "• *Run:* ${run_url}"
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
      local job_log_content=""
      if [[ -n "${log_out}" ]]; then
        # Filter logs by job name (first field before tab) - escape special chars in job name
        local escaped_job_name
        escaped_job_name=$(printf '%s' "${job_name}" | sed 's/[[\.*^$()+?{|]/\\&/g')
        job_log_content=$(echo "${log_out}" | grep -E "^${escaped_job_name}"$'\t' | \
          awk '{print} /Post job cleanup\./ {exit}' | tail -n "${LOG_TAIL_LINES}" || true)
      fi
      
      # Analyze this job's logs with AI (only if --cause flag is set)
      if [[ "${ANALYZE_CAUSE}" == "true" ]]; then
        local ai_response cause fix stacktrace
        if [[ -n "${job_log_content}" ]]; then
          ai_response="$(analyze_logs_with_ai "${job_log_content}" "${job_name}" "${wf_name}")"
        else
          ai_response="STACKTRACE:Unable to fetch logs for this job
END_STACKTRACE
CAUSE:Unable to fetch logs for this job
FIX:Check the run link above for details"
        fi
        
        # Parse STACKTRACE (multiline, between STACKTRACE: and END_STACKTRACE)
        stacktrace="$(echo "${ai_response}" | sed -n '/^STACKTRACE:/,/^END_STACKTRACE/p' | sed '1s/^STACKTRACE:[[:space:]]*//' | sed '/^END_STACKTRACE/d')"
        # Parse CAUSE and FIX from response (guard against no matches)
        cause="$(echo "${ai_response}" | grep -i "^CAUSE:" | sed 's/^CAUSE:[[:space:]]*//' | head -1 || true)"
        fix="$(echo "${ai_response}" | grep -i "^FIX:" | sed 's/^FIX:[[:space:]]*//' | head -1 || true)"
        
        # Fallback if parsing failed
        if [[ -z "${stacktrace}" ]]; then
          stacktrace="_Unable to extract stacktrace_"
        fi
        if [[ -z "${cause}" ]]; then
          cause="_Unable to determine cause_"
        fi
        if [[ -z "${fix}" ]]; then
          fix="_Pending investigation_"
        fi

        # Print stacktrace (LLM-extracted relevant error portion)
        # Strip any backticks the LLM may have included
        local clean_stacktrace
        clean_stacktrace="$(echo "${stacktrace}" | sed '/^```/d; /```$/d' | sed '/^$/d')"
        if [[ -n "${clean_stacktrace}" ]]; then
          echo "    *Stacktrace:*"
          echo '```'
          echo "${clean_stacktrace}"
          echo '```'
        else
          echo "    *Stacktrace:* _Unavailable_"
        fi
        echo "    *Cause:* _${cause}_"
        if [[ "${ANALYZE_FIX}" == "true" ]]; then
          echo "    *Fix:* _${fix}_"
        fi
      fi
      
    done <<< "${failed_jobs}"
  else
    echo "  ◦ _WARN: failed to fetch job/step details (network/API timeout). See run link above._" >&2
  fi

  # Print all logs if enabled (combined for all jobs)
  if [[ "${PRINT_LOGS}" == "true" ]]; then
    echo ""
    echo "*Failed log tail (last ${LOG_TAIL_LINES} lines):*"
    if [[ -n "${log_out}" ]]; then
      local log_content
      log_content="$(echo "${log_out}" | awk '{print} /Post job cleanup\./ {exit}' | tail -n "${LOG_TAIL_LINES}" || true)"
      echo '```'
      echo "${log_content}"
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
        LOG_TAIL_LINES="${LOG_TAIL_LINES:-150}"
        echo ""
        echo "*First attempt failed log tail (last ${LOG_TAIL_LINES} lines):*"
        if log_out="$(gh_retry gh run view -R "${REPO}" "${first_id}" --log-failed 2>/dev/null)"; then
          echo '```'
          # Use || true to prevent pipefail from exiting before closing backticks
          echo "${log_out}" \
            | awk '{print} /Post job cleanup\./ {exit}' \
            | tail -n "${LOG_TAIL_LINES}" || true
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

  fail_idx=1
  for i in "${!fail_runs[@]}"; do
    print_slack_failure_details "${fail_idx}" "${fail_labels[$i]}" "${fail_runs[$i]}" "${fail_filters[$i]}"
    if [[ $((i + 1)) -lt "${#fail_runs[@]}" ]]; then
      echo ""
      echo "---"
    fi
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

