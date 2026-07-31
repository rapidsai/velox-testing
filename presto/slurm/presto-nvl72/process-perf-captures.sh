#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Generate reports from raw host-perf captures after a benchmark. When invoked
# outside Slurm, this script starts one CPU-variant compute allocation so that
# perf unwinds the capture on the same architecture that recorded it. Raw data
# and the portable symfs are staged onto that node's local disk; only derived
# artifacts are copied back to the shared result directory.

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename -- "${BASH_SOURCE[0]}")"
FLAMEGRAPH_SCRIPT="${SCRIPT_DIR}/../../scripts/perf_flamegraph.py"

worker_id=0
query_id=""
force=0
no_inline=0
local_only=0
requested_jobs="${PERF_POSTPROCESS_JOBS:-0}"
compute_time="${PERF_POSTPROCESS_TIME:-00:30:00}"
result_dir=""
worker_root=""

usage() {
  cat <<EOF
Usage:
  $0 RESULT_DIR [OPTIONS]
  $0 --worker-root DIR [OPTIONS]

Generate perf-report.txt, perf.script.gz, stacks.folded, and flamegraph.html
from already captured perf.data files or perf.data.partNNN shards. RESULT_DIR
is a completed result_dir_<jobid>.

Outside an existing Slurm allocation, the default is to submit one blocking
CPU compute-node step with srun. The capture and symfs are copied to node-local
storage, shards are symbolized in parallel, and derived files are atomically
published back into RESULT_DIR.

Options:
  --worker-id N       Worker ID below RESULT_DIR/profiles/perf (default: 0)
  --worker-root DIR   Process this profiles/perf/worker_<N> directory directly
  --query ID          Process one capture directory, e.g. Q18_iter1
  --no-inline         Disable inline-function expansion in perf report/script
  --jobs N            Maximum parallel perf processes (default: raw shard count)
  --time LIMIT        Compute allocation time limit (default: ${compute_time})
  --local             Do not launch srun; process on this host/allocation
  --force             Reprocess a matching completed capture
  -h, --help          Show this help

Environment:
  CLUSTER_CONFIG                 Cluster config path (default:
                                 ~/.cluster_config.env)
  PERF_POSTPROCESS_PARTITION     Override CLUSTER_CPU_PARTITION
  PERF_POSTPROCESS_ACCOUNT       Override CLUSTER_CPU_ACCOUNT
  PERF_POSTPROCESS_TMPDIR        Node-local staging base (default:
                                 \${SLURM_TMPDIR:-/tmp})
  PERF_POSTPROCESS_KEEP_TMP=1    Retain successful staging directories
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)
      worker_id="${2:?missing --worker-id value}"
      shift 2
      ;;
    --worker-root)
      worker_root="${2:?missing --worker-root value}"
      shift 2
      ;;
    --query)
      query_id="${2:?missing --query value}"
      shift 2
      ;;
    --no-inline)
      no_inline=1
      shift
      ;;
    --jobs)
      requested_jobs="${2:?missing --jobs value}"
      shift 2
      ;;
    --time)
      compute_time="${2:?missing --time value}"
      shift 2
      ;;
    --local)
      local_only=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -n "${result_dir}" ]]; then
        echo "Error: only one RESULT_DIR may be specified" >&2
        usage >&2
        exit 2
      fi
      result_dir="$1"
      shift
      ;;
  esac
done

[[ "${worker_id}" =~ ^[0-9]+$ ]] || {
  echo "Error: invalid worker ID: ${worker_id}" >&2
  exit 2
}
[[ "${requested_jobs}" =~ ^[0-9]+$ ]] || {
  echo "Error: --jobs must be a positive integer, or 0 for automatic; got ${requested_jobs}" >&2
  exit 2
}
[[ "${compute_time}" != *[[:space:]]* && -n "${compute_time}" ]] || {
  echo "Error: invalid --time value: ${compute_time}" >&2
  exit 2
}
if [[ -n "${query_id}" && ! "${query_id}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "Error: unsafe query profile identifier: ${query_id}" >&2
  exit 2
fi
if [[ -n "${worker_root}" && -n "${result_dir}" ]]; then
  echo "Error: specify RESULT_DIR or --worker-root, not both" >&2
  exit 2
fi
if [[ -z "${worker_root}" ]]; then
  [[ -n "${result_dir}" ]] || {
    echo "Error: RESULT_DIR or --worker-root is required" >&2
    usage >&2
    exit 2
  }
  worker_root="${result_dir%/}/profiles/perf/worker_${worker_id}"
fi

[[ -d "${worker_root}" ]] || {
  echo "Error: perf worker directory not found: ${worker_root}" >&2
  exit 1
}
worker_root="$(cd -- "${worker_root}" >/dev/null 2>&1 && pwd -P)"
[[ -d "${worker_root}/symfs" ]] || {
  echo "Error: symbol filesystem not found: ${worker_root}/symfs" >&2
  exit 1
}
[[ -f "${FLAMEGRAPH_SCRIPT}" ]] || {
  echo "Error: bundled flamegraph renderer not found: ${FLAMEGRAPH_SCRIPT}" >&2
  exit 1
}

shopt -s nullglob

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

# Populate capture_data_files for one capture directory.
capture_data_files=()
find_capture_data_files() {
  local -r session_dir="$1"
  local data_file=""
  capture_data_files=()
  if [[ -s "${session_dir}/perf.data" ]]; then
    capture_data_files+=("${session_dir}/perf.data")
  fi
  for data_file in "${session_dir}"/perf.data.part*; do
    [[ -s "${data_file}" ]] && capture_data_files+=("${data_file}")
  done
}

capture_validation_error=""
validate_capture() {
  local -r session_dir="$1"
  local -r metadata="${session_dir}/capture-metadata.txt"
  local -r manifest="${session_dir}/perf-shards.tsv"
  local expected_shards=""
  local explicit_valid=""

  capture_validation_error=""
  if [[ ! -s "${metadata}" ]]; then
    capture_validation_error="capture metadata is missing"
    return 1
  fi
  explicit_valid="$(awk -F= '$1 == "capture_valid" { value=$2 } END { print value }' "${metadata}")"
  if [[ "${explicit_valid}" == "0" ]]; then
    capture_validation_error="capture metadata marks the recording invalid"
    return 1
  fi
  if ! grep -qx 'perf_control_handshake=enabled' "${metadata}"; then
    capture_validation_error="not every perf shard completed the enable handshake"
    return 1
  fi
  if ! grep -qx 'capture_phase=queued' "${metadata}"; then
    capture_validation_error="recorders did not stop and queue a complete raw capture"
    return 1
  fi
  if ! grep -qx 'capture_phase=published' "${metadata}"; then
    capture_validation_error="raw capture publication did not complete"
    return 1
  fi
  if [[ ! -s "${manifest}" ]]; then
    capture_validation_error="perf shard manifest is missing"
    return 1
  fi
  if ! awk -F '\t' '
      NR > 1 {
        count++
        if ($2 != "stopped" || ($9 != "0" && $9 != "130")) {
          invalid=1
        }
      }
      END { exit !(count > 0 && !invalid) }
    ' "${manifest}"; then
    capture_validation_error="one or more perf shards did not stop cleanly"
    return 1
  fi

  find_capture_data_files "${session_dir}"
  expected_shards="$(awk -F= '$1 == "perf_data_shards" { value=$2 } END { print value }' "${metadata}")"
  if [[ ! "${expected_shards}" =~ ^[1-9][0-9]*$ ]]; then
    capture_validation_error="published shard count is missing from capture metadata"
    return 1
  fi
  if (( ${#capture_data_files[@]} != expected_shards )); then
    capture_validation_error="found ${#capture_data_files[@]} raw shard(s), expected ${expected_shards}"
    return 1
  fi
}

session_dirs=()
max_shards=0
invalid_capture_count=0
if [[ -n "${query_id}" ]]; then
  [[ -d "${worker_root}/${query_id}" ]] || {
    echo "Error: perf capture directory not found: ${worker_root}/${query_id}" >&2
    exit 1
  }
  if ! validate_capture "${worker_root}/${query_id}"; then
    echo "Error: ${query_id} is not a complete perf capture: ${capture_validation_error}" >&2
    exit 1
  fi
  session_dirs=("${worker_root}/${query_id}")
  max_shards="${#capture_data_files[@]}"
else
  for candidate in "${worker_root}"/*; do
    [[ -d "${candidate}" ]] || continue
    candidate_name="$(basename -- "${candidate}")"
    [[ "${candidate_name}" =~ ^[A-Za-z0-9_.-]+$ ]] || {
      log "Skipping unsafe capture directory name: ${candidate_name}"
      continue
    }
    [[ -s "${candidate}/capture-metadata.txt" ]] || continue
    if ! validate_capture "${candidate}"; then
      log "ERROR ${candidate_name}: refusing incomplete perf capture (${capture_validation_error})"
      ((invalid_capture_count += 1))
      continue
    fi
    session_dirs+=("${candidate}")
    if (( ${#capture_data_files[@]} > max_shards )); then
      max_shards="${#capture_data_files[@]}"
    fi
  done
fi

if (( ${#session_dirs[@]} == 0 )); then
  echo "Error: no perf.data captures or shards found below ${worker_root}" >&2
  exit 1
fi

parallel_jobs="${max_shards}"
if (( requested_jobs > 0 && requested_jobs < parallel_jobs )); then
  parallel_jobs="${requested_jobs}"
fi
(( parallel_jobs > 0 )) || parallel_jobs=1

load_cluster_config() {
  local cluster_config="${CLUSTER_CONFIG:-${HOME}/.cluster_config.env}"
  if [[ -f "${cluster_config}" ]]; then
    # shellcheck disable=SC1090
    source "${cluster_config}"
  else
    log "WARNING cluster config not found at ${cluster_config}; srun will use Slurm defaults"
  fi
}

launch_compute_postprocessor() {
  local cluster_cpu_limit=""
  local partition=""
  local account=""
  local -a srun_args=()
  local -a forwarded_args=()

  command -v srun >/dev/null 2>&1 || {
    echo "Error: srun is not installed; run on a compatible compute node with --local" >&2
    return 1
  }

  load_cluster_config
  cluster_cpu_limit="${CLUSTER_CPU_CPUS_PER_TASK:-}"
  if [[ "${cluster_cpu_limit}" =~ ^[1-9][0-9]*$ ]] && (( parallel_jobs > cluster_cpu_limit )); then
    log "Capping --jobs from ${parallel_jobs} to configured CPU allocation ${cluster_cpu_limit}"
    parallel_jobs="${cluster_cpu_limit}"
  fi
  partition="${PERF_POSTPROCESS_PARTITION:-${CLUSTER_CPU_PARTITION:-}}"
  account="${PERF_POSTPROCESS_ACCOUNT:-${CLUSTER_CPU_ACCOUNT:-}}"

  srun_args=(
    --nodes=1
    --ntasks=1
    --cpus-per-task="${parallel_jobs}"
    --time="${compute_time}"
    --job-name=presto-perf-postprocess
    --kill-on-bad-exit=1
    --export=ALL
  )
  [[ -n "${partition}" ]] && srun_args+=(--partition="${partition}")
  [[ -n "${account}" ]] && srun_args+=(--account="${account}")

  forwarded_args=(
    --worker-root "${worker_root}"
    --worker-id "${worker_id}"
    --jobs "${parallel_jobs}"
    --time "${compute_time}"
  )
  [[ -n "${query_id}" ]] && forwarded_args+=(--query "${query_id}")
  (( no_inline )) && forwarded_args+=(--no-inline)
  (( force )) && forwarded_args+=(--force)

  log "Launching Arm-compatible compute postprocessor (${parallel_jobs} parallel perf process(es), time ${compute_time})"
  (
    export PRESTO_PERF_POSTPROCESS_COMPUTE=1
    exec srun "${srun_args[@]}" bash "${SCRIPT_PATH}" "${forwarded_args[@]}"
  )
}

# The sidecar already calls this script inside the benchmark allocation.
# PRESTO_PERF_POSTPROCESS_COMPUTE also prevents recursion in lightweight srun
# wrappers that do not expose SLURM_JOB_ID to the launched shell.
if (( ! local_only )) \
    && [[ -z "${SLURM_JOB_ID:-}" ]] \
    && [[ "${PRESTO_PERF_POSTPROCESS_COMPUTE:-0}" != "1" ]]; then
  launch_compute_postprocessor
  exit $?
fi

for required_command in perf python3 gzip file mktemp cp sort; do
  command -v "${required_command}" >/dev/null 2>&1 || {
    echo "Error: ${required_command} is not installed on $(hostname)" >&2
    exit 1
  }
done

normalize_arch() {
  case "$1" in
    aarch64|arm64) printf 'aarch64\n' ;;
    x86_64|amd64) printf 'x86_64\n' ;;
    *) printf '%s\n' "$1" ;;
  esac
}

captured_worker="${worker_root}/symfs/usr/bin/presto_server"
if [[ ! -f "${captured_worker}" ]]; then
  captured_worker="$(find "${worker_root}/symfs" -type f -name presto_server -print -quit 2>/dev/null || true)"
fi
if [[ -n "${captured_worker}" ]]; then
  captured_file_description="$(file -L "${captured_worker}" 2>/dev/null || true)"
  case "${captured_file_description}" in
    *aarch64*|*"ARM aarch64"*) captured_arch="aarch64" ;;
    *x86-64*|*x86_64*) captured_arch="x86_64" ;;
    *) captured_arch="" ;;
  esac
  current_arch="$(normalize_arch "$(uname -m)")"
  if [[ -n "${captured_arch}" && "${captured_arch}" != "${current_arch}" ]]; then
    echo "Error: capture contains a ${captured_arch} worker, but $(hostname) is ${current_arch}." >&2
    echo "       Omit --local so the script can launch on the CPU compute partition." >&2
    exit 1
  fi
else
  captured_arch=""
  current_arch="$(normalize_arch "$(uname -m)")"
  log "WARNING could not locate presto_server in symfs for an architecture preflight"
fi

# perf can open one DSO descriptor per mapped image during symbolization. Use
# all descriptor headroom granted to the postprocessing allocation.
nofile_soft_before="$(ulimit -Sn)"
nofile_hard="$(ulimit -Hn)"
if [[ "${nofile_soft_before}" != "${nofile_hard}" ]]; then
  ulimit -Sn "${nofile_hard}" 2>/dev/null || true
fi

stage_base="${PERF_POSTPROCESS_TMPDIR:-${SLURM_TMPDIR:-/tmp}}"
mkdir -p "${stage_base}" || {
  echo "Error: cannot create node-local staging base: ${stage_base}" >&2
  exit 1
}
stage_base="$(cd -- "${stage_base}" >/dev/null 2>&1 && pwd -P)"
stage_root="$(mktemp -d "${stage_base}/presto-perf-postprocess.${SLURM_JOB_ID:-manual}.XXXXXX")" || {
  echo "Error: cannot create a node-local staging directory below ${stage_base}" >&2
  exit 1
}

cleanup_stage() {
  local rc=$?
  trap - EXIT
  if [[ -n "${stage_root:-}" && -d "${stage_root}" ]]; then
    if [[ "${PERF_POSTPROCESS_KEEP_TMP:-0}" == "1" ]]; then
      log "Retaining node-local staging directory: ${stage_root}"
    else
      case "${stage_root}" in
        "${stage_base}"/presto-perf-postprocess.*)
          rm -rf -- "${stage_root}"
          ;;
        *)
          log "WARNING refusing to remove unexpected staging path: ${stage_root}"
          ;;
      esac
    fi
  fi
  exit "${rc}"
}
trap cleanup_stage EXIT

copy_without_metadata() {
  cp --no-preserve=mode,ownership,timestamps -- "$1" "$2"
}

stage_worker="${stage_root}/worker_${worker_id}"
stage_symfs="${stage_worker}/symfs"
mkdir -p "${stage_symfs}"
log "Staging symfs on $(hostname): ${stage_root}"
cp -R --reflink=auto --no-preserve=mode,ownership,timestamps \
  -- "${worker_root}/symfs/." "${stage_symfs}/" || {
  echo "Error: could not stage ${worker_root}/symfs" >&2
  exit 1
}

perf_inline_args=()
inline_expansion="enabled"
if (( no_inline )); then
  perf_inline_args+=(--no-inline)
  inline_expansion="disabled"
fi

batch_pids=()
parallel_failed=0
wait_for_batch() {
  local pid=""
  for pid in "${batch_pids[@]}"; do
    if ! wait "${pid}"; then
      parallel_failed=1
    fi
  done
  batch_pids=()
}

discard_staged_session() {
  local -r staged_session="$1"
  [[ "${PERF_POSTPROCESS_KEEP_TMP:-0}" != "1" ]] || return 0
  case "${staged_session}" in
    "${stage_worker}"/*)
      rm -rf -- "${staged_session}"
      ;;
    *)
      log "WARNING refusing to remove unexpected staged capture path: ${staged_session}"
      ;;
  esac
}

publish_derived_files() {
  local -r staged_session="$1"
  local -r destination_session="$2"
  local -r successful="$3"
  local filename=""
  local pending=""
  local publishing_file=""
  local -a files_to_publish=()
  local -a derived_names=(
    build-ids.txt
    build-ids.stderr.txt
    perf-report.txt
    perf-report.stderr.txt
    perf-script.stderr.txt
    perf.script.gz
    stacks.folded
    flamegraph.html
    flamegraph-generator.txt
    flamegraph-unavailable.txt
    postprocess-metadata.txt
  )

  # Copy every new artifact under a hidden temporary name before changing the
  # published set. A shared-filesystem copy failure therefore leaves the
  # previous complete result intact. Raw perf.data and capture metadata are
  # never touched.
  for filename in "${derived_names[@]}"; do
    [[ -f "${staged_session}/${filename}" ]] || continue
    publishing_file="${destination_session}/.${filename}.publishing.${SLURM_JOB_ID:-$$}"
    rm -f "${publishing_file}"
    if ! copy_without_metadata "${staged_session}/${filename}" "${publishing_file}"; then
      echo "Error: could not publish ${filename} to ${destination_session}" >&2
      rm -f "${publishing_file}"
      for pending in "${files_to_publish[@]}"; do
        rm -f "${destination_session}/.${pending}.publishing.${SLURM_JOB_ID:-$$}"
      done
      return 1
    fi
    files_to_publish+=("${filename}")
  done

  rm -f \
    "${destination_session}/.postprocess-complete" \
    "${destination_session}/.postprocess-failed"
  for filename in "${derived_names[@]}"; do
    if [[ ! -f "${staged_session}/${filename}" ]]; then
      rm -f "${destination_session}/${filename}"
    fi
  done
  for filename in "${files_to_publish[@]}"; do
    publishing_file="${destination_session}/.${filename}.publishing.${SLURM_JOB_ID:-$$}"
    if ! mv -f -- "${publishing_file}" "${destination_session}/${filename}"; then
      for pending in "${files_to_publish[@]}"; do
        rm -f "${destination_session}/.${pending}.publishing.${SLURM_JOB_ID:-$$}"
      done
      return 1
    fi
  done
  if [[ "${successful}" == "1" ]]; then
    touch "${destination_session}/.postprocess-complete"
  else
    touch "${destination_session}/.postprocess-failed"
  fi
}

process_capture() {
  local -r source_session="$1"
  local -r qid="$(basename -- "${source_session}")"
  local -r staged_session="${stage_worker}/${qid}"
  local -r script_file="${staged_session}/perf.script"
  local -r generator_log="${staged_session}/flamegraph-generator.txt"
  local data_file=""
  local part_name=""
  local part_file=""
  local total_data_bytes=0
  local index=0
  local report_failed=0
  local renderer_status="failed"
  local existing_inline=""
  local -a data_files=()
  local -a staged_data_files=()
  local -a part_files=()

  if [[ -f "${source_session}/.postprocess-complete" && "${force}" != "1" ]]; then
    existing_inline="$(awk -F= '$1 == "postprocess_inline_expansion" { print $2 }' \
      "${source_session}/postprocess-metadata.txt" 2>/dev/null | tail -1)"
    if [[ -s "${source_session}/flamegraph.html" \
          && -s "${source_session}/perf.script.gz" \
          && "${existing_inline}" == "${inline_expansion}" ]]; then
      log "Skipping ${qid}; already post-processed with inline expansion ${inline_expansion} (use --force to regenerate)"
      return 0
    fi
    log "Reprocessing ${qid}; existing marker is incomplete or used a different inline mode"
  fi

  find_capture_data_files "${source_session}"
  data_files=("${capture_data_files[@]}")
  if (( ${#data_files[@]} == 0 )); then
    log "ERROR ${qid}: missing or empty perf.data capture"
    return 1
  fi

  mkdir -p "${staged_session}"
  for data_file in "${data_files[@]}"; do
    ((total_data_bytes += $(stat -c '%s' "${data_file}")))
    copy_without_metadata "${data_file}" "${staged_session}/$(basename -- "${data_file}")" || {
      log "ERROR ${qid}: could not stage $(basename -- "${data_file}")"
      return 1
    }
    staged_data_files+=("${staged_session}/$(basename -- "${data_file}")")
  done

  log "Processing ${qid} (${#staged_data_files[@]} raw shard(s), ${total_data_bytes} bytes, jobs=${parallel_jobs}, inline=${inline_expansion})"

  log "${qid}: build IDs"
  parallel_failed=0
  batch_pids=()
  index=0
  for data_file in "${staged_data_files[@]}"; do
    printf -v part_name 'part%03d' "${index}"
    (
      perf buildid-list -i "${data_file}" \
        > "${staged_session}/.build-ids.${part_name}.txt" \
        2> "${staged_session}/.build-ids.${part_name}.stderr.txt"
    ) &
    batch_pids+=("$!")
    ((index += 1))
    if (( ${#batch_pids[@]} >= parallel_jobs )); then
      wait_for_batch
    fi
  done
  wait_for_batch
  : > "${staged_session}/build-ids.txt"
  : > "${staged_session}/build-ids.stderr.txt"
  for part_file in "${staged_session}"/.build-ids.part*.txt; do
    cat "${part_file}" >> "${staged_session}/build-ids.txt"
  done
  for part_file in "${staged_session}"/.build-ids.part*.stderr.txt; do
    cat "${part_file}" >> "${staged_session}/build-ids.stderr.txt"
  done
  sort -u -o "${staged_session}/build-ids.txt" "${staged_session}/build-ids.txt"
  rm -f "${staged_session}"/.build-ids.part*.txt "${staged_session}"/.build-ids.part*.stderr.txt

  log "${qid}: parallel text reports"
  parallel_failed=0
  batch_pids=()
  index=0
  for data_file in "${staged_data_files[@]}"; do
    printf -v part_name 'part%03d' "${index}"
    (
      perf report "${perf_inline_args[@]}" \
        --stdio --no-children --percent-limit 0.1 \
        --symfs "${stage_symfs}" -i "${data_file}" \
        > "${staged_session}/.perf-report.${part_name}.txt" \
        2> "${staged_session}/.perf-report.${part_name}.stderr.txt"
    ) &
    batch_pids+=("$!")
    ((index += 1))
    if (( ${#batch_pids[@]} >= parallel_jobs )); then
      wait_for_batch
    fi
  done
  wait_for_batch
  (( parallel_failed )) && report_failed=1

  : > "${staged_session}/perf-report.txt"
  : > "${staged_session}/perf-report.stderr.txt"
  if (( ${#staged_data_files[@]} > 1 )); then
    echo "This logical capture used multiple perf recorder shards. Percentages below are local to each named raw shard; flamegraph.html and perf.script.gz aggregate samples from all shards." \
      >> "${staged_session}/perf-report.txt"
  fi
  index=0
  for data_file in "${staged_data_files[@]}"; do
    printf -v part_name 'part%03d' "${index}"
    printf '\n===== %s =====\n' "$(basename -- "${data_file}")" >> "${staged_session}/perf-report.txt"
    cat "${staged_session}/.perf-report.${part_name}.txt" >> "${staged_session}/perf-report.txt"
    if [[ -s "${staged_session}/.perf-report.${part_name}.stderr.txt" ]]; then
      printf '\n===== %s =====\n' "$(basename -- "${data_file}")" >> "${staged_session}/perf-report.stderr.txt"
      cat "${staged_session}/.perf-report.${part_name}.stderr.txt" >> "${staged_session}/perf-report.stderr.txt"
    fi
    ((index += 1))
  done
  rm -f "${staged_session}"/.perf-report.part*.txt "${staged_session}"/.perf-report.part*.stderr.txt
  if (( report_failed )); then
    log "WARNING ${qid}: one or more text-report shards failed; continuing with flamegraph generation"
  fi

  log "${qid}: parallel symbolized perf scripts"
  parallel_failed=0
  batch_pids=()
  part_files=()
  index=0
  for data_file in "${staged_data_files[@]}"; do
    printf -v part_name 'part%03d' "${index}"
    part_file="${staged_session}/.perf-script.${part_name}"
    part_files+=("${part_file}")
    (
      perf script "${perf_inline_args[@]}" \
        --symfs "${stage_symfs}" -i "${data_file}" \
        > "${part_file}" \
        2> "${part_file}.stderr.txt"
    ) &
    batch_pids+=("$!")
    ((index += 1))
    if (( ${#batch_pids[@]} >= parallel_jobs )); then
      wait_for_batch
    fi
  done
  wait_for_batch
  if (( parallel_failed )); then
    : > "${staged_session}/perf-script.stderr.txt"
    for part_file in "${part_files[@]}"; do
      cat "${part_file}.stderr.txt" >> "${staged_session}/perf-script.stderr.txt"
    done
    log "ERROR ${qid}: perf script failed for one or more raw shards"
    return 1
  fi

  : > "${script_file}"
  : > "${staged_session}/perf-script.stderr.txt"
  for part_file in "${part_files[@]}"; do
    cat "${part_file}" >> "${script_file}"
    cat "${part_file}.stderr.txt" >> "${staged_session}/perf-script.stderr.txt"
  done
  rm -f "${staged_session}"/.perf-script.part* "${staged_session}"/.perf-script.part*.stderr.txt

  : > "${generator_log}"
  rm -f \
    "${staged_session}/flamegraph.html" \
    "${staged_session}/flamegraph-unavailable.txt" \
    "${staged_session}/stacks.folded"

  log "${qid}: bundled Python flamegraph renderer"
  {
    echo "Logical capture has ${#staged_data_files[@]} raw shard(s)."
    echo "perf script inline-function expansion: ${inline_expansion}."
    echo "Rendering the combined symbolized stream as a self-contained, click-to-zoom flamegraph."
  } >> "${generator_log}"
  if python3 "${FLAMEGRAPH_SCRIPT}" \
      --input "${script_file}" \
      --folded "${staged_session}/stacks.folded" \
      --output "${staged_session}/flamegraph.html" \
      --title "Presto worker ${worker_id}: ${qid}" \
      >> "${generator_log}" 2>&1; then
    renderer_status="bundled-python"
  else
    {
      echo "Flamegraph rendering failed; raw captures and the compressed symbolized stream were retained."
      echo "See flamegraph-generator.txt and perf-script.stderr.txt."
    } > "${staged_session}/flamegraph-unavailable.txt"
  fi

  log "${qid}: compressing symbolized script"
  gzip -1 -f "${script_file}" || {
    log "ERROR ${qid}: could not compress perf.script"
    return 1
  }

  {
    echo "postprocess_version=2"
    echo "postprocess_timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "postprocess_hostname=$(hostname)"
    echo "postprocess_architecture=${current_arch}"
    echo "capture_architecture=${captured_arch:-unknown}"
    echo "postprocess_node_local_staging=1"
    echo "postprocess_parallel_jobs=${parallel_jobs}"
    echo "postprocess_perf_data_shards=${#staged_data_files[@]}"
    echo "postprocess_inline_expansion=${inline_expansion}"
    echo "postprocess_text_report_status=$([[ "${report_failed}" == "0" ]] && echo complete || echo partial)"
    echo "flamegraph_renderer=${renderer_status}"
  } > "${staged_session}/postprocess-metadata.txt"

  if [[ "${renderer_status}" != "bundled-python" ]]; then
    publish_derived_files "${staged_session}" "${source_session}" 0 || return 1
    discard_staged_session "${staged_session}"
    log "ERROR ${qid}: flamegraph rendering failed; diagnostics were published"
    return 1
  fi

  publish_derived_files "${staged_session}" "${source_session}" 1 || return 1
  discard_staged_session "${staged_session}"
  log "Completed ${qid}; flamegraph=${source_session}/flamegraph.html"
}

log "Postprocessing on $(hostname) (${current_arch}); node-local staging=${stage_root}"
failed=0
(( invalid_capture_count == 0 )) || failed=1
for session_dir in "${session_dirs[@]}"; do
  process_capture "${session_dir}" || failed=1
done

if (( failed )); then
  log "One or more captures failed to post-process"
  exit 1
fi
log "All selected captures processed successfully"
