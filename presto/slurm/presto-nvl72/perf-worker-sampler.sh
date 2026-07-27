#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Runs directly on the worker's compute host (outside Enroot) and attaches the
# host's kernel-matched perf binary to a presto_server PID published by the
# worker container. Pyxis does not create a PID namespace for these workers, so
# the PID is identical inside and outside the container. Query start/stop is
# controlled by token files written by perf_profiler_functions.sh.

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
SHARDED_RECORDER="${SCRIPT_DIR}/perf-record-sharded.sh"

worker_id=0
control_dir=""
output_dir=""
postprocess_script=""
postprocess=0
event="cpu-clock:u"
frequency=19
call_graph="dwarf,8192"
user_regs="auto"
nofile_limit=65536
threads_per_shard=128
pid_wait_timeout=180
record_stop_timeout=120
control_ack_timeout="${PERF_CONTROL_ACK_TIMEOUT:-120}"
startup_attempts="${PERF_STARTUP_ATTEMPTS:-3}"
startup_ready_timeout="${PERF_STARTUP_READY_TIMEOUT:-30}"

usage() {
  cat <<EOF
Usage: $0 --control-dir DIR --output-dir DIR --postprocess-script PATH [OPTIONS]

Options:
  --worker-id N          Global worker ID (default: 0)
  --postprocess 0|1      Generate reports after every raw capture is published
                         (default: 0)
  --event EVENT          perf event (default: cpu-clock:u)
  --frequency HZ         Sample frequency (default: 19)
  --call-graph MODE      perf call-graph mode (default: dwarf,8192)
  --user-regs REGS       Registers for perf --user-regs, or auto/perf-default
                         (default: auto)
  --nofile-limit N       Minimum host perf soft nofile limit (default: 65536)
  --threads-per-shard N  Maximum target TIDs opened by each perf process
                         (default: 128)
  --pid-wait-timeout S   Seconds to wait for the worker PID (default: 180)
  --record-stop-timeout S
                         Seconds perf record may take to stop (default: 120)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id) worker_id="${2:?missing --worker-id value}"; shift 2 ;;
    --control-dir) control_dir="${2:?missing --control-dir value}"; shift 2 ;;
    --output-dir) output_dir="${2:?missing --output-dir value}"; shift 2 ;;
    --postprocess-script) postprocess_script="${2:?missing --postprocess-script value}"; shift 2 ;;
    --postprocess) postprocess="${2:?missing --postprocess value}"; shift 2 ;;
    --event) event="${2:?missing --event value}"; shift 2 ;;
    --frequency) frequency="${2:?missing --frequency value}"; shift 2 ;;
    --call-graph) call_graph="${2:?missing --call-graph value}"; shift 2 ;;
    --user-regs) user_regs="${2:?missing --user-regs value}"; shift 2 ;;
    --nofile-limit) nofile_limit="${2:?missing --nofile-limit value}"; shift 2 ;;
    --threads-per-shard) threads_per_shard="${2:?missing --threads-per-shard value}"; shift 2 ;;
    --pid-wait-timeout) pid_wait_timeout="${2:?missing --pid-wait-timeout value}"; shift 2 ;;
    --record-stop-timeout) record_stop_timeout="${2:?missing --record-stop-timeout value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "${worker_id}" =~ ^[0-9]+$ ]] || { echo "Error: invalid worker ID: ${worker_id}" >&2; exit 2; }
[[ "${frequency}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid frequency: ${frequency}" >&2; exit 2; }
[[ "${nofile_limit}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid nofile limit: ${nofile_limit}" >&2; exit 2; }
[[ "${threads_per_shard}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid threads per shard: ${threads_per_shard}" >&2; exit 2; }
[[ "${pid_wait_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid PID wait timeout: ${pid_wait_timeout}" >&2; exit 2; }
[[ "${record_stop_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid perf record stop timeout: ${record_stop_timeout}" >&2; exit 2; }
[[ "${control_ack_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: PERF_CONTROL_ACK_TIMEOUT must be a positive integer; got ${control_ack_timeout}" >&2; exit 2; }
[[ "${startup_attempts}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: PERF_STARTUP_ATTEMPTS must be a positive integer; got ${startup_attempts}" >&2; exit 2; }
[[ "${startup_ready_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: PERF_STARTUP_READY_TIMEOUT must be a positive integer; got ${startup_ready_timeout}" >&2; exit 2; }
[[ "${postprocess}" =~ ^[01]$ ]] || { echo "Error: --postprocess must be 0 or 1; got ${postprocess}" >&2; exit 2; }
[[ -n "${control_dir}" ]] || { echo "Error: --control-dir is required" >&2; exit 2; }
[[ -n "${output_dir}" ]] || { echo "Error: --output-dir is required" >&2; exit 2; }
[[ -f "${postprocess_script}" ]] || { echo "Error: perf postprocessor not found: ${postprocess_script}" >&2; exit 2; }
[[ -f "${SHARDED_RECORDER}" ]] || { echo "Error: sharded perf recorder not found: ${SHARDED_RECORDER}" >&2; exit 2; }

mkdir -p "${control_dir}" "${output_dir}/worker_${worker_id}"

prefix="${control_dir}/.perf"
pid_file="${control_dir}/.presto_worker_pid_w${worker_id}"
ready_file="${prefix}_sidecar_ready_w${worker_id}"
sidecar_error_file="${prefix}_sidecar_error_w${worker_id}"
shutdown_file="${prefix}_sidecar_shutdown_w${worker_id}"
done_file="${prefix}_sidecar_done_w${worker_id}"
active_perf_pid=""
active_perf_control_ready=0
active_control_fd=""
active_ack_fd=""
active_control_fifo=""
active_ack_fifo=""
capture_staging_root=""
finalization_failed=0
last_perf_rc=0
last_perf_stop_forced=0
declare -a capture_qids=()
declare -a capture_staging_dirs=()
declare -a capture_session_dirs=()

rm -f "${ready_file}" "${sidecar_error_file}" "${shutdown_file}" "${done_file}"

write_sidecar_error() {
  printf 'Error: worker %s perf sidecar: %s\n' "${worker_id}" "$*" | tee -a "${sidecar_error_file}" >&2
}

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

# perf_event_open consumes a descriptor for each event/target combination.
# Presto creates most of its driver and I/O pools lazily during Q1, so an early
# preflight can need substantially fewer descriptors than a later query. Treat
# --nofile-limit as a minimum, never lower an inherited soft limit, and use all
# descriptor headroom already granted to this Slurm job. Raising RLIMIT_NOFILE
# only raises a ceiling; it does not allocate descriptors by itself.
nofile_soft_before="$(ulimit -Sn)"
nofile_hard="$(ulimit -Hn)"
if [[ "${nofile_hard}" != "unlimited" ]] && (( nofile_hard < nofile_limit )); then
  write_sidecar_error "host hard nofile limit ${nofile_hard} is below the requested minimum ${nofile_limit}"
  exit 1
fi
nofile_target="${nofile_hard}"
if [[ "${nofile_soft_before}" != "${nofile_target}" ]] && ! ulimit -Sn "${nofile_target}"; then
  write_sidecar_error "cannot raise host perf nofile limit from ${nofile_soft_before} to ${nofile_target} (hard=${nofile_hard})"
  exit 1
fi
nofile_soft_after="$(ulimit -Sn)"
if [[ "${nofile_soft_after}" != "unlimited" ]] && (( nofile_soft_after < nofile_limit )); then
  write_sidecar_error "host perf nofile limit ${nofile_soft_after} is below the requested minimum ${nofile_limit} (hard=${nofile_hard})"
  exit 1
fi

perf_process_is_running() {
  local state=""
  [[ -n "${active_perf_pid}" ]] || return 1
  kill -0 "${active_perf_pid}" 2>/dev/null || return 1
  state="$(awk '{ print $3 }' "/proc/${active_perf_pid}/stat" 2>/dev/null || true)"
  [[ "${state}" != "Z" ]]
}

close_active_perf_control() {
  if [[ "${active_ack_fd}" =~ ^[0-9]+$ ]]; then
    exec {active_ack_fd}>&-
  fi
  if [[ "${active_control_fd}" =~ ^[0-9]+$ ]]; then
    exec {active_control_fd}>&-
  fi
  active_ack_fd=""
  active_control_fd=""
  [[ -z "${active_ack_fifo}" ]] || rm -f "${active_ack_fifo}"
  [[ -z "${active_control_fifo}" ]] || rm -f "${active_control_fifo}"
  active_ack_fifo=""
  active_control_fifo=""
  active_perf_control_ready=0
}

send_perf_control() {
  local -r command="$1"
  local -r timeout_seconds="$2"
  local response=""
  local tick=0

  [[ "${active_control_fd}" =~ ^[0-9]+$ && "${active_ack_fd}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${command}" >&"${active_control_fd}" || return 1
  for ((tick = 0; tick < timeout_seconds * 5; ++tick)); do
    if IFS= read -r -t 0.2 -u "${active_ack_fd}" response; then
      [[ "${response}" == "ack" ]] && return 0
      log "ignoring unexpected perf control response '${response}' for '${command}'"
    fi
    perf_process_is_running || return 1
  done
  return 2
}

stop_active_perf() {
  last_perf_rc=0
  last_perf_stop_forced=0
  if [[ -n "${active_perf_pid}" ]]; then
    local -r stop_marker="${control_dir}/.perf_record_stop_forced_w${worker_id}"
    local watchdog_pid=""
    rm -f "${stop_marker}"
    if perf_process_is_running; then
      if [[ "${active_perf_control_ready}" != "1" ]] \
          || ! send_perf_control "stop" "${control_ack_timeout}"; then
        kill -INT "${active_perf_pid}" 2>/dev/null || true
      fi
      (
        sleep "${record_stop_timeout}"
        if kill -0 "${active_perf_pid}" 2>/dev/null; then
          touch "${stop_marker}"
          kill -TERM "${active_perf_pid}" 2>/dev/null || true
          sleep 10
          kill -KILL "${active_perf_pid}" 2>/dev/null || true
        fi
      ) &
      watchdog_pid=$!
    fi
    wait "${active_perf_pid}" 2>/dev/null || last_perf_rc=$?
    if [[ -n "${watchdog_pid}" ]]; then
      kill -TERM "${watchdog_pid}" 2>/dev/null || true
      wait "${watchdog_pid}" 2>/dev/null || true
    fi
    if [[ -f "${stop_marker}" ]]; then
      last_perf_stop_forced=1
      rm -f "${stop_marker}"
    fi
  fi
  active_perf_pid=""
  close_active_perf_control
}

cleanup() {
  stop_active_perf
  # Every successfully finalized capture is moved out of this directory. Keep
  # it (and any partial raw capture) when it is unexpectedly non-empty.
  [[ -z "${capture_staging_root}" ]] || rmdir "${capture_staging_root}" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# perf's DWARF records can be write-heavy even at a modest frequency. Retain all
# active-query captures on node-local storage, then atomically publish the
# completed files only after the suite. This keeps NFS/home throughput out of
# both the sampling path and subsequent measured queries.
capture_staging_base="${PERF_CAPTURE_TMPDIR:-${SLURM_TMPDIR:-/tmp}}"
if [[ ! -d "${capture_staging_base}" || ! -w "${capture_staging_base}" ]]; then
  write_sidecar_error "perf staging directory is not writable: ${capture_staging_base}"
  exit 1
fi
capture_staging_root="$(mktemp -d "${capture_staging_base%/}/presto-perf-${SLURM_JOB_ID:-nojob}-w${worker_id}.XXXXXX")" || {
  write_sidecar_error "could not create node-local perf staging directory below ${capture_staging_base}"
  exit 1
}

if ! command -v perf >/dev/null 2>&1; then
  write_sidecar_error "perf is not installed on host $(hostname). Install the kernel-matched perf package on compute nodes."
  exit 1
fi
perf_record_help="$(perf record -h 2>&1 || true)"
if [[ "${perf_record_help}" != *"--control"* || "${perf_record_help}" != *"--delay"* ]]; then
  write_sidecar_error "host perf does not support the --control/--delay handshake required for query-scoped capture"
  exit 1
fi

# Grace advertises Arm SVE, so perf's automatic DWARF register mask includes
# the extended VG pseudo-register. Hardware core PMUs support it, but the Linux
# cpu-clock/task-clock software PMUs do not advertise EXTENDED_REGS. The kernel
# consequently rejects an otherwise valid software-event recording with
# EOPNOTSUPP. Preserve every base Arm64 GPR needed by normal DWARF CFI while
# omitting only VG. Explicit --user-regs values always win; "perf-default"
# restores perf's own automatic mask.
resolved_user_regs="${user_regs}"
if [[ "${resolved_user_regs}" == "auto" ]]; then
  resolved_user_regs=""
  if [[ "$(uname -m)" =~ ^(aarch64|arm64)$ \
      && "${call_graph}" == dwarf* \
      && "${event}" =~ ^(cpu-clock|task-clock)(:|$) ]]; then
    resolved_user_regs="x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29,lr,sp,pc"
  fi
elif [[ "${resolved_user_regs}" == "perf-default" ]]; then
  resolved_user_regs=""
fi
declare -a perf_sampling_args=(
  -e "${event}"
  -F "${frequency}"
  -g
  --call-graph "${call_graph}"
)
if [[ -n "${resolved_user_regs}" ]]; then
  [[ "${perf_record_help}" == *"--user-regs"* ]] || {
    write_sidecar_error "host perf does not support --user-regs, required for '${event}' with '${call_graph}' on $(uname -m)"
    exit 1
  }
  perf_sampling_args+=("--user-regs=${resolved_user_regs}")
fi
perf_require_symbols="${PERF_REQUIRE_SYMBOLS:-1}"
[[ "${perf_require_symbols}" =~ ^[01]$ ]] || {
  write_sidecar_error "PERF_REQUIRE_SYMBOLS must be 0 or 1; got ${perf_require_symbols}"
  exit 1
}

worker_pid=""
for ((waited = 0; waited < pid_wait_timeout; ++waited)); do
  if [[ -s "${pid_file}" ]]; then
    read -r candidate_pid < "${pid_file}" || true
    if [[ "${candidate_pid:-}" =~ ^[0-9]+$ ]] && kill -0 "${candidate_pid}" 2>/dev/null; then
      worker_pid="${candidate_pid}"
      break
    fi
  fi
  sleep 1
done

if [[ -z "${worker_pid}" ]]; then
  write_sidecar_error "did not receive a live presto_server PID in ${pid_file} within ${pid_wait_timeout}s"
  exit 1
fi

# The PID file is written immediately after forking numactl/presto_server. Wait
# for numactl to exec the actual worker so symbol roots and ownership checks are
# against presto_server rather than its short-lived launcher image.
worker_exe=""
for _ in {1..60}; do
  worker_exe="$(readlink "/proc/${worker_pid}/exe" 2>/dev/null || true)"
  if [[ "$(basename -- "${worker_exe:-unknown}")" == "presto_server" ]]; then
    break
  fi
  sleep 1
done
if [[ "$(basename -- "${worker_exe:-unknown}")" != "presto_server" ]]; then
  write_sidecar_error "PID ${worker_pid} is not presto_server (exe=${worker_exe:-unavailable})"
  exit 1
fi

worker_uid="$(stat -c '%u' "/proc/${worker_pid}" 2>/dev/null || true)"
if [[ -z "${worker_uid}" || "${worker_uid}" != "$(id -u)" ]]; then
  write_sidecar_error "PID ${worker_pid} is owned by uid ${worker_uid:-unknown}, sidecar uid is $(id -u)"
  exit 1
fi

# Preserve the exact executable mappings behind /proc/<pid>/root once per run.
# Besides making perf.data portable, a stable ordinary-directory symfs avoids
# symbol lookup failures against Enroot's private mount namespace while the
# worker is live. Anonymous mappings and data-only files cannot contribute
# symbols to a sampled call chain and are intentionally omitted.
profile_root="${output_dir}/worker_${worker_id}"
symbol_root="${profile_root}/symfs"
symbol_manifest="${profile_root}/symbol-manifest.tsv"
mkdir -p "${symbol_root}"
cp "/proc/${worker_pid}/maps" "${profile_root}/worker.maps"
: > "${symbol_manifest}"
while IFS= read -r mapped_file; do
  [[ -n "${mapped_file}" ]] || continue
  source_file="/proc/${worker_pid}/root${mapped_file}"
  destination_file="${symbol_root}${mapped_file}"
  [[ -f "${source_file}" && -r "${source_file}" ]] || continue
  mkdir -p "$(dirname -- "${destination_file}")"
  if cp -L --reflink=auto --preserve=mode,timestamps -- "${source_file}" "${destination_file}"; then
    printf '%s\t%s\n' "$(stat -c '%s' "${destination_file}")" "${mapped_file}" >> "${symbol_manifest}"
  else
    printf 'warning: could not snapshot mapped executable %s\n' "${mapped_file}" >&2
  fi
done < <(awk '$2 ~ /x/ && $6 ~ /^\// { print $6 }' "/proc/${worker_pid}/maps" | sort -u)

worker_snapshot="${symbol_root}${worker_exe}"
if [[ ! -s "${worker_snapshot}" ]]; then
  write_sidecar_error "could not snapshot ${worker_exe} from the worker mount namespace"
  exit 1
fi
if command -v file >/dev/null 2>&1; then
  file -L "${worker_snapshot}" > "${profile_root}/worker-executable.txt" 2>&1 || true
fi
if command -v readelf >/dev/null 2>&1; then
  readelf -SW "${worker_snapshot}" > "${profile_root}/worker-sections.txt" 2>&1 || true
  if grep -qE '[[:space:]]\.(symtab|debug_info)[[:space:]]' "${profile_root}/worker-sections.txt"; then
    worker_symbol_table="available"
  else
    worker_symbol_table="unavailable"
  fi
else
  worker_symbol_table="unchecked (readelf unavailable)"
fi

preflight_log="${output_dir}/worker_${worker_id}/preflight.txt"
declare -a preflight_tids=()
for task_dir in "/proc/${worker_pid}/task/"*; do
  tid="${task_dir##*/}"
  [[ "${tid}" =~ ^[1-9][0-9]*$ ]] && preflight_tids+=("${tid}")
done
mapfile -t preflight_tids < <(printf '%s\n' "${preflight_tids[@]}" | sort -n)
worker_threads_at_preflight="${#preflight_tids[@]}"
if (( ${#preflight_tids[@]} > threads_per_shard )); then
  preflight_tids=("${preflight_tids[@]:0:threads_per_shard}")
fi
preflight_tid_csv="$(IFS=,; echo "${preflight_tids[*]}")"
{
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "hostname=$(hostname)"
  echo "worker_id=${worker_id}"
  echo "worker_pid=${worker_pid}"
  echo "worker_exe=${worker_exe}"
  echo "event=${event}"
  echo "frequency=${frequency}"
  echo "call_graph=${call_graph}"
  echo "user_regs_requested=${user_regs}"
  echo "user_regs_resolved=${resolved_user_regs:-perf-default}"
  echo "record_stop_timeout=${record_stop_timeout}"
  echo "control_ack_timeout=${control_ack_timeout}"
  echo "startup_attempts=${startup_attempts}"
  echo "startup_ready_timeout=${startup_ready_timeout}"
  echo "nofile_requested_minimum=${nofile_limit}"
  echo "nofile_target=${nofile_target}"
  echo "nofile_soft_before=${nofile_soft_before}"
  echo "nofile_soft_after=${nofile_soft_after}"
  echo "nofile_hard=${nofile_hard}"
  echo "worker_threads_at_preflight=${worker_threads_at_preflight}"
  echo "threads_per_perf_shard=${threads_per_shard}"
  echo "preflight_target_threads=${#preflight_tids[@]}"
  echo "symbol_root=${symbol_root}"
  echo "worker_symbol_table=${worker_symbol_table}"
  echo "capture_staging_root=${capture_staging_root}"
  echo "capture_staging_available_kb=$(df -Pk "${capture_staging_root}" | awk 'NR==2 {print $4}')"
  echo "postprocess_after_suite=${postprocess}"
  echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unavailable)"
  echo "kptr_restrict=$(cat /proc/sys/kernel/kptr_restrict 2>/dev/null || echo unavailable)"
  echo "kernel=$(uname -srvmo)"
  perf version
} > "${preflight_log}"

if [[ "${perf_require_symbols}" == "1" && "${worker_symbol_table}" == "unavailable" ]]; then
  write_sidecar_error "${worker_exe} has no .symtab or .debug_info section; use a symbol-bearing worker image or set PERF_REQUIRE_SYMBOLS=0 to retain raw address-only captures"
  exit 1
fi

# Exercise sampling and the requested call-graph/register attributes against
# the actual target before acknowledging readiness. `perf stat` only proves
# that an event can count; it does not detect a PMU rejecting overflow
# interrupts, DWARF registers, or stack sampling.
preflight_data="${capture_staging_root}/preflight.perf.data"
if ! perf record \
    "${perf_sampling_args[@]}" \
    --inherit \
    -t "${preflight_tid_csv}" \
    -o "${preflight_data}" \
    -- sleep 0.2 \
    >"${output_dir}/worker_${worker_id}/preflight-perf-stdout.txt" \
    2>"${output_dir}/worker_${worker_id}/preflight-perf-stderr.txt"; then
  write_sidecar_error "cannot sample '${event}' with call graph '${call_graph}' on PID ${worker_pid}; see profiles/perf/worker_${worker_id}/preflight-perf-stderr.txt"
  exit 1
fi
echo "preflight_perf_data_bytes=$(stat -c '%s' "${preflight_data}")" >> "${preflight_log}"
rm -f "${preflight_data}"

touch "${ready_file}"
log "perf sidecar ready: worker=${worker_id} pid=${worker_pid} exe=${worker_exe}"

write_capture_metadata() {
  local -r session_dir="$1"
  local -r qid="$2"
  local -r phase="$3"
  {
    echo "capture_phase=${phase}"
    echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname)"
    echo "slurm_job_id=${SLURM_JOB_ID:-unknown}"
    echo "worker_id=${worker_id}"
    echo "worker_pid=${worker_pid}"
    echo "query_profile_id=${qid}"
    echo "event=${event}"
    echo "frequency=${frequency}"
    echo "call_graph=${call_graph}"
    echo "user_regs=${resolved_user_regs:-perf-default}"
    echo "symbol_root=${symbol_root}"
    echo "worker_symbol_table=${worker_symbol_table}"
    echo "worker_exe=$(readlink "/proc/${worker_pid}/exe" 2>/dev/null || echo unavailable)"
  } >> "${session_dir}/capture-metadata.txt"
}

capture_one() {
  local -r qid="$1"
  local -r session_dir="${output_dir}/worker_${worker_id}/${qid}"
  local -r error_file="${prefix}_error_w${worker_id}_${qid}"
  local -r started_file="${prefix}_started_w${worker_id}_${qid}"
  local -r stop_file="${prefix}_stop_w${worker_id}_${qid}"
  local -r finished_file="${prefix}_finished_w${worker_id}_${qid}"
  local -r staged_capture_dir="${capture_staging_root}/${qid}"
  local worker_threads_at_capture=""
  local worker_mappings_at_capture=""
  local worker_allowed_cpus=""
  local perf_shard_count=""
  local perf_total_open_fds=""
  local perf_max_shard_open_fds=""
  local perf_startup_attempt_count=""
  local perf_startup_retry_count=""
  local perf_snapshot_tid_count=""
  local perf_attached_tid_count=""
  local perf_dropped_tid_occurrence_count=""
  local perf_dropped_unique_tid_count=""
  local expected_shards=0
  local staged_total_bytes=0
  local shard_file=""
  local lost_samples=""
  local control_rc=0
  local perf_rc=0
  local -a staged_shards=()
  local -a stderr_files=()
  local -a recorder_args=()

  mkdir -p "${session_dir}" "${staged_capture_dir}"
  rm -f \
    "${error_file}" \
    "${started_file}" \
    "${stop_file}" \
    "${finished_file}"
  : > "${session_dir}/capture-metadata.txt"
  worker_threads_at_capture="$(find "/proc/${worker_pid}/task" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)"
  worker_mappings_at_capture="$(wc -l < "/proc/${worker_pid}/maps" 2>/dev/null || echo unavailable)"
  worker_allowed_cpus="$(awk '/^Cpus_allowed_list:/ { print $2 }' "/proc/${worker_pid}/status" 2>/dev/null || true)"
  write_capture_metadata "${session_dir}" "${qid}" "start"
  {
    echo "worker_threads_at_capture_start=${worker_threads_at_capture}"
    echo "worker_mappings_at_capture_start=${worker_mappings_at_capture}"
    echo "worker_allowed_cpus=${worker_allowed_cpus:-unavailable}"
    echo "sidecar_nofile_soft_at_capture=$(ulimit -Sn)"
    echo "sidecar_open_fds_before_perf=$(find "/proc/$$/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
    echo "perf_target_mode=sharded-tids-with-inheritance"
    echo "perf_threads_per_shard=${threads_per_shard}"
  } >> "${session_dir}/capture-metadata.txt"
  cp "/proc/${worker_pid}/maps" "${session_dir}/worker.maps" 2>/dev/null || true
  tr '\0' ' ' < "/proc/${worker_pid}/cmdline" > "${session_dir}/worker.cmdline.txt" 2>/dev/null || true

  active_control_fifo="${capture_staging_root}/${qid}.control"
  active_ack_fifo="${capture_staging_root}/${qid}.ack"
  rm -f "${active_control_fifo}" "${active_ack_fifo}"
  if ! mkfifo "${active_control_fifo}" "${active_ack_fifo}"; then
    echo "Error: could not create perf control FIFOs for ${qid}" | tee "${error_file}" >&2
    touch "${finished_file}"
    return 1
  fi
  if ! exec {active_control_fd}<>"${active_control_fifo}"; then
    echo "Error: could not open perf control FIFO for ${qid}" | tee "${error_file}" >&2
    close_active_perf_control
    touch "${finished_file}"
    return 1
  fi
  if ! exec {active_ack_fd}<>"${active_ack_fifo}"; then
    echo "Error: could not open perf acknowledgement FIFO for ${qid}" | tee "${error_file}" >&2
    close_active_perf_control
    touch "${finished_file}"
    return 1
  fi

  log "starting perf capture for ${qid}: worker=${worker_id} pid=${worker_pid} threads=${worker_threads_at_capture} nofile=$(ulimit -Sn)"
  recorder_args=(
    --target-pid "${worker_pid}"
    --output-dir "${staged_capture_dir}"
    --diagnostic-dir "${session_dir}"
    --control-fifo "${active_control_fifo}"
    --ack-fifo "${active_ack_fifo}"
    --event "${event}"
    --frequency "${frequency}"
    --call-graph "${call_graph}"
    --threads-per-shard "${threads_per_shard}"
    --control-ack-timeout "${control_ack_timeout}"
    --record-stop-timeout "${record_stop_timeout}"
    --startup-attempts "${startup_attempts}"
    --startup-ready-timeout "${startup_ready_timeout}"
  )
  if [[ -n "${resolved_user_regs}" ]]; then
    recorder_args+=(--user-regs "${resolved_user_regs}")
  fi
  bash "${SHARDED_RECORDER}" "${recorder_args[@]}" \
    >"${session_dir}/perf-record.stdout.txt" \
    2>"${session_dir}/perf-record.stderr.txt" &
  active_perf_pid=$!

  # Every perf shard starts with events disabled. The helper acknowledges only
  # after all shards have opened their disjoint TID sets and acknowledged
  # enable. Therefore .perf_started remains an actual whole-worker attach
  # barrier rather than a timing guess.
  send_perf_control "enable" "${control_ack_timeout}" || control_rc=$?
  if (( control_rc != 0 )); then
    stop_active_perf
    perf_rc="${last_perf_rc}"
    staged_shards=("${staged_capture_dir}"/perf.data.part*)
    staged_total_bytes=0
    for shard_file in "${staged_shards[@]}"; do
      [[ -s "${shard_file}" ]] || continue
      ((staged_total_bytes += $(stat -c '%s' "${shard_file}")))
    done
    write_capture_metadata "${session_dir}" "${qid}" "attach_failed"
    {
      echo "capture_valid=0"
      echo "capture_failure_stage=attach"
      echo "capture_failure_control_status=${control_rc}"
      echo "capture_failure_perf_status=${perf_rc}"
      echo "discarded_partial_perf_data_shards=${#staged_shards[@]}"
      echo "discarded_partial_perf_data_bytes=${staged_total_bytes}"
    } >> "${session_dir}/capture-metadata.txt"
    # The query has not started, so these files contain only an incomplete
    # recorder setup—not query samples. Retain the detailed stderr, startup
    # attempts, target TIDs, and shard manifest, but do not publish hundreds of
    # megabytes of unusable partial perf.data to the shared result directory.
    if (( ${#staged_shards[@]} > 0 )); then
      rm -f -- "${staged_shards[@]}"
    fi
    rmdir "${staged_capture_dir}" 2>/dev/null || true
    printf 'Error: perf did not acknowledge attachment before %s (control status %s, perf status %s); see %s\n' \
      "${qid}" "${control_rc}" "${perf_rc}" "${session_dir}/perf-record.stderr.txt" | tee "${error_file}" >&2
    touch "${finished_file}"
    return 1
  fi
  active_perf_control_ready=1
  if [[ -s "${session_dir}/perf-shards.tsv" ]]; then
    perf_shard_count="$(awk 'NR > 1 { count++ } END { print count + 0 }' "${session_dir}/perf-shards.tsv")"
    perf_attached_tid_count="$(awk 'NR > 1 { total += $4 } END { print total + 0 }' "${session_dir}/perf-shards.tsv")"
    perf_total_open_fds="$(awk 'NR > 1 { total += $8 } END { print total + 0 }' "${session_dir}/perf-shards.tsv")"
    perf_max_shard_open_fds="$(awk 'NR > 1 && $8 > maximum { maximum = $8 } END { print maximum + 0 }' "${session_dir}/perf-shards.tsv")"
  else
    perf_shard_count="unavailable"
    perf_attached_tid_count="unavailable"
    perf_total_open_fds="unavailable"
    perf_max_shard_open_fds="unavailable"
  fi
  if [[ -s "${session_dir}/perf-target-tids.txt" ]]; then
    perf_snapshot_tid_count="$(wc -l < "${session_dir}/perf-target-tids.txt")"
  else
    perf_snapshot_tid_count="unavailable"
  fi
  if [[ -s "${session_dir}/perf-dropped-tids.tsv" ]]; then
    perf_dropped_tid_occurrence_count="$(awk 'NR > 1 { count++ } END { print count + 0 }' "${session_dir}/perf-dropped-tids.tsv")"
    perf_dropped_unique_tid_count="$(awk -F '\t' 'NR > 1 && !seen[$3]++ { count++ } END { print count + 0 }' "${session_dir}/perf-dropped-tids.tsv")"
  else
    perf_dropped_tid_occurrence_count="unavailable"
    perf_dropped_unique_tid_count="unavailable"
  fi
  if [[ -s "${session_dir}/perf-startup-attempts.tsv" ]]; then
    perf_startup_attempt_count="$(awk 'NR > 1 { count++ } END { print count + 0 }' "${session_dir}/perf-startup-attempts.tsv")"
    perf_startup_retry_count="$(awk -F '\t' 'NR > 1 && $3 == "failed" { count++ } END { print count + 0 }' "${session_dir}/perf-startup-attempts.tsv")"
  else
    perf_startup_attempt_count="unavailable"
    perf_startup_retry_count="unavailable"
  fi
  {
    echo "perf_control_handshake=enabled"
    echo "perf_shard_count=${perf_shard_count}"
    echo "perf_total_open_fds_after_enable=${perf_total_open_fds}"
    echo "perf_max_shard_open_fds_after_enable=${perf_max_shard_open_fds}"
    echo "perf_startup_attempt_count=${perf_startup_attempt_count}"
    echo "perf_startup_retry_count=${perf_startup_retry_count}"
    echo "perf_snapshot_tid_count=${perf_snapshot_tid_count}"
    echo "perf_attached_tid_count=${perf_attached_tid_count}"
    echo "perf_dropped_tid_occurrence_count=${perf_dropped_tid_occurrence_count}"
    echo "perf_dropped_unique_tid_count=${perf_dropped_unique_tid_count}"
  } >> "${session_dir}/capture-metadata.txt"
  touch "${started_file}"
  log "perf attached for ${qid}: helper_pid=${active_perf_pid} shards=${perf_shard_count} tids=${perf_attached_tid_count}/${perf_snapshot_tid_count} dropped=${perf_dropped_unique_tid_count} total_open_fds=${perf_total_open_fds} max_shard_open_fds=${perf_max_shard_open_fds}"

  while [[ ! -f "${stop_file}" && ! -f "${shutdown_file}" ]]; do
    if ! kill -0 "${worker_pid}" 2>/dev/null; then
      echo "Error: presto_server PID ${worker_pid} exited during ${qid}" | tee "${error_file}" >&2
      break
    fi
    if ! perf_process_is_running; then
      wait "${active_perf_pid}" || perf_rc=$?
      active_perf_pid=""
      close_active_perf_control
      echo "Error: perf record exited during ${qid} with status ${perf_rc}" | tee "${error_file}" >&2
      break
    fi
    sleep 0.2
  done
  rm -f "${stop_file}"

  if [[ -n "${active_perf_pid}" ]]; then
    stop_active_perf
    perf_rc="${last_perf_rc}"
    if [[ "${last_perf_stop_forced}" == "1" ]]; then
      echo "Error: perf record did not stop within ${record_stop_timeout}s for ${qid}; it was terminated" \
        | tee "${error_file}" >&2
    fi
  fi
  # perf versions that do not convert our requested SIGINT into a clean zero
  # still finalize perf.data before returning the conventional 128+SIGINT.
  [[ "${perf_rc}" == "130" ]] && perf_rc=0
  write_capture_metadata "${session_dir}" "${qid}" "stop"

  if (( perf_rc != 0 )) && [[ ! -s "${error_file}" ]]; then
    echo "Error: perf record for ${qid} exited with status ${perf_rc}" | tee "${error_file}" >&2
  fi
  staged_shards=("${staged_capture_dir}"/perf.data.part*)
  expected_shards="$(awk 'NR > 1 { count++ } END { print count + 0 }' "${session_dir}/perf-shards.tsv" 2>/dev/null || echo 0)"
  for shard_file in "${staged_shards[@]}"; do
    [[ -s "${shard_file}" ]] || continue
    ((staged_total_bytes += $(stat -c '%s' "${shard_file}")))
  done
  if (( ${#staged_shards[@]} == 0 || staged_total_bytes == 0 )); then
    echo "Error: perf produced no data for ${qid}" | tee "${error_file}" >&2
  else
    if (( expected_shards > 0 && ${#staged_shards[@]} != expected_shards )); then
      echo "Error: perf produced ${#staged_shards[@]} of ${expected_shards} expected raw shards for ${qid}" \
        | tee "${error_file}" >&2
    fi
    capture_qids+=("${qid}")
    capture_staging_dirs+=("${staged_capture_dir}")
    capture_session_dirs+=("${session_dir}")
    {
      echo "node_local_perf_data_shards=${#staged_shards[@]}"
      echo "node_local_perf_data_bytes=${staged_total_bytes}"
    } >> "${session_dir}/capture-metadata.txt"
    write_capture_metadata "${session_dir}" "${qid}" "queued"
  fi
  stderr_files=("${session_dir}/perf-record.stderr.txt" "${session_dir}"/perf-record.part*.stderr.txt)
  lost_samples="$(grep -hEo 'lost [0-9]+([.][0-9]+)?%' "${stderr_files[@]}" 2>/dev/null | tail -n 1 || true)"
  if [[ -n "${lost_samples}" ]]; then
    echo "sample_loss=${lost_samples#lost }" >> "${session_dir}/capture-metadata.txt"
  else
    echo "sample_loss=0%" >> "${session_dir}/capture-metadata.txt"
  fi
  if [[ -s "${error_file}" ]]; then
    echo "capture_valid=0" >> "${session_dir}/capture-metadata.txt"
  else
    echo "capture_valid=1" >> "${session_dir}/capture-metadata.txt"
  fi

  # This token means perf record has stopped, closed its output, and the raw
  # capture is safely retained on node-local storage. Publication and derived
  # report generation deliberately happen after every benchmark query.
  touch "${finished_file}"
  if (( staged_total_bytes > 0 )); then
    log "capture stopped for ${qid}; queued ${#staged_shards[@]} shard(s), ${staged_total_bytes} node-local bytes"
  fi
  [[ ! -s "${error_file}" ]]
}

publish_capture() {
  local -r qid="$1"
  local -r staged_capture_dir="$2"
  local -r session_dir="$3"
  local -r error_file="${prefix}_error_w${worker_id}_${qid}"
  local staged_data_file=""
  local published_name=""
  local published_data_file=""
  local publishing_data_file=""
  local data_bytes=0
  local total_bytes=0
  local -a staged_shards=()
  local -a valid_shards=()
  local -a published_names=()

  staged_shards=("${staged_capture_dir}"/perf.data.part*)
  for staged_data_file in "${staged_shards[@]}"; do
    [[ -s "${staged_data_file}" ]] && valid_shards+=("${staged_data_file}")
  done
  if (( ${#valid_shards[@]} == 0 )); then
    echo "Error: node-local perf data disappeared before publication for ${qid}: ${staged_capture_dir}" \
      | tee -a "${error_file}" >&2
    write_sidecar_error "node-local capture disappeared before publication for ${qid}"
    return 1
  fi

  for staged_data_file in "${valid_shards[@]}"; do
    ((total_bytes += $(stat -c '%s' "${staged_data_file}")))
  done
  log "publishing ${qid}: ${#valid_shards[@]} shard(s), ${total_bytes} bytes from ${staged_capture_dir}"
  write_capture_metadata "${session_dir}" "${qid}" "publish_start"

  for staged_data_file in "${valid_shards[@]}"; do
    if (( ${#valid_shards[@]} == 1 )); then
      published_name="perf.data"
    else
      published_name="$(basename -- "${staged_data_file}")"
    fi
    published_data_file="${session_dir}/${published_name}"
    publishing_data_file="${session_dir}/.${published_name}.publishing"
    data_bytes="$(stat -c '%s' "${staged_data_file}")"
    rm -f "${publishing_data_file}"

    # mv performs a copy followed by unlink across filesystems. The
    # dot-prefixed destination remains visibly incomplete until the final
    # same-filesystem rename; if copying is interrupted, the node-local source
    # remains available.
    if ! mv -- "${staged_data_file}" "${publishing_data_file}"; then
      echo "Error: could not copy node-local perf shard for ${qid} into ${session_dir}" \
        | tee -a "${error_file}" >&2
      write_sidecar_error "could not publish node-local perf data for ${qid}"
      return 1
    fi
    if ! mv -- "${publishing_data_file}" "${published_data_file}"; then
      echo "Error: could not atomically finalize ${published_name} for ${qid} in ${session_dir}" \
        | tee -a "${error_file}" >&2
      write_sidecar_error "could not finalize published perf data for ${qid}"
      return 1
    fi
    published_names+=("${published_name}")
    log "published ${qid} shard: ${published_data_file} (${data_bytes} bytes)"
  done

  {
    echo "perf_data_shards=${#published_names[@]}"
    echo "perf_data_bytes=${total_bytes}"
    echo "perf_data_files=$(IFS=,; echo "${published_names[*]}")"
  } >> "${session_dir}/capture-metadata.txt"
  write_capture_metadata "${session_dir}" "${qid}" "published"
  rmdir "${staged_capture_dir}" 2>/dev/null || true
  log "published ${qid}: ${#published_names[@]} raw shard(s), ${total_bytes} bytes"
}

shopt -s nullglob
while [[ ! -f "${shutdown_file}" ]]; do
  start_tokens=("${prefix}_start_w${worker_id}_"*)
  if (( ${#start_tokens[@]} == 0 )); then
    sleep 0.2
    continue
  fi

  start_token="${start_tokens[0]}"
  qid="${start_token#${prefix}_start_w${worker_id}_}"
  rm -f "${start_token}"
  if [[ ! "${qid}" =~ ^[A-Za-z0-9_.-]+$ ]]; then
    write_sidecar_error "unsafe query profile identifier: ${qid}"
    break
  fi
  capture_one "${qid}" || true
done

rm -f "${ready_file}"
log "benchmark capture phase complete; publishing ${#capture_qids[@]} queued capture(s)"
for ((capture_index = 0; capture_index < ${#capture_qids[@]}; ++capture_index)); do
  publish_capture \
    "${capture_qids[capture_index]}" \
    "${capture_staging_dirs[capture_index]}" \
    "${capture_session_dirs[capture_index]}" || finalization_failed=1
done

if [[ "${postprocess}" == "1" && "${finalization_failed}" == "0" && ${#capture_qids[@]} -gt 0 ]]; then
  log "starting deferred report and flamegraph generation"
  if ! bash "${postprocess_script}" --worker-root "${profile_root}" --worker-id "${worker_id}"; then
    write_sidecar_error "one or more published captures failed deferred post-processing"
    finalization_failed=1
  fi
fi

rm -f "${shutdown_file}"
touch "${done_file}"
log "perf sidecar complete for worker ${worker_id}; finalization_failed=${finalization_failed}"
exit "${finalization_failed}"
