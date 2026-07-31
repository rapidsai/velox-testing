#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run one logical perf capture as several independently limited perf record
# processes. Some Arm server perf builds expand even a software event over the
# PMU CPU map for every selected thread. A warmed Presto worker can therefore
# exceed RLIMIT_NOFILE in one perf process. Each shard targets a disjoint set of
# existing TIDs and enables inheritance, so the union covers the complete
# worker while every recorder stays comfortably below its descriptor ceiling.

set -uo pipefail

target_pid=""
output_dir=""
diagnostic_dir=""
control_fifo=""
ack_fifo=""
event="cpu-clock:u"
frequency=19
call_graph="dwarf,8192"
user_regs=""
threads_per_shard=128
control_ack_timeout=120
record_stop_timeout=120
startup_attempts=3
startup_ready_timeout=30

usage() {
  cat <<EOF
Usage: $0 --target-pid PID --output-dir DIR --diagnostic-dir DIR \\
  --control-fifo FIFO --ack-fifo FIFO [OPTIONS]

Options:
  --event EVENT               perf event (default: cpu-clock:u)
  --frequency HZ              Sampling frequency (default: 19)
  --call-graph MODE           perf call-graph mode (default: dwarf,8192)
  --user-regs REGS            Explicit perf user-register list
  --threads-per-shard N       Maximum snapshotted TIDs per recorder (default: 128)
  --control-ack-timeout S     Seconds to await a perf control reply (default: 120)
  --record-stop-timeout S     Seconds to await recorder finalization (default: 120)
  --startup-attempts N        Attempts to initialize each recorder (default: 3)
  --startup-ready-timeout S   Seconds to await each recorder's ping (default: 30)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-pid) target_pid="${2:?missing --target-pid value}"; shift 2 ;;
    --output-dir) output_dir="${2:?missing --output-dir value}"; shift 2 ;;
    --diagnostic-dir) diagnostic_dir="${2:?missing --diagnostic-dir value}"; shift 2 ;;
    --control-fifo) control_fifo="${2:?missing --control-fifo value}"; shift 2 ;;
    --ack-fifo) ack_fifo="${2:?missing --ack-fifo value}"; shift 2 ;;
    --event) event="${2:?missing --event value}"; shift 2 ;;
    --frequency) frequency="${2:?missing --frequency value}"; shift 2 ;;
    --call-graph) call_graph="${2:?missing --call-graph value}"; shift 2 ;;
    --user-regs) user_regs="${2:?missing --user-regs value}"; shift 2 ;;
    --threads-per-shard) threads_per_shard="${2:?missing --threads-per-shard value}"; shift 2 ;;
    --control-ack-timeout) control_ack_timeout="${2:?missing --control-ack-timeout value}"; shift 2 ;;
    --record-stop-timeout) record_stop_timeout="${2:?missing --record-stop-timeout value}"; shift 2 ;;
    --startup-attempts) startup_attempts="${2:?missing --startup-attempts value}"; shift 2 ;;
    --startup-ready-timeout) startup_ready_timeout="${2:?missing --startup-ready-timeout value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "${target_pid}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --target-pid: ${target_pid}" >&2; exit 2; }
[[ "${frequency}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --frequency: ${frequency}" >&2; exit 2; }
[[ "${threads_per_shard}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --threads-per-shard: ${threads_per_shard}" >&2; exit 2; }
[[ "${control_ack_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --control-ack-timeout: ${control_ack_timeout}" >&2; exit 2; }
[[ "${record_stop_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --record-stop-timeout: ${record_stop_timeout}" >&2; exit 2; }
[[ "${startup_attempts}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --startup-attempts: ${startup_attempts}" >&2; exit 2; }
[[ "${startup_ready_timeout}" =~ ^[1-9][0-9]*$ ]] || { echo "Error: invalid --startup-ready-timeout: ${startup_ready_timeout}" >&2; exit 2; }
[[ -n "${output_dir}" ]] || { echo "Error: --output-dir is required" >&2; exit 2; }
[[ -n "${diagnostic_dir}" ]] || { echo "Error: --diagnostic-dir is required" >&2; exit 2; }
[[ -p "${control_fifo}" ]] || { echo "Error: control FIFO not found: ${control_fifo}" >&2; exit 2; }
[[ -p "${ack_fifo}" ]] || { echo "Error: acknowledgement FIFO not found: ${ack_fifo}" >&2; exit 2; }
kill -0 "${target_pid}" 2>/dev/null || { echo "Error: target PID ${target_pid} is not alive" >&2; exit 1; }

mkdir -p "${output_dir}" "${diagnostic_dir}"

declare -a target_tids=()
for task_dir in "/proc/${target_pid}/task/"*; do
  tid="${task_dir##*/}"
  [[ "${tid}" =~ ^[1-9][0-9]*$ ]] && target_tids+=("${tid}")
done
if (( ${#target_tids[@]} == 0 )); then
  echo "Error: target PID ${target_pid} has no visible threads" >&2
  exit 1
fi
mapfile -t target_tids < <(printf '%s\n' "${target_tids[@]}" | sort -n)
printf '%s\n' "${target_tids[@]}" > "${diagnostic_dir}/perf-target-tids.txt"

declare -a perf_sampling_args=(
  -e "${event}"
  -F "${frequency}"
  -g
  --call-graph "${call_graph}"
)
if [[ -n "${user_regs}" ]]; then
  perf_sampling_args+=("--user-regs=${user_regs}")
fi

declare -a child_pids=()
declare -a child_control_fds=()
declare -a child_ack_fds=()
declare -a child_control_fifos=()
declare -a child_ack_fifos=()
declare -a child_data_files=()
declare -a child_thread_counts=()
declare -a child_first_tids=()
declare -a child_last_tids=()
declare -a child_exit_rcs=()
declare -a child_enabled=()
declare -a child_open_fds_at_enable=()

outer_control_fd=""
outer_ack_fd=""
cleanup_started=0
normal_shutdown=0

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

child_is_running() {
  local -r index="$1"
  local -r pid="${child_pids[index]:-}"
  local state=""
  [[ "${pid}" =~ ^[1-9][0-9]*$ ]] || return 1
  kill -0 "${pid}" 2>/dev/null || return 1
  state="$(awk '{ print $3 }' "/proc/${pid}/stat" 2>/dev/null || true)"
  [[ "${state}" != "Z" ]]
}

record_child_exit() {
  local -r index="$1"
  local rc=0
  [[ -z "${child_exit_rcs[index]:-}" ]] || return 0
  if [[ ! "${child_pids[index]:-}" =~ ^[1-9][0-9]*$ ]]; then
    child_exit_rcs[index]="not_started"
    return 0
  fi
  wait "${child_pids[index]}" 2>/dev/null || rc=$?
  child_exit_rcs[index]="${rc}"
}

close_child_control() {
  local -r index="$1"
  local fd=""
  fd="${child_ack_fds[index]:-}"
  if [[ "${fd}" =~ ^[0-9]+$ ]]; then
    exec {fd}>&-
  fi
  fd="${child_control_fds[index]:-}"
  if [[ "${fd}" =~ ^[0-9]+$ ]]; then
    exec {fd}>&-
  fi
  child_ack_fds[index]=""
  child_control_fds[index]=""
  [[ -z "${child_ack_fifos[index]:-}" ]] || rm -f "${child_ack_fifos[index]}"
  [[ -z "${child_control_fifos[index]:-}" ]] || rm -f "${child_control_fifos[index]}"
}

write_manifest() {
  local -r phase="$1"
  local -r manifest="${diagnostic_dir}/perf-shards.tsv"
  local index=""
  local pid=""
  local state=""
  local open_fds=""
  local open_fds_at_enable=""
  local data_bytes=""

  printf 'shard\tphase\tperf_pid\tthread_count\tfirst_tid\tlast_tid\tcurrent_open_fds\topen_fds_at_enable\texit_status\tdata_file\tdata_bytes\n' > "${manifest}"
  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    pid="${child_pids[index]}"
    if child_is_running "${index}"; then
      state="running"
      open_fds="$(find "/proc/${pid}/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
    else
      record_child_exit "${index}"
      state="${child_exit_rcs[index]:-unknown}"
      open_fds="0"
    fi
    if [[ "${phase}" == "enabled" ]]; then
      child_open_fds_at_enable[index]="${open_fds}"
    fi
    open_fds_at_enable="${child_open_fds_at_enable[index]:-unavailable}"
    if [[ -s "${child_data_files[index]}" ]]; then
      data_bytes="$(stat -c '%s' "${child_data_files[index]}")"
    else
      data_bytes="0"
    fi
    printf '%03d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${index}" \
      "${phase}" \
      "${pid}" \
      "${child_thread_counts[index]}" \
      "${child_first_tids[index]}" \
      "${child_last_tids[index]}" \
      "${open_fds}" \
      "${open_fds_at_enable}" \
      "${state}" \
      "$(basename -- "${child_data_files[index]}")" \
      "${data_bytes}" >> "${manifest}"
  done
}

await_child_ack() {
  local -r index="$1"
  local -r command="$2"
  local -r timeout_seconds="${3:-${control_ack_timeout}}"
  local response=""
  local tick=0
  local -r fd="${child_ack_fds[index]}"

  for ((tick = 0; tick < timeout_seconds * 5; ++tick)); do
    if IFS= read -r -t 0.2 -u "${fd}" response; then
      [[ "${response}" == "ack" ]] && return 0
      log "shard ${index}: ignoring unexpected control response '${response}' for '${command}'"
    fi
    if ! child_is_running "${index}"; then
      record_child_exit "${index}"
      return 1
    fi
  done
  return 2
}

terminate_child() {
  local -r index="$1"
  local elapsed=0

  if child_is_running "${index}"; then
    kill -INT "${child_pids[index]}" 2>/dev/null || true
  fi
  while child_is_running "${index}" && (( elapsed < 50 )); do
    sleep 0.2
    ((elapsed += 1))
  done
  if child_is_running "${index}"; then
    kill -TERM "${child_pids[index]}" 2>/dev/null || true
    sleep 1
  fi
  if child_is_running "${index}"; then
    kill -KILL "${child_pids[index]}" 2>/dev/null || true
  fi
  record_child_exit "${index}"
}

launch_child_ready() {
  local -r index="$1"
  local -r requested_tid_csv="$2"
  local -r data_file="$3"
  local -r child_control_fifo="$4"
  local -r child_ack_fifo="$5"
  local -r stdout_file="$6"
  local -r stderr_file="$7"
  local attempt=0
  local child_control_fd=""
  local child_ack_fd=""
  local failure_stdout=""
  local failure_stderr=""
  local data_bytes=0
  local dropped_count=0
  local live_tid_csv=""
  local ready_rc=0
  local requested_count=0
  local shard_id=""
  local tid=""
  local -a dropped_tids=()
  local -a live_tids=()
  local -a perf_verbosity_args=()
  local -a requested_tids=()

  printf -v shard_id '%03d' "${index}"
  IFS=',' read -r -a requested_tids <<< "${requested_tid_csv}"
  requested_count="${#requested_tids[@]}"
  for ((attempt = 1; attempt <= startup_attempts; ++attempt)); do
    # Worker pools may retire a thread while the recorders are being opened
    # serially. perf aborts the whole -t list on ESRCH, so rebuild this shard
    # from its original, disjoint TID set before every attempt. New worker
    # threads remain covered through --inherit; never move a TID between shards.
    live_tids=()
    dropped_tids=()
    for tid in "${requested_tids[@]}"; do
      if [[ -d "/proc/${target_pid}/task/${tid}" ]]; then
        live_tids+=("${tid}")
      else
        dropped_tids+=("${tid}")
        printf '%03d\t%d\t%s\texited_before_attach\n' \
          "${index}" "${attempt}" "${tid}" \
          >> "${diagnostic_dir}/perf-dropped-tids.tsv"
      fi
    done
    dropped_count="${#dropped_tids[@]}"
    child_thread_counts[index]="${#live_tids[@]}"
    child_data_files[index]="${data_file}"
    if (( ${#live_tids[@]} == 0 )); then
      child_first_tids[index]="unavailable"
      child_last_tids[index]="unavailable"
      child_pids[index]="unavailable"
      child_exit_rcs[index]="not_started"
      printf '%03d\t%d\tfailed\tunavailable\t%s\t0\t%s\t0\tno_live_tids\t0\n' \
        "${index}" "${attempt}" "${requested_count}" "${dropped_count}" \
        >> "${diagnostic_dir}/perf-startup-attempts.tsv"
      echo "Error: all ${requested_count} snapshotted TIDs in perf shard ${index} exited before attachment" >&2
      return 1
    fi
    child_first_tids[index]="${live_tids[0]}"
    child_last_tids[index]="${live_tids[${#live_tids[@]} - 1]}"
    live_tid_csv="$(IFS=,; printf '%s' "${live_tids[*]}")"

    rm -f "${data_file}" "${child_control_fifo}" "${child_ack_fifo}"
    if ! mkfifo "${child_control_fifo}" "${child_ack_fifo}"; then
      echo "Error: could not create control FIFOs for perf shard ${index}" >&2
      return 1
    fi
    child_control_fd=""
    child_ack_fd=""
    if ! exec {child_control_fd}<>"${child_control_fifo}"; then
      echo "Error: could not open control FIFO for perf shard ${index}" >&2
      return 1
    fi
    if ! exec {child_ack_fd}<>"${child_ack_fifo}"; then
      echo "Error: could not open acknowledgement FIFO for perf shard ${index}" >&2
      exec {child_control_fd}>&-
      return 1
    fi

    # Successful verbose perf processes emit millions of overlapping-map
    # messages on this worker. Keep normal attempts quiet; if prior attempts
    # could not recover, make only the final attempt verbose for diagnostics.
    perf_verbosity_args=()
    if (( attempt > 1 && attempt == startup_attempts )); then
      perf_verbosity_args=(-v)
    fi
    perf record \
      "${perf_verbosity_args[@]}" \
      --buildid-all \
      --delay=-1 \
      --control "fifo:${child_control_fifo},${child_ack_fifo}" \
      "${perf_sampling_args[@]}" \
      --inherit \
      -t "${live_tid_csv}" \
      -o "${data_file}" \
      >"${stdout_file}" \
      2>"${stderr_file}" &

    child_pids[index]=$!
    child_control_fds[index]="${child_control_fd}"
    child_ack_fds[index]="${child_ack_fd}"
    child_control_fifos[index]="${child_control_fifo}"
    child_ack_fifos[index]="${child_ack_fifo}"
    child_data_files[index]="${data_file}"
    child_exit_rcs[index]=""
    child_enabled[index]=0
    child_open_fds_at_enable[index]=""

    # perf accepts control input before it has finished opening events. A ping
    # acknowledgement is the supported readiness barrier. Starting recorders
    # serially avoids a burst of hundreds of thousands of concurrent
    # perf_event_open calls on a warmed worker.
    ready_rc=0
    if ! printf 'ping\n' >&"${child_control_fds[index]}"; then
      ready_rc=1
    else
      await_child_ack "${index}" "startup ping" "${startup_ready_timeout}"
      ready_rc=$?
    fi
    if (( ready_rc == 0 )); then
      printf '%s\n' "${live_tids[@]}" \
        > "${diagnostic_dir}/perf-attached-tids.part${shard_id}.txt"
      printf '%03d\t%d\tready\t%s\t%s\t%s\t%s\t%s\trunning\t%s\n' \
        "${index}" "${attempt}" "${child_pids[index]}" \
        "${requested_count}" "${#live_tids[@]}" "${dropped_count}" \
        "$(find "/proc/${child_pids[index]}/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)" \
        "$(stat -c '%s' "${data_file}" 2>/dev/null || echo 0)" \
        >> "${diagnostic_dir}/perf-startup-attempts.tsv"
      log "shard ${index}/${shard_count} ready on startup attempt ${attempt}"
      return 0
    fi

    terminate_child "${index}"
    data_bytes="$(stat -c '%s' "${data_file}" 2>/dev/null || echo 0)"
    printf '%03d\t%d\tfailed\t%s\t%s\t%s\t%s\t0\t%s\t%s\n' \
      "${index}" "${attempt}" "${child_pids[index]}" \
      "${requested_count}" "${#live_tids[@]}" "${dropped_count}" \
      "${child_exit_rcs[index]:-unknown}" "${data_bytes}" \
      >> "${diagnostic_dir}/perf-startup-attempts.tsv"
    close_child_control "${index}"

    if (( attempt < startup_attempts )); then
      printf -v failure_stdout '%s/perf-record.part%s.attempt%03d.failed.stdout.txt' \
        "${diagnostic_dir}" "${shard_id}" "${attempt}"
      printf -v failure_stderr '%s/perf-record.part%s.attempt%03d.failed.stderr.txt' \
        "${diagnostic_dir}" "${shard_id}" "${attempt}"
      mv -f -- "${stdout_file}" "${failure_stdout}" 2>/dev/null || true
      mv -f -- "${stderr_file}" "${failure_stderr}" 2>/dev/null || true
      rm -f "${data_file}"
      log "shard ${index} startup attempt ${attempt} failed with status ${child_exit_rcs[index]:-unknown}; retrying"
      sleep 1
    fi
  done

  echo "Error: perf shard ${index} failed all ${startup_attempts} startup attempts; see ${stderr_file}" >&2
  return 1
}

send_all_child_controls() {
  local -r command="$1"
  local index=""
  local failed=0

  # Send every command first so a slow shard cannot serialize attachment or
  # finalization of the remaining recorders.
  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    if ! child_is_running "${index}"; then
      record_child_exit "${index}"
      echo "Error: perf shard ${index} exited with status ${child_exit_rcs[index]:-unknown} before '${command}'" >&2
      failed=1
      continue
    fi
    if ! printf '%s\n' "${command}" >&"${child_control_fds[index]}"; then
      echo "Error: could not send '${command}' to perf shard ${index}" >&2
      failed=1
    fi
  done
  (( failed == 0 )) || return 1

  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    if ! await_child_ack "${index}" "${command}"; then
      echo "Error: perf shard ${index} did not acknowledge '${command}'; see ${diagnostic_dir}/perf-record.part$(printf '%03d' "${index}").stderr.txt" >&2
      failed=1
    fi
  done
  (( failed == 0 ))
}

wait_for_children() {
  local elapsed=0
  local index=""
  local running=0
  local failed=0

  while (( elapsed < record_stop_timeout * 5 )); do
    running=0
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      if child_is_running "${index}"; then
        running=1
      elif [[ -z "${child_exit_rcs[index]:-}" ]]; then
        record_child_exit "${index}"
      fi
    done
    (( running == 0 )) && break
    sleep 0.2
    ((elapsed += 1))
  done

  if (( running != 0 )); then
    echo "Error: one or more perf shards did not stop within ${record_stop_timeout}s; terminating them" >&2
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      child_is_running "${index}" && kill -TERM "${child_pids[index]}" 2>/dev/null || true
    done
    sleep 2
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      child_is_running "${index}" && kill -KILL "${child_pids[index]}" 2>/dev/null || true
    done
    failed=1
  fi

  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    record_child_exit "${index}"
    if [[ "${child_exit_rcs[index]}" != "0" && "${child_exit_rcs[index]}" != "130" ]]; then
      echo "Error: perf shard ${index} exited with status ${child_exit_rcs[index]}" >&2
      failed=1
    fi
  done
  (( failed == 0 ))
}

terminate_children() {
  local index=""
  local elapsed=0
  local running=0

  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    child_is_running "${index}" && kill -INT "${child_pids[index]}" 2>/dev/null || true
  done
  while (( elapsed < record_stop_timeout * 5 )); do
    running=0
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      child_is_running "${index}" && running=1
    done
    (( running == 0 )) && break
    sleep 0.2
    ((elapsed += 1))
  done
  if (( running != 0 )); then
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      child_is_running "${index}" && kill -TERM "${child_pids[index]}" 2>/dev/null || true
    done
    sleep 2
    for ((index = 0; index < ${#child_pids[@]}; ++index)); do
      child_is_running "${index}" && kill -KILL "${child_pids[index]}" 2>/dev/null || true
    done
  fi
  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    [[ -n "${child_exit_rcs[index]:-}" ]] || record_child_exit "${index}"
  done
}

cleanup() {
  local index=""
  local fd=""
  (( cleanup_started == 0 )) || return 0
  cleanup_started=1
  if (( normal_shutdown == 0 )); then
    terminate_children
  fi
  for ((index = 0; index < ${#child_pids[@]}; ++index)); do
    close_child_control "${index}"
  done
  fd="${outer_ack_fd}"
  if [[ "${fd}" =~ ^[0-9]+$ ]]; then
    exec {fd}>&-
  fi
  fd="${outer_control_fd}"
  if [[ "${fd}" =~ ^[0-9]+$ ]]; then
    exec {fd}>&-
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if ! exec {outer_control_fd}<>"${control_fifo}"; then
  echo "Error: could not open outer control FIFO ${control_fifo}" >&2
  exit 1
fi
if ! exec {outer_ack_fd}<>"${ack_fifo}"; then
  echo "Error: could not open outer acknowledgement FIFO ${ack_fifo}" >&2
  exit 1
fi

shard_count="$(( (${#target_tids[@]} + threads_per_shard - 1) / threads_per_shard ))"
log "snapshot ${#target_tids[@]} target threads; starting ${shard_count} perf shard(s) with at most ${threads_per_shard} TIDs each"
printf 'shard\tattempt\tstatus\tperf_pid\trequested_thread_count\tlive_thread_count\tdropped_thread_count\tcurrent_open_fds\texit_status\tdata_bytes\n' \
  > "${diagnostic_dir}/perf-startup-attempts.tsv"
printf 'shard\tattempt\ttid\treason\n' \
  > "${diagnostic_dir}/perf-dropped-tids.tsv"

for ((shard = 0; shard < shard_count; ++shard)); do
  offset="$(( shard * threads_per_shard ))"
  shard_tids=("${target_tids[@]:offset:threads_per_shard}")
  tid_csv="$(IFS=,; echo "${shard_tids[*]}")"
  shard_id="$(printf '%03d' "${shard}")"
  data_file="${output_dir}/perf.data.part${shard_id}"
  child_control_fifo="${output_dir}/.control.part${shard_id}"
  child_ack_fifo="${output_dir}/.ack.part${shard_id}"
  stdout_file="${diagnostic_dir}/perf-record.part${shard_id}.stdout.txt"
  stderr_file="${diagnostic_dir}/perf-record.part${shard_id}.stderr.txt"

  if ! launch_child_ready \
      "${shard}" \
      "${tid_csv}" \
      "${data_file}" \
      "${child_control_fifo}" \
      "${child_ack_fifo}" \
      "${stdout_file}" \
      "${stderr_file}"; then
    write_manifest "startup_failed"
    exit 255
  fi
done

write_manifest "ready"

while true; do
  outer_command=""
  if IFS= read -r -t 0.2 -u "${outer_control_fd}" outer_command; then
    case "${outer_command}" in
      enable)
        if ! send_all_child_controls "enable"; then
          write_manifest "enable_failed"
          exit 255
        fi
        for ((shard = 0; shard < shard_count; ++shard)); do
          child_enabled[shard]=1
        done
        write_manifest "enabled"
        printf 'ack\n' >&"${outer_ack_fd}"
        log "all ${shard_count} perf shards enabled"
        ;;
      stop)
        if ! send_all_child_controls "stop"; then
          write_manifest "stop_failed"
          exit 255
        fi
        printf 'ack\n' >&"${outer_ack_fd}"
        wait_rc=0
        wait_for_children || wait_rc=$?
        write_manifest "stopped"
        normal_shutdown=1
        exit "${wait_rc}"
        ;;
      ping)
        printf 'ack\n' >&"${outer_ack_fd}"
        ;;
      *)
        log "ignoring unexpected outer control command '${outer_command}'"
        ;;
    esac
  fi

  for ((shard = 0; shard < shard_count; ++shard)); do
    if ! child_is_running "${shard}"; then
      record_child_exit "${shard}"
      echo "Error: perf shard ${shard} exited unexpectedly with status ${child_exit_rcs[shard]}; see ${diagnostic_dir}/perf-record.part$(printf '%03d' "${shard}").stderr.txt" >&2
      write_manifest "unexpected_exit"
      exit 255
    fi
  done
done
