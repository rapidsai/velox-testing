#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Helper functions for start_spark_connect.sh.

# merge_config_files <device_type> <user_config_file>
#
# Builds a merged Spark config by layering:
#   default.conf.jinja (rendered) [+ gpu_default.conf if GPU] [+ user overlay]
#
# Template variables executor_cores and executor_instances are read from the
# environment (SPARK_WORKER_CORES, NUM_EXECUTORS) when rendering the Jinja
# template.  Falls back to the template defaults (16 cores, 1 instance).
#
# Writes the merged config to a fixed path (.temp-spark-connect.conf) in
# SCRIPT_DIR. Cleaned up by stop_spark_connect.sh.
merge_config_files() {
  local -r device_type="$1"
  local user_config_file="$2"
  local -r config_dir="${REPO_ROOT}/spark_gluten/testing/config"

  local -r merged="${SCRIPT_DIR}/.temp-spark-connect.conf"
  local -r template="${config_dir}/default.conf.jinja"

  if [[ -f "${template}" ]]; then
    local render_script
    render_script=$(readlink -f "${REPO_ROOT}/template_rendering/render_template.py")
    local render_vars="--var executor_cores=${SPARK_WORKER_CORES:-16}"
    render_vars="${render_vars} --var executor_instances=${NUM_EXECUTORS:-1}"
    "${REPO_ROOT}/scripts/run_py_script.sh" -q -p "${render_script}" \
      --template-path "${template}" \
      --output-path "${merged}" \
      ${render_vars}
  fi

  if [[ "${device_type}" == "gpu" && -f "${config_dir}/gpu_default.conf" ]]; then
    cat "${config_dir}/gpu_default.conf" >> "${merged}"
  fi

  if [[ -n ${user_config_file} ]]; then
    user_config_file="$(readlink -f "${user_config_file}")"
    if [[ ! -f "${user_config_file}" ]]; then
      echo "Error: Spark config file not found: ${user_config_file}"
      exit 1
    fi
    cat "${user_config_file}" >> "${merged}"
  fi

  echo "${merged}"
}

# parse_env_file <device_type> <env_file>
#
# Parses an environment file and appends -e flags to EXTRA_DOCKER_ARGS.
# For GPU images, the default GPU env file is used automatically unless
# a custom env file is provided.
parse_env_file() {
  local -r device_type="$1"
  local env_file="$2"
  local -r config_dir="${REPO_ROOT}/spark_gluten/testing/config"

  if [[ -z ${env_file} && "${device_type}" == "gpu" && -f "${config_dir}/gpu_default.env" ]]; then
    env_file="${config_dir}/gpu_default.env"
  fi

  if [[ -n ${env_file} ]]; then
    env_file="$(readlink -f "${env_file}")"
    if [[ ! -f "${env_file}" ]]; then
      echo "Error: Environment file not found: ${env_file}"
      exit 1
    fi
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "$line" || "$line" == \#* ]] && continue
      EXTRA_DOCKER_ARGS+=(-e "$line")
    done < "${env_file}"
  fi
}
