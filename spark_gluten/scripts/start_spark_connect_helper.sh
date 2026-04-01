#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Helper functions for start_spark_connect.sh.

# merge_config_files <device_type> <user_config_file>
#
# Builds a merged Spark config by layering:
#   default.conf [+ gpu_default.conf if GPU] [+ user overlay]
#
# Prints the path to the merged temp file. The caller is responsible for
# cleaning it up (e.g. via trap).
merge_config_files() {
  local -r device_type="$1"
  local user_config_file="$2"
  local -r config_dir="${REPO_ROOT}/spark_gluten/testing/config"

  local merged
  merged="$(mktemp)"

  if [[ -f "${config_dir}/default.conf" ]]; then
    cp "${config_dir}/default.conf" "${merged}"
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
