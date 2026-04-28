#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script sets up a new python virtual environment, installs dependencies,
runs the given python script, and then deletes the created virtual environment.

OPTIONS:
    -h, --help                      Show this help message.
    -p, --python-script-path        Path of the python script to be run.
    -q, --quiet                     Suppress setup messages (venv, pip, etc.).
    -r, --requirements-file-path    Path of the requirements.txt file for the python script.
                                    By default, the requirements.txt file is assumed to be in
                                    the same directory as the python script.

EXAMPLES:
    $0 -p ../benchmark_data_tools/generate_data_files.py --scale-factor 10 \
    --benchmark-type tpch --data-dir-path my_tpch_data --convert-decimals-to-floats
    $0 -h

EOF
}

QUIET=false
SCRIPT_ARGS=()

log() {
  if [[ "$QUIET" != true ]]; then
    echo "$@"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help)
        print_help
        exit 0
        ;;
      -p|--python-script-path)
        if [[ -n $2 ]]; then
          SCRIPT_PATH=$2
          shift 2
        else
          echo "Error: --python-script-path requires a value"
          exit 1
        fi
        ;;
      -q|--quiet)
        QUIET=true
        shift
        ;;
      -r|--requirements-file-path)
        if [[ -n $2 ]]; then
          REQUIREMENTS_FILE_PATH=$2
          shift 2
        else
          echo "Error: --requirements-file-path requires a value"
          exit 1
        fi
        ;;
      --)
        shift
        SCRIPT_ARGS+=("$@")
        break
        ;;
      *)
        SCRIPT_ARGS+=($1)
        shift
        ;;
    esac
  done
}

parse_args "$@"

if [[ -z $SCRIPT_PATH ]]; then
  echo "Error: --python-script-path must be set"
  print_help
  exit 1
fi

if [[ -z $REQUIREMENTS_FILE_PATH ]]; then
  REQUIREMENTS_FILE_PATH="$(dirname $SCRIPT_PATH)/requirements.txt"
fi

source "$(dirname $(readlink -f $0))/py_env_functions.sh"

if [[ "$QUIET" == true ]]; then
  init_python_virtual_env > /dev/null 2>&1
else
  init_python_virtual_env
fi

STAMP_FILE=".venv/.requirements_stamp"
if [[ ! -f "$STAMP_FILE" ]] || ! diff -q "$REQUIREMENTS_FILE_PATH" "$STAMP_FILE" &>/dev/null; then
  log "Running pip install for requirements file: $REQUIREMENTS_FILE_PATH"
  pip install -q -r $REQUIREMENTS_FILE_PATH
  cp "$REQUIREMENTS_FILE_PATH" "$STAMP_FILE"
else
  log "Requirements unchanged, skipping pip install"
fi

log -e "\nRunning python script with args:\n$SCRIPT_PATH ${SCRIPT_ARGS[@]}\n"
python $SCRIPT_PATH ${SCRIPT_ARGS[@]}
log "Finished running python script"
