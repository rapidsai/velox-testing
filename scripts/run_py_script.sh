#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

print_help() {
  cat << EOF

Usage: $0 [OPTIONS]

This script sets up a new python virtual environment, installs dependencies,
runs the given python script, and then deletes the created virtual environment.

OPTIONS:
    -h, --help                      Show this help message.
    -p, --python-script-path        Path of the python script to be run.
    -r, --requirements-file-path    Path of the requirements.txt file for the python script. 
                                    By default, the requirements.txt file is assumed to be in 
                                    the same directory as the python script.

EXAMPLES:
    $0 -p ../benchmark_data_tools/generate_data_files.py --scale-factor 10 \
    --benchmark-type tpch --data-dir-path my_tpch_data --convert-decimals-to-floats
    $0 -h

EOF
}

source ./helper_functions.sh

declare -A OPTION_MAP=( ["-p"]="--python-script-path" ["-r"]="--requirements-file-path" )
make_options "OPTION_MAP"

SCRIPT_ARGS=()

custom_parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -h|--help) print_help; exit 0;;
      $OPTIONS) parse_option $1 $2; shift 2;;
      *) SCRIPT_ARGS+=($1); shift 1;;
    esac
  done
}

custom_parse_args "$@"

if [[ -z $PYTHON_SCRIPT_PATH ]]; then
  echo "Error: --python-script-path must be set"
  print_help
  exit 1
fi

if [[ -z $REQUIREMENTS_FILE_PATH ]]; then
  REQUIREMENTS_FILE_PATH="$(dirname $PYTHON_SCRIPT_PATH)/requirements.txt"
fi

source "$(dirname $(readlink -f $0))/py_env_functions.sh"

trap delete_python_virtual_env EXIT

init_python_virtual_env


echo "Running pip install for requirements file: $REQUIREMENTS_FILE_PATH"
pip install -q -r $REQUIREMENTS_FILE_PATH

echo -e "\nRunning python script with args:\n$PYTHON_SCRIPT_PATH ${SCRIPT_ARGS[@]}\n"
python $PYTHON_SCRIPT_PATH ${SCRIPT_ARGS[@]}
echo "Finished running python script"
