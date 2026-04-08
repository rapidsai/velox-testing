#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DATA_TOOLS_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$BENCHMARK_DATA_TOOLS_DIR/.."
TPCHGEN_CLI="$BENCHMARK_DATA_TOOLS_DIR/.local_installs/bin/tpchgen-cli"
REQUIREMENTS_FILE="$BENCHMARK_DATA_TOOLS_DIR/requirements.txt"

# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/py_env_functions.sh"

install_pip_requirements() {
    local quiet="$1"
    local stamp_file=".venv/.requirements_stamp"
    if [[ ! -f "$stamp_file" ]] || ! diff -q "$REQUIREMENTS_FILE" "$stamp_file" &>/dev/null; then
        if [[ "$quiet" != true ]]; then
            echo "Running pip install for requirements file: $REQUIREMENTS_FILE"
        fi
        pip install -q -r "$REQUIREMENTS_FILE" > /dev/null 2>&1
        cp "$REQUIREMENTS_FILE" "$stamp_file"
    elif [[ "$quiet" != true ]]; then
        echo "Requirements unchanged, skipping pip install"
    fi
}

show_help() {
    pushd "$BENCHMARK_DATA_TOOLS_DIR" > /dev/null
    init_python_virtual_env > /dev/null 2>&1
    install_pip_requirements true
    cat <<EOF
Usage: $(basename "$0") [--reset-venv] [generate_data_files.py options]

Wrapper script that sets up a Python virtual environment, installs dependencies,
and then runs generate_data_files.py to generate benchmark Parquet data files.

Wrapper options:
  --reset-venv              Delete and recreate the Python virtual environment
                            before running.

The remaining options are forwarded to generate_data_files.py:

EOF
    python "$BENCHMARK_DATA_TOOLS_DIR/generate_data_files.py" --help
    popd > /dev/null
}

RESET_VENV=false
SCRIPT_ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--reset-venv" ]]; then
        RESET_VENV=true
    elif [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
        show_help
        exit 0
    else
        SCRIPT_ARGS+=("$arg")
    fi
done

pushd "$BENCHMARK_DATA_TOOLS_DIR"

if [[ "$RESET_VENV" == true ]]; then
    delete_python_virtual_env
fi

init_python_virtual_env
install_pip_requirements

if [ ! -f "$TPCHGEN_CLI" ]; then
    echo "tpchgen-cli not found. Installing..."
    "$SCRIPT_DIR/install_tpchgen_cli.sh"
fi

echo -e "\nRunning generate_data_files.py with args: ${SCRIPT_ARGS[*]}\n"
python "$BENCHMARK_DATA_TOOLS_DIR/generate_data_files.py" "${SCRIPT_ARGS[@]}"

popd
