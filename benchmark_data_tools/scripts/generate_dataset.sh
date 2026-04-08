#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DATA_TOOLS_DIR="$SCRIPT_DIR/.."
REPO_ROOT="$BENCHMARK_DATA_TOOLS_DIR/.."
TPCHGEN_CLI="$BENCHMARK_DATA_TOOLS_DIR/.local_installs/bin/tpchgen-cli"
RUN_PY_SCRIPT="$REPO_ROOT/scripts/run_py_script.sh"
GENERATE_DATA_FILES="$BENCHMARK_DATA_TOOLS_DIR/generate_data_files.py"

# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/py_env_functions.sh"

show_help() {
    cat <<EOF
Usage: $(basename "$0") [--reset-venv] [generate_data_files.py options]

Wrapper script that sets up a Python virtual environment, installs dependencies,
and then runs generate_data_files.py to generate benchmark Parquet data files.

Wrapper options:
  --reset-venv              Delete and recreate the Python virtual environment
                            before running.

The remaining options are forwarded to generate_data_files.py:

EOF
    cd "$BENCHMARK_DATA_TOOLS_DIR"
    "$RUN_PY_SCRIPT" -q -p "$GENERATE_DATA_FILES" -- --help
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

cd "$BENCHMARK_DATA_TOOLS_DIR"

if [[ "$RESET_VENV" == true ]]; then
    delete_python_virtual_env
fi

if [ ! -f "$TPCHGEN_CLI" ]; then
    echo "tpchgen-cli not found. Installing..."
    "$SCRIPT_DIR/install_tpchgen_cli.sh"
fi

"$RUN_PY_SCRIPT" -p "$GENERATE_DATA_FILES" -- "${SCRIPT_ARGS[@]}"
