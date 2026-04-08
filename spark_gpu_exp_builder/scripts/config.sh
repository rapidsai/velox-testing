#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Generate a builder.xml configuration file populated with current defaults.
# Delegates to parse-config.py write, which reads defaults from config_def.sh.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROG_NAME="${GGBUILD_CMD:-$(basename "$0")}"
OUTPUT_PATH=""

usage() {
  cat <<EOF
Usage: ${PROG_NAME} [options]

Generate a builder.xml configuration file populated with current defaults.

Options:
  --output=PATH     Write the config to PATH (default: print to stdout)
  -h, --help        Show this help
EOF
}

for arg in "$@"; do
  case $arg in
    --output=*)  OUTPUT_PATH="${arg#*=}" ;;
    -h|--help)   usage; exit 0 ;;
    *)           echo "Unknown option: $arg" >&2; usage >&2; exit 1 ;;
  esac
done

if [ -n "$OUTPUT_PATH" ]; then
  # If path is an existing directory or ends with /, append default filename.
  if [ -d "$OUTPUT_PATH" ] || [[ "$OUTPUT_PATH" == */ ]]; then
    OUTPUT_PATH="${OUTPUT_PATH%/}/builder.xml"
  fi
  mkdir -p "$(dirname "$OUTPUT_PATH")"
  python3 "${SCRIPT_DIR}/parse-config.py" write "$OUTPUT_PATH"
  echo "Config written to: $OUTPUT_PATH" >&2
else
  python3 "${SCRIPT_DIR}/parse-config.py" write -
fi
