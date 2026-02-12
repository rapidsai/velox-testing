#!/bin/bash

# Compute the directory where this script resides
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VARIANT_TYPE=cpu SCRIPT_NAME=$0 "${SCRIPT_DIR}/build_presto_helper.sh" "$@"
