#!/bin/bash

set -e

# Change to the script's directory to ensure correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PATCH_FILE_PATH=$(readlink -f copy_arrow_patch.patch)

pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
