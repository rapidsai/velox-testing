#!/bin/bash

set -e

PATCH_FILE_PATH=$(readlink -f copy_arrow_patch.patch)

pushd ../../../presto/presto-native-execution

# Conditionally pull submodules
if [ -z "$NO_SUBMODULES" ]; then
  echo "Pulling submodules..."
  make submodules
else
  echo "Skipping submodule pull as requested"
fi

docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
