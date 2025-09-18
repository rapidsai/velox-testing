#!/bin/bash

set -e

PATCH_FILE_PATH=$(readlink -f copy_arrow_patch.patch)

pushd ../../../presto/presto-native-execution
make submodules
docker compose --progress=plain up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
