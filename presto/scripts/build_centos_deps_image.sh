#!/bin/bash

set -e

PATCH_FILE_PATH=$(readlink -f copy_arrow_patch.patch)

pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency
popd
