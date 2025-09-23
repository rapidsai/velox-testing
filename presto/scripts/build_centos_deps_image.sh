#!/bin/bash

set -e

# patch Velox submodule for deps container build success
pushd ../../../presto
git apply ../velox-testing/presto/scripts/patch_hadoop_and_nvjitlink_092225.diff || true
popd

pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
