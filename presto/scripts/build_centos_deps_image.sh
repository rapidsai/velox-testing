#!/bin/bash

set -e

#
# apply current patches for Presto deps container build success
# as of 09/25/25
#

# in Presto, disable re-build of arrow
pushd ../../../presto
git apply ../velox-testing/presto/patches/patch_arrow_092525.diff || true
popd
# in Velox sub-module, change the Hadoop version and mirror, and add libnvjitlink install
pushd ../../../presto/presto-native-execution/velox
git apply ../../../velox-testing/presto/patches/patch_hadoop_and_nvjitlink_092225.diff || true
popd

#
# build deps container
#

pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
