#!/bin/bash

set -e

# patch deps image script to omit problematic Hadoop SDK download
pushd ../../../presto
git apply ../velox-testing/presto/scripts/omit_hadoop_sdk_install_patch.diff
popd

# now build the deps image if needed
pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd
