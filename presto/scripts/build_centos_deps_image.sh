#!/bin/bash

set -e

#
# check for existing container image
#

DEPS_IMAGE="presto/prestissimo-dependency:centos9"

if [ "${REBUILD_DEPS}" == "1" ]; then
	echo "Forcing rebuild of Presto dependencies/run-time container image"
	echo "This does not delete any existing worker container images which must be manually deleted"
	docker rmi -f ${DEPS_IMAGE} 
elif [ ! -z $(docker images -q ${DEPS_IMAGE}) ]; then
	echo "Found existing Presto dependencies/run-time container image"
	exit 0
else
	echo "Presto dependencies/run-time container image not found, attempting to re-build..."
fi

#
# apply current patches for Presto deps container build success
# as of 09/25/25
#

echo "Applying required local patches to Presto repo and contained Velox sub-module (as of 9/25/25)"

# in Presto, disable re-build of arrow
pushd ../../../presto
if [ ${REBUILD_DEPS} == "1" ]; then
	git checkout .
fi
git apply ../velox-testing/presto/patches/patch_arrow_092525.diff
popd
# in Velox sub-module, change the Hadoop version and mirror, and add libnvjitlink install
pushd ../../../presto/presto-native-execution/velox
if [ ${REBUILD_DEPS} == "1" ]; then
	git checkout .
fi
git apply ../../../velox-testing/presto/patches/patch_hadoop_and_nvjitlink_092225.diff
popd

#
# build deps container
#

echo "Building Presto dependencies/run-time image..."

pushd ../../../presto/presto-native-execution
make submodules
docker compose up centos-native-dependency # Build dependencies image if there is none present.
docker compose down centos-native-dependency
popd

echo "Presto dependencies/run-time container image built!"
