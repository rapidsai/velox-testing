#!/bin/bash

set -e

source ../../scripts/fetch_docker_image_from_s3.sh

IMAGE_NAME="presto/prestissimo-dependency:centos9"

if [[ -v REBUILD_DEPS ]]; then

	#
	# remove any existing image?
	#

	if [[ ! -z $(docker images -q ${IMAGE_NAME}) ]]; then
		echo "Removing existing Presto dependencies/run-time image..."
	  	docker rmi -f ${IMAGE_NAME}
  	fi

	#
	# try to build deps container image locally
	#

	echo "Building Presto dependencies/run-time image..."

	# for this simpler version, report this but continue
	echo "WARNING: Build patches will not be applied, local build will likely fail"

	pushd ../../../presto/presto-native-execution
	docker compose --progress plain build centos-native-dependency
	popd

	echo "Presto dependencies/run-time container image built!"

else

	#
	# check for existing container image
	#

	validate_docker_image ${IMAGE_NAME}

	echo "Presto dependencies/run-time container image not found"

	#
	# try to pull container image from our S3 bucket
	#

	ARCH=$(uname -m)
	BUCKET_SUBDIR="presto-docker-images"
	IMAGE_FILE="presto_deps_container_image_centos9_${ARCH}.tar.gz"
	
	fetch_docker_image_from_s3 ${IMAGE_NAME} ${BUCKET_SUBDIR} ${IMAGE_FILE}
	
	echo "Failed to fetch pre-built Presto dependencies/run-time container image"

fi
