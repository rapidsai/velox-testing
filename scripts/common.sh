#!/usr/bin/env bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Common utility functions for Docker and S3 operations

validate_docker_image() {
  local IMAGE_NAME=$1
  echo "Validating Docker image ${IMAGE_NAME}..."
  if [[ -z $(docker images -q ${IMAGE_NAME}) ]]; then
    echo "ERROR: Docker image ${IMAGE_NAME} does not exist"
    exit 1
  fi
  echo "✓ Docker image exists"
}

