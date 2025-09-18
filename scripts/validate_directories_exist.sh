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

set -euo pipefail

# validate_directories_exist <dir1> [<dir2> ...]
# Checks that each directory in the argument list exists. Exits with error if
# any are missing.
validate_directories_exist() {
  if [[ "$#" -eq 0 ]]; then
    echo "Usage: verify_directories_exist <dir1> [<dir2> ...]" >&2
    exit 2
  fi

  local missing=0
  for dir in "$@"; do
    if [[ ! -d "$dir" ]]; then
      echo "ERROR: Expected directory '$dir' does not exist." >&2
      missing=1
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

# If executed directly, run with provided args
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  validate_directories_exist "$@"
fi
