# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Thin standalone CMake project that builds and installs only the libcudf stack
# (rapids-cmake, rmm, kvikio, cudf) without configuring the full Velox project.
#
# Used by prebuild.dockerfile to pre-bake libcudf into a shareable base image.
# Downstream builds that use this image pass -Dcudf_SOURCE=SYSTEM so cmake
# finds the already-installed libraries via find_package instead of triggering
# FetchContent.
#
# Requires:
#   - CMAKE_CUDA_ARCHITECTURES  (e.g. "70;75;80;86;89;90" for all-major)
#   - VELOX_CMAKE_DIR           pointing to the Velox CMake/ directory
#                               (default: /opt/velox_setup/CMake)

cmake_minimum_required(VERSION 3.30.4)
project(cudf-prebuild LANGUAGES CXX CUDA)
cmake_policy(SET CMP0104 NEW)

if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
  message(FATAL_ERROR "CMAKE_CUDA_ARCHITECTURES must be set. "
    "Example: -DCMAKE_CUDA_ARCHITECTURES=\"70;75;80;86;89;90\"")
endif()

if(NOT DEFINED VELOX_CMAKE_DIR)
  set(VELOX_CMAKE_DIR "/opt/velox_setup/CMake")
endif()

# ResolveDependency.cmake defines the velox_resolve_dependency_url macro used
# by cudf.cmake to handle optional URL / checksum env-var overrides.
include("${VELOX_CMAKE_DIR}/ResolveDependency.cmake")

# cudf.cmake fetches and builds the full RAPIDS stack via FetchContent:
#   rapids-cmake -> rmm -> kvikio -> cudf
include("${VELOX_CMAKE_DIR}/resolve_dependency_modules/cudf.cmake")
