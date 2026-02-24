#!/bin/bash
# Shared constants and functions for velox/presto build scripts.
# Source this file; do not execute directly.

# RAPIDS-supported CUDA architectures for CUDA 13.x (from rapids-cmake)
RAPIDS_CUDA_ARCHITECTURES="75-real;80-real;86-real;90a-real;100f-real;120a-real;120"

# Resolve CUDAARCHS: expand "RAPIDS" or empty to the concrete arch list.
resolve_cudaarchs() {
    if [[ "${CUDAARCHS:-}" == "RAPIDS" || -z "${CUDAARCHS:-}" ]]; then
        export CUDAARCHS="${RAPIDS_CUDA_ARCHITECTURES}"
    fi
}

# Look up the cmake build directory for a RAPIDS library (cudf, rmm, kvikio).
# Prints the path if found; prints nothing otherwise.
get_rapids_cmake_dir() {
    local lib_name="$1"
    local repo_path="${HOME}/${lib_name}"

    if command -v rapids-get-cmake-build-dir &>/dev/null && [[ -d "${repo_path}" ]]; then
        local build_dir
        build_dir=$(rapids-get-cmake-build-dir "${repo_path}/cpp" 2>/dev/null || true)
        if [[ -n "${build_dir}" && -d "${build_dir}" ]]; then
            echo "${build_dir}"
        fi
    fi
}

# Append -D flags for pre-built cudf, rmm, kvikio to the variable whose
# name is passed as $1 (must already exist in the caller's scope).
append_rapids_cmake_flags() {
    local __var="$1"

    local cudf_dir rmm_dir kvikio_dir
    cudf_dir=$(get_rapids_cmake_dir "cudf")
    rmm_dir=$(get_rapids_cmake_dir "rmm")
    kvikio_dir=$(get_rapids_cmake_dir "kvikio")

    if [[ -n "${cudf_dir}" ]]; then
        echo "Found cudf build at: ${cudf_dir}"
        printf -v "$__var" '%s %s' "${!__var}" "-Dcudf_ROOT=${cudf_dir}"
    fi
    if [[ -n "${rmm_dir}" ]]; then
        echo "Found rmm build at: ${rmm_dir}"
        printf -v "$__var" '%s %s' "${!__var}" "-Drmm_ROOT=${rmm_dir}"
    fi
    if [[ -n "${kvikio_dir}" ]]; then
        echo "Found kvikio build at: ${kvikio_dir}"
        printf -v "$__var" '%s %s' "${!__var}" "-Dkvikio_ROOT=${kvikio_dir}"
    fi
}
