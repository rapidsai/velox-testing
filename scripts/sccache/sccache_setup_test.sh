#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SETUP_SCRIPT="${1:-${SCRIPT_DIR}/sccache_setup.sh}"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local file="$1"
    local expected="$2"
    grep -Fq -- "${expected}" "${file}" \
        || fail "Expected ${file} to contain: ${expected}"
}

run_setup_case() {
    local name="$1"
    local dist_status="$2"
    local dist_status_exit_code="$3"
    local fallback_enabled="$4"
    local initially_disabled="$5"
    local expected_exit_code="$6"
    local expected_no_dist="$7"
    local expected_status_calls="$8"

    local case_dir="${TEST_ROOT}/${name}"
    local output="${case_dir}/output"
    local trace="${case_dir}/trace"
    mkdir -p "${case_dir}"

    local actual_exit_code=0
    (
        export SCCACHE_VERSION=test
        export SCCACHE_ERROR_LOG="${case_dir}/sccache.log"
        export SCCACHE_DIST_AUTH_TOKEN=test-token
        export SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE="${fallback_enabled}"
        export TEST_DIST_STATUS="${dist_status}"
        export TEST_DIST_STATUS_EXIT_CODE="${dist_status_exit_code}"
        export TEST_SCCACHE_TRACE="${trace}"

        if [[ "${initially_disabled}" == "true" ]]; then
            export SCCACHE_NO_DIST_COMPILE=1
        else
            unset SCCACHE_NO_DIST_COMPILE
        fi

        # These mocks are called indirectly by the sourced setup script.
        # shellcheck disable=SC2317
        wget() {
            return 0
        }

        # shellcheck disable=SC2317
        tar() {
            return 0
        }

        # shellcheck disable=SC2317
        chmod() {
            return 0
        }

        # shellcheck disable=SC2317
        sccache() {
            printf '%s|no_dist=%s\n' "$*" "${SCCACHE_NO_DIST_COMPILE:-}" \
                >>"${TEST_SCCACHE_TRACE}"
            case "${1:-}" in
                --version)
                    echo "sccache test"
                    ;;
                --stop-server | --zero-stats)
                    ;;
                --dist-status)
                    if (( TEST_DIST_STATUS_EXIT_CODE != 0 )); then
                        return "${TEST_DIST_STATUS_EXIT_CODE}"
                    fi
                    printf '%s\n' "${TEST_DIST_STATUS}"
                    ;;
                *)
                    fail "Unexpected sccache arguments: $*"
                    ;;
            esac
        }

        # shellcheck disable=SC1090
        source "${SETUP_SCRIPT}"
        printf 'final_no_dist=%s\n' "${SCCACHE_NO_DIST_COMPILE:-unset}"
    ) >"${output}" 2>&1 || actual_exit_code=$?

    [[ "${actual_exit_code}" == "${expected_exit_code}" ]] \
        || fail "${name}: expected exit ${expected_exit_code}, got ${actual_exit_code}"

    local actual_status_calls
    actual_status_calls="$(grep -c '^--dist-status|' "${trace}" || true)"
    [[ "${actual_status_calls}" == "${expected_status_calls}" ]] \
        || fail "${name}: expected ${expected_status_calls} status calls, got ${actual_status_calls}"

    if [[ "${expected_exit_code}" == "0" ]]; then
        assert_contains "${output}" "final_no_dist=${expected_no_dist}"
    fi
}

healthy_status='{"SchedulerStatus":["https://arm64.example",{"servers":[{"id":"worker-1"}]}]}'
empty_status='{"SchedulerStatus":["https://arm64.example",{"servers":[]}]}'

run_setup_case healthy "${healthy_status}" 0 true false 0 unset 1
assert_contains "${TEST_ROOT}/healthy/output" "server count: 1"

run_setup_case zero-workers "${empty_status}" 0 true false 0 1 1
assert_contains "${TEST_ROOT}/zero-workers/output" "scheduler has no available servers"
assert_contains "${TEST_ROOT}/zero-workers/output" "server count: 0"
assert_contains "${TEST_ROOT}/zero-workers/trace" "--zero-stats|no_dist=1"

run_setup_case status-error '{}' 1 true false 0 1 1
run_setup_case missing-status '{}' 0 true false 0 1 1
run_setup_case invalid-status 'not-json' 0 true false 0 1 1
run_setup_case fallback-disabled "${empty_status}" 0 false false 1 unset 1
run_setup_case explicitly-disabled "${empty_status}" 0 true true 0 1 0

for dockerfile in \
    "${REPOSITORY_ROOT}/presto/docker/native_build.dockerfile" \
    "${REPOSITORY_ROOT}/velox/docker/adapters_build.dockerfile"
do
    assert_contains "${dockerfile}" "source /sccache_setup.sh;"
    if grep -Fq "bash /sccache_setup.sh;" "${dockerfile}"; then
        fail "${dockerfile} runs sccache setup in a child shell"
    fi
done

echo "All sccache setup tests passed"
