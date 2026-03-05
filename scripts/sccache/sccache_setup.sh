#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Install RAPIDS sccache fork

if test "${SCCACHE_VERSION:-latest}" = latest; then
    wget --no-hsts -q -O- "https://github.com/rapidsai/sccache/releases/latest/download/sccache-$(uname -m)-unknown-linux-musl.tar.gz" \
  | tar -C /usr/bin -vzf - --wildcards --strip-components=1 -x '*/sccache'
else
    wget --no-hsts -q -O- "https://github.com/rapidsai/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-$(uname -m)-unknown-linux-musl.tar.gz" \
  | tar -C /usr/bin -vzf - --wildcards --strip-components=1 -x '*/sccache'
fi

chmod +x /usr/bin/sccache
sccache --version

# Increase file descriptor limit for high parallelism (as done in rapids-configure-sccache-dist)
ulimit -n $(ulimit -Hn) || echo "Could not increase file descriptor limit"

# Ensure sccache logfile directory exists
mkdir -p "$(dirname "${SCCACHE_ERROR_LOG:-/tmp/sccache.log}")"

# Stop any stale sccache server so it picks up current env vars on restart
sccache --stop-server >/dev/null 2>&1 || true

if ! test -v SCCACHE_NO_DIST_COMPILE; then
    echo "=== sccache-dist diagnostics ==="
    echo "SCCACHE_DIST_SCHEDULER_URL=${SCCACHE_DIST_SCHEDULER_URL:-<unset>}"
    echo "SCCACHE_DIST_AUTH_TYPE=${SCCACHE_DIST_AUTH_TYPE:-<unset>}"
    if [[ -n "${SCCACHE_DIST_AUTH_TOKEN:+x}" ]]; then
        echo "SCCACHE_DIST_AUTH_TOKEN is set"
        #print the length of the token
        echo "SCCACHE_DIST_AUTH_TOKEN length: ${#SCCACHE_DIST_AUTH_TOKEN}"
    else
        echo "SCCACHE_DIST_AUTH_TOKEN is EMPTY/UNSET"
    fi
    echo "SCCACHE_DIST_MAX_RETRIES=${SCCACHE_DIST_MAX_RETRIES:-<unset>}"
    echo "SCCACHE_DIST_REQUEST_TIMEOUT=${SCCACHE_DIST_REQUEST_TIMEOUT:-<unset>}"
    echo "SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=${SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE:-<unset>}"
    echo "SCCACHE_SERVER_LOG=${SCCACHE_SERVER_LOG:-<unset>}"

    echo "=== end diagnostics ==="
fi

sccache --zero-stats

if test -v SCCACHE_NO_DIST_COMPILE; then
    echo "Distributed compilation is DISABLED - using local compilation with remote S3 caching"
else
    if sccache --dist-status 2>/dev/null | jq -er '.SchedulerStatus? != null' >/dev/null 2>&1; then
        echo "Distributed compilation is available:"
        sccache --dist-status | jq -r '["scheduler URL: " + .SchedulerStatus[0], "server count: " + (.SchedulerStatus[1].servers | length | tostring)][]';
    else
        echo "Error: Distributed compilation not available, check connectivity"
        if [[ -f "${SCCACHE_ERROR_LOG:-}" ]]; then
            echo "sccache error log:"
            cat "$SCCACHE_ERROR_LOG";
        fi
        if [[ "${SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE:-false}" == "true" ]]; then
            echo "SCCACHE_DIST_FALLBACK_TO_LOCAL_COMPILE=true, continuing with local compilation"
            export SCCACHE_NO_DIST_COMPILE=1
            sccache --stop-server >/dev/null 2>&1 || true
            sccache --zero-stats
        else
            exit 1
        fi
    fi
fi
