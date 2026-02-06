#!/bin/bash
set -euo pipefail

# Install RAPIDS sccache fork

if test "${SCCACHE_VERSION:-latest}" = latest; then
    # Install the latest version
    wget --no-hsts -q -O- "https://github.com/rapidsai/sccache/releases/latest/download/sccache-$(uname -m)-unknown-linux-musl.tar.gz" \
  | tar -C /usr/bin -vzf - --wildcards --strip-components=1 -x '*/sccache'
else
    # Install pinned version
    wget --no-hsts -q -O- "https://github.com/rapidsai/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-$(uname -m)-unknown-linux-musl.tar.gz" \
  | tar -C /usr/bin -vzf - --wildcards --strip-components=1 -x '*/sccache'
fi

chmod +x /usr/bin/sccache

# Verify installation
sccache --version

# Configure sccache for high parallelism
# Increase file descriptor limit for high parallelism (if possible)
ulimit -n $(ulimit -Hn) || echo "Could not increase file descriptor limit"

# Restart sccache server to avoid stale/duplicate instances
sccache --stop-server >/dev/null 2>&1 || true
sccache --start-server

# Test sccache
sccache --show-stats

# Testing distributed compilation status (only if enabled)
if test -v SCCACHE_NO_DIST_COMPILE; then
    echo "Distributed compilation is DISABLED by default - using local compilation with remote S3 caching"
else
    if sccache --dist-status 2>/dev/null | jq -er '.SchedulerStatus? != null' >/dev/null 2>&1; then
        echo "Distributed compilation is available:"
        sccache --dist-status | jq -r '["scheduler URL: " + .SchedulerStatus[0], "server count: " + (.SchedulerStatus[1].servers | length | tostring)][]';
    else
        echo "Error: Distributed compilation not available, check connectivity"
        cat "$SCCACHE_ERROR_LOG";
        exit 1
    fi
fi
