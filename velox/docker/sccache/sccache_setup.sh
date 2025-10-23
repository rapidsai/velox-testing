#!/bin/bash
set -euo pipefail

# Check for required auth files
if [[ ! -f /sccache_auth/github_token ]]; then
    echo "ERROR: GitHub token not found at /sccache_auth/github_token"
    exit 1
fi

if [[ ! -f /sccache_auth/aws_credentials ]]; then
    echo "ERROR: AWS credentials not found at /sccache_auth/aws_credentials"
    exit 1
fi

# Set up directories
mkdir -p ~/.config/sccache ~/.aws

# Install AWS credentials (safe in Docker container environment)
cp /sccache_auth/aws_credentials ~/.aws/credentials

# Read GitHub token
GITHUB_TOKEN=$(cat /sccache_auth/github_token | tr -d '\n\r ')

# Create sccache config
SCCACHE_ARCH=$(if test "$(uname -m)" = x86_64; then echo amd64; else echo arm64; fi)

# Check if we should disable distributed compilation (disabled by default)
if [[ "${SCCACHE_DISABLE_DIST:-ON}" == "ON" ]]; then
    cat > ~/.config/sccache/config << SCCACHE_EOF
[cache.disk]
size = 107374182400

[cache.disk.preprocessor_cache_mode]
use_preprocessor_cache_mode = true

[cache.s3]
bucket = "rapids-sccache-devs"
region = "us-east-2"
no_credentials = false

# No [dist] section -> disables distributed compilation
SCCACHE_EOF
else
    cat > ~/.config/sccache/config << SCCACHE_EOF
[cache.disk]
size = 107374182400

[cache.disk.preprocessor_cache_mode]
use_preprocessor_cache_mode = true

[cache.s3]
bucket = "rapids-sccache-devs"
region = "us-east-2"
no_credentials = false

[dist]
scheduler_url = "https://${SCCACHE_ARCH}.linux.sccache.rapids.nvidia.com"
fallback_to_local_compile = true
max_retries = 4

[dist.net]
request_timeout = 7140

[dist.auth]
type = "token"
token = "${GITHUB_TOKEN}"
SCCACHE_EOF
fi

# Configure sccache for high parallelism
# Increase file descriptor limit for high parallelism (if possible)
ulimit -n $(ulimit -Hn) || echo "Could not increase file descriptor limit"

# Start sccache server
sccache --start-server

# Test sccache 
sccache --show-stats

# Testing distributed compilation status (only if enabled)
if [[ "${SCCACHE_DISABLE_DIST:-ON}" == "ON" ]]; then
    echo "Distributed compilation is DISABLED by default - using local compilation with remote S3 caching"
else
    if sccache --dist-status; then
        echo "Distributed compilation is available"
    else
        echo "Error: Distributed compilation not available, check connectivity"
        exit 1
    fi
fi 
