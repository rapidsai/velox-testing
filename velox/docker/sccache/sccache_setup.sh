#!/bin/bash
set -euxo pipefail

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

# Install AWS credentials
cp /sccache_auth/aws_credentials ~/.aws/credentials

# Verify AWS credentials file exists and has content
if [[ ! -s ~/.aws/credentials ]]; then
    echo "ERROR: AWS credentials file is empty or invalid"
    exit 1
fi

echo "AWS credentials file preview:"
head -3 ~/.aws/credentials

# Set AWS environment variables to ensure sccache uses the credentials file
export AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials
export AWS_PROFILE=default

# Read GitHub token
GITHUB_TOKEN=$(cat /sccache_auth/github_token | tr -d '\n\r ')

# Create sccache config
SCCACHE_ARCH=$(uname -m | sed 's/x86_64/amd64/')

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

[dist.auth]
type = "token"
token = "${GITHUB_TOKEN}"
SCCACHE_EOF

# Configure sccache for high parallelism
# Increase file descriptor limit for high parallelism (if possible)
ulimit -n $(ulimit -Hn) || echo "Could not increase file descriptor limit"

# Start sccache server
sccache --start-server

# Test sccache 
sccache --show-stats

# Testing distributed compilation status
if sccache --dist-status; then
    echo "Distributed compilation is available"
else
    echo "Error: Distributed compilation not available, check connectivity"
    exit 1
fi 