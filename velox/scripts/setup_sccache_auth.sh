#!/bin/bash
set -euo pipefail

# Default output directory
DEFAULT_OUTPUT_DIR="$HOME/.sccache-auth"

# Output directory
OUTPUT_DIR="${1:-$DEFAULT_OUTPUT_DIR}"

# Timeout for AWS credentials
AWS_CREDENTIALS_TIMEOUT=43200 # 12 hours

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}sccache Authentication Setup${NC}"
echo "This script will help you set up authentication for distributed compilation caching."
echo "Output directory: $OUTPUT_DIR"
echo

# Create output directory
mkdir -p "$OUTPUT_DIR"

if [[ -f "$OUTPUT_DIR/github_token" || -f "$OUTPUT_DIR/aws_credentials" ]]; then
  echo -e "${YELLOW}Warning: Existing authentication files detected in $OUTPUT_DIR.${NC}"
  echo -e "${YELLOW}Continuing will overwrite your current GitHub and AWS credentials.${NC}"
  echo -e "${YELLOW}Press Enter to continue or Ctrl+C to abort.${NC}"
  read
  rm -f "$OUTPUT_DIR/github_token" "$OUTPUT_DIR/aws_credentials"
fi

# Build the authentication container
echo -e "${YELLOW}Building sccache authentication container...${NC}"
docker build -f ../docker/sccache_auth.dockerfile -t sccache-auth .

echo -e "${GREEN}Authentication container built successfully${NC}"
echo

# Step 1: GitHub Authentication
echo -e "${BLUE}GitHub Authentication${NC}"
echo "Please follow the instructions to authenticate with GitHub."
echo "A device code will be displayed for you to enter in your browser."
echo

docker run --rm -it \
  -v "$OUTPUT_DIR:/output" \
  sccache-auth \
  bash -c '
    echo "GitHub is authenticating with required scopes: gist, repo, read:org, read:enterprise"
    echo

    echo -e "'${YELLOW}'Note: Browser opening will fail expectedly, please open the URL in your browser.'${NC}'"
    BROWSER="false" gh auth login --git-protocol ssh --skip-ssh-key --web --scopes gist --scopes repo --scopes read:org --scopes read:enterprise

    echo
    echo "Verifying authentication"
    gh auth status

    gh auth token | tr -d "\n\r " > /output/github_token
    echo "GitHub token saved to '$OUTPUT_DIR'/github_token"
  '

if [[ ! -f "$OUTPUT_DIR/github_token" ]]; then
  echo -e "${RED}GitHub token not found. Authentication has failed.${NC}"
  exit 1
fi

echo -e "${GREEN}GitHub authentication successful${NC}"
echo

# Step 2: AWS Credential Generation
echo -e "${BLUE}AWS Credential Generation${NC}"
echo "Using the gh-nv-gha-aws plugin for GitHub to generate required AWS credentials."
echo

docker run --rm -it \
  -v "$OUTPUT_DIR:/output" \
  sccache-auth \
  bash -c '
    if [[ ! -f /output/github_token ]]; then
      echo "Error: GitHub token not found"
      exit 1
    fi

    # Authenticate with the saved token
    cat /output/github_token | gh auth login --with-token

    # Verify GitHub CLI authentication
    gh auth status

    #Generate AWS credentials
    mkdir -p /root/.aws

    gh nv-gha-aws org rapidsai \
      --profile default \
      --output creds-file \
      --duration '$AWS_CREDENTIALS_TIMEOUT' \
      --aud sts.amazonaws.com \
      --idp-url https://token.gha-runners.nvidia.com \
      --role-arn arn:aws:iam::279114543810:role/nv-gha-token-sccache-devs \
      > /root/.aws/credentials

    # Copy AWS credentials to output
    cp /root/.aws/credentials /output/aws_credentials
  '

if [[ ! -f "$OUTPUT_DIR/aws_credentials" ]]; then
  echo -e "${RED}AWS credentials not found. Generation has failed.${NC}"
  exit 1
fi

echo -e "${GREEN}AWS credentials generated successfully${NC}"
echo

# Summary
echo -e "${BLUE}Authentication Setup Complete${NC}"
echo "Authentication files created in: $OUTPUT_DIR"
echo

echo -e "${YELLOW}Next steps:${NC}"
echo "1. Use these credentials with build_velox.sh:"
echo "   ./build_velox.sh --sccache --sccache-auth-dir \"$OUTPUT_DIR\""
echo
echo "2. Or set the environment variable:"
echo "   export SCCACHE_AUTH_DIR=\"$OUTPUT_DIR\""
echo "   ./build_velox.sh --sccache"
echo

echo -e "${GREEN}Setup complete!${NC}"
