#!/bin/bash
set -euo pipefail

# Default output directory
DEFAULT_OUTPUT_DIR="$HOME/.sccache-auth"

# Parse arguments
GITHUB_TOKEN=""
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
INTERACTIVE_MODE=true

print_help() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [OUTPUT_DIR]

Setup sccache authentication for distributed compilation caching.

Options:
  --github-token TOKEN    Use provided GitHub token (non-interactive mode)
  -h, --help             Show this help message and exit

Arguments:
  OUTPUT_DIR             Output directory for auth files (default: $DEFAULT_OUTPUT_DIR)

Examples:
  $(basename "$0")                                    # Interactive mode
  $(basename "$0") --github-token ghp_xxxx           # Non-interactive with token
  $(basename "$0") --github-token ghp_xxxx /tmp/auth # Non-interactive with custom output dir
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --github-token)
      if [[ -z "${2:-}" || "${2}" =~ ^- ]]; then
        echo "Error: --github-token requires a token value"
        exit 1
      fi
      GITHUB_TOKEN="$2"
      INTERACTIVE_MODE=false
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    -*)
      echo "Unknown option: $1"
      print_help
      exit 1
      ;;
    *)
      OUTPUT_DIR="$1"
      shift
      ;;
  esac
done

# Timeout for AWS credentials
AWS_CREDENTIALS_TIMEOUT=43200 # 12 hours

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}sccache Authentication Setup${NC}"
if [[ "$INTERACTIVE_MODE" == true ]]; then
  echo "Running in interactive mode - will prompt for GitHub authentication."
else
  echo "Running in non-interactive mode with provided GitHub token."
fi
echo "Output directory: $OUTPUT_DIR"
echo

# Create output directory
mkdir -p "$OUTPUT_DIR"

if [[ -f "$OUTPUT_DIR/github_token" || -f "$OUTPUT_DIR/aws_credentials" ]]; then
  echo -e "${YELLOW}Warning: Existing authentication files detected in $OUTPUT_DIR.${NC}"
  if [[ "$INTERACTIVE_MODE" == true ]]; then
    echo -e "${YELLOW}Continuing will overwrite your current GitHub and AWS credentials.${NC}"
    echo -e "${YELLOW}Press Enter to continue or Ctrl+C to abort.${NC}"
    read
  else
    echo -e "${YELLOW}Overwriting existing files in non-interactive mode.${NC}"
  fi
  rm -f "$OUTPUT_DIR/github_token" "$OUTPUT_DIR/aws_credentials"
fi

# Use Docker BuildKit to ensure hereto RUN commands are available
export DOCKER_BUILDKIT=1

# Build the authentication container
echo -e "${YELLOW}Building sccache authentication container...${NC}"
docker build -f ../docker/sccache_auth.dockerfile -t sccache-auth .

echo -e "${GREEN}Authentication container built successfully${NC}"
echo

if [[ "$INTERACTIVE_MODE" == true ]]; then
  # Step 1: Interactive GitHub Authentication
  echo -e "${BLUE}GitHub Authentication (Interactive)${NC}"
  echo "Please follow the instructions to authenticate with GitHub."
  echo "A device code will be displayed for you to enter in your browser."
  echo

  docker run --rm -it \
    -v "$OUTPUT_DIR:/output" \
    sccache-auth \
    bash -c '
      echo "GitHub is authenticating with required scopes: gist, repo, read:org, read:enterprise"
      echo
      
      BROWSER="false" gh auth login --git-protocol ssh --skip-ssh-key --web --scopes gist --scopes repo --scopes read:org --scopes read:enterprise
      
      echo
      echo "Verifying authentication"
      gh auth status
      
      gh auth token > /output/github_token
      echo "GitHub token saved to /output/github_token"
    '
else
  # Step 1: Non-interactive GitHub Authentication
  echo -e "${BLUE}GitHub Authentication (Non-interactive)${NC}"
  echo "Using provided GitHub token..."
  echo

  # Save the token directly
  echo "$GITHUB_TOKEN" > "$OUTPUT_DIR/github_token"
  echo "GitHub token saved to $OUTPUT_DIR/github_token"
fi

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

# Debug: Check what token we have
echo "=== Token Debug (outside Docker) ==="
if [[ -f "$OUTPUT_DIR/github_token" ]]; then
  TOKEN_LENGTH=$(wc -c < "$OUTPUT_DIR/github_token")
  TOKEN_PREFIX=$(head -c 10 "$OUTPUT_DIR/github_token")
  echo "Token file exists, length: $TOKEN_LENGTH"
  echo "Token prefix: ${TOKEN_PREFIX}..."
else
  echo "ERROR: Token file not found at $OUTPUT_DIR/github_token"
fi
echo "=================================="

docker run --rm \
  -v "$OUTPUT_DIR:/output" \
  sccache-auth \
  bash <<EOF
    # Create debug log file
    DEBUG_LOG="/output/debug.log"
    echo "=== Docker Container Debug Started ===" > \$DEBUG_LOG
    
    if [[ ! -f /output/github_token ]]; then
      echo "Error: GitHub token not found" | tee -a \$DEBUG_LOG
      exit 1
    fi
    
    echo "=== Docker Container Debug ===" | tee -a \$DEBUG_LOG
    echo "Token file exists: \$(ls -la /output/github_token)" | tee -a \$DEBUG_LOG
    echo "Token length: \$(wc -c < /output/github_token)" | tee -a \$DEBUG_LOG
    echo "Token prefix: \$(head -c 10 /output/github_token)..." | tee -a \$DEBUG_LOG
    
    # Authenticate with the saved token
    echo "Authenticating with GitHub using token..." | tee -a \$DEBUG_LOG
    cat /output/github_token | gh auth login --with-token 2>&1 | tee -a \$DEBUG_LOG
    
    # Show auth status
    echo "=== GitHub Auth Status ===" | tee -a \$DEBUG_LOG
    gh auth status 2>&1 | tee -a \$DEBUG_LOG || echo "Auth status failed" | tee -a \$DEBUG_LOG
    
    # Generate AWS credentials
    mkdir -p /root/.aws
    
    echo "Attempting to generate AWS credentials..." | tee -a \$DEBUG_LOG
    echo "Command: gh nv-gha-aws org rapidsai --profile default --output creds-file --duration $AWS_CREDENTIALS_TIMEOUT --aud sts.amazonaws.com --idp-url https://token.gha-runners.nvidia.com --role-arn arn:aws:iam::279114543810:role/nv-gha-token-sccache-devs" | tee -a \$DEBUG_LOG
    
    # Run the command and capture everything
    echo "=== Running gh nv-gha-aws command ===" | tee -a \$DEBUG_LOG
    gh nv-gha-aws org rapidsai \
      --profile default \
      --output creds-file \
      --duration $AWS_CREDENTIALS_TIMEOUT \
      --aud sts.amazonaws.com \
      --idp-url https://token.gha-runners.nvidia.com \
      --role-arn arn:aws:iam::279114543810:role/nv-gha-token-sccache-devs \
      > /root/.aws/credentials 2>&1
    
    AWS_EXIT_CODE=\$?
    echo "gh nv-gha-aws exit code: \$AWS_EXIT_CODE" | tee -a \$DEBUG_LOG
    
    echo "=== AWS credentials file content ===" | tee -a \$DEBUG_LOG
    if [[ -f /root/.aws/credentials ]]; then
      echo "File exists, size: \$(wc -c < /root/.aws/credentials)" | tee -a \$DEBUG_LOG
      echo "Content:" | tee -a \$DEBUG_LOG
      cat /root/.aws/credentials | tee -a \$DEBUG_LOG
    else
      echo "File does not exist" | tee -a \$DEBUG_LOG
    fi
    echo "=== End credentials content ===" | tee -a \$DEBUG_LOG
    
    # Check if valid credentials
    if [[ -f /root/.aws/credentials ]] && [[ -s /root/.aws/credentials ]] && grep -q "aws_access_key_id" /root/.aws/credentials; then
      cp /root/.aws/credentials /output/aws_credentials
      echo "SUCCESS: AWS credentials generated and copied" | tee -a \$DEBUG_LOG
    else
      echo "FAILED: AWS credentials not valid" | tee -a \$DEBUG_LOG
      exit 1
    fi
    
    echo "=== Docker Container Debug Finished ===" >> \$DEBUG_LOG
EOF

# After Docker finishes, show the debug log
echo "=== Docker Debug Log ==="
if [[ -f "$OUTPUT_DIR/debug.log" ]]; then
  cat "$OUTPUT_DIR/debug.log"
else
  echo "No debug log found"
fi
echo "========================="

# Verify the AWS credentials file has actual content
if [[ ! -s "$OUTPUT_DIR/aws_credentials" ]]; then
  echo -e "${RED}AWS credentials file is empty. Generation has failed.${NC}"
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
