#!/bin/bash
set -e

# Download Presto CLI jar file

PRESTO_VERSION="0.289"
DOWNLOAD_URL="https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar"
TARGET_DIR="/raid/pwilson/velox-testing/presto/docker"
TARGET_FILE="${TARGET_DIR}/presto-cli.jar"

echo "Downloading Presto CLI version ${PRESTO_VERSION}..."
echo "URL: ${DOWNLOAD_URL}"
echo "Target: ${TARGET_FILE}"

# Create target directory if it doesn't exist
mkdir -p "${TARGET_DIR}"

# Download the CLI jar
if curl -L "${DOWNLOAD_URL}" -o "${TARGET_FILE}"; then
    echo "✅ Successfully downloaded Presto CLI to ${TARGET_FILE}"
    echo "File size: $(ls -lh "${TARGET_FILE}" | awk '{print $5}')"
    
    # Make it executable
    chmod +x "${TARGET_FILE}"
    echo "✅ Made ${TARGET_FILE} executable"
else
    echo "❌ Failed to download Presto CLI"
    exit 1
fi
