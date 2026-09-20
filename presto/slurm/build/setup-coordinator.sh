#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Setup script for Presto Coordinator (Java-based)
# Replicates the steps from presto/docker/Dockerfile
# Run this inside a CentOS Stream 9 container

set -e

# Configuration
PRESTO_VERSION=${PRESTO_VERSION:-testing}
PRESTO_HOME=${PRESTO_HOME:-/opt/presto-server}
PRESTO_BUILD_DIR=${PRESTO_BUILD_DIR:-/presto_build}
JMX_PROMETHEUS_JAVAAGENT_VERSION=${JMX_PROMETHEUS_JAVAAGENT_VERSION:-0.20.0}

# Source locations (adjust if needed)
PRESTO_SOURCE_DIR=${PRESTO_SOURCE_DIR:-/veloxtesting/presto}
PRESTO_DOCKER_DIR=${PRESTO_DOCKER_DIR:-${PRESTO_SOURCE_DIR}/docker}

echo "============================================"
echo "Setting up Presto Coordinator"
echo "============================================"
echo "PRESTO_VERSION: $PRESTO_VERSION"
echo "PRESTO_HOME: $PRESTO_HOME"
echo "PRESTO_SOURCE_DIR: $PRESTO_SOURCE_DIR"
echo "============================================"

# Step 1: Install Java and dependencies
echo ""
echo "============================================"
echo "Step 1/8: Installing Java 17 and dependencies..."
echo "============================================"
if ! command -v java &> /dev/null; then
    dnf install -y java-17-openjdk java-17-openjdk-devel less procps python3
    echo "✓ Java 17 installed"
else
    echo "✓ Java already installed: $(java -version 2>&1 | head -1)"
    # Ensure python3 is installed even if Java was already present
    if ! command -v python3 &> /dev/null; then
        dnf install -y python3 less procps
    fi
fi

# Ensure python symlink exists (Presto launcher scripts need it)
if ! command -v python &> /dev/null; then
    if command -v python3 &> /dev/null; then
        ln -sf $(which python3) /usr/bin/python
        echo "✓ Created python -> python3 symlink"
    else
        echo "ERROR: python3 not found, cannot create python symlink"
        exit 1
    fi
fi

# Set JAVA_HOME to Java 17
if [[ -z "$JAVA_HOME" ]] || [[ ! -d "$JAVA_HOME" ]] || ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -q "17"; then
    # Find Java 17 installation
    if [[ -d /usr/lib/jvm/java-17-openjdk ]]; then
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
    elif [[ -d /usr/lib/jvm/java-17 ]]; then
        export JAVA_HOME=/usr/lib/jvm/java-17
    else
        # Try to find it dynamically
        JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    fi
    echo "✓ Set JAVA_HOME=${JAVA_HOME}"
else
    echo "✓ JAVA_HOME already set: ${JAVA_HOME}"
fi

# Verify JAVA_HOME is correct
if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
    echo "ERROR: JAVA_HOME is not set correctly: ${JAVA_HOME}"
    echo "Cannot find executable: ${JAVA_HOME}/bin/java"
    exit 1
fi

echo "✓ Java version: $("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)"

# Step 2: Build Presto Java package if needed
echo ""
echo "============================================"
echo "Step 2/8: Building Presto Java package..."
echo "============================================"

if [[ ! -f "${PRESTO_BUILD_DIR}/presto-server-${PRESTO_VERSION}.tar.gz" ]]; then
    if [[ ! -f "${PRESTO_SOURCE_DIR}/pom.xml" ]]; then
        echo "ERROR: Presto source not found at ${PRESTO_SOURCE_DIR}"
        echo "Please mount or copy the Presto source code"
        exit 1
    fi

    # Build Presto (requires Maven)
    if ! command -v mvn &> /dev/null; then
        echo "Installing Maven..."
        dnf install -y maven
    fi

    echo "Building Presto server package (this may take 30-60 minutes)..."
    echo "Using JAVA_HOME: ${JAVA_HOME}"
    echo "Java version: $("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)"
    cd "${PRESTO_SOURCE_DIR}"

    # Ensure JAVA_HOME is exported for Maven wrapper
    export JAVA_HOME

    # Build with minimal tests for faster build
    # Skip presto-docs (requires Sphinx 8.2.1 which doesn't exist yet)
    ./mvnw clean install -DskipTests -Dmaven.javadoc.skip=true -T 1C -pl '!presto-docs' || {
        echo "ERROR: Presto build failed"
        echo "JAVA_HOME was: ${JAVA_HOME}"
        echo ""
        echo "If the build failed on presto-docs (Sphinx version issue), this is expected."
        echo "The script already skips presto-docs, but if you see other errors, please check the log."
        exit 1
    }

    # Copy build artifacts
    mkdir -p "${PRESTO_BUILD_DIR}"

    # Detect actual built version (may differ from PRESTO_VERSION)
    ACTUAL_SERVER_TAR=$(ls -1 presto-server/target/presto-server-*.tar.gz 2>/dev/null | head -1)
    if [[ -z "$ACTUAL_SERVER_TAR" ]]; then
        echo "ERROR: Could not find presto-server tarball"
        echo "Expected: presto-server/target/presto-server-*.tar.gz"
        echo "Contents of presto-server/target:"
        ls -la presto-server/target/ || true
        exit 1
    fi

    # Extract actual version from filename
    ACTUAL_VERSION=$(basename "$ACTUAL_SERVER_TAR" | sed 's/presto-server-//' | sed 's/.tar.gz//')
    echo "✓ Found presto-server version: ${ACTUAL_VERSION}"

    cp "$ACTUAL_SERVER_TAR" "${PRESTO_BUILD_DIR}/" || {
        echo "ERROR: Failed to copy presto-server tarball"
        exit 1
    }

    # Copy CLI if it exists
    ACTUAL_CLI_JAR=$(ls -1 presto-cli/target/presto-cli-*-executable.jar 2>/dev/null | head -1)
    if [[ -n "$ACTUAL_CLI_JAR" ]]; then
        cp "$ACTUAL_CLI_JAR" "${PRESTO_BUILD_DIR}/" || echo "Warning: Failed to copy CLI jar"
        echo "✓ Found presto-cli"
    else
        echo "Warning: presto-cli jar not found, continuing..."
        touch "${PRESTO_BUILD_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar"
    fi

    # Copy function server if it exists
    ACTUAL_FUNC_JAR=$(ls -1 presto-function-server/target/presto-function-server-*-executable.jar 2>/dev/null | head -1)
    if [[ -n "$ACTUAL_FUNC_JAR" ]]; then
        cp "$ACTUAL_FUNC_JAR" "${PRESTO_BUILD_DIR}/" || echo "Warning: Failed to copy function server jar"
        echo "✓ Found presto-function-server"
    else
        echo "Warning: presto-function-server jar not found, continuing..."
        touch "${PRESTO_BUILD_DIR}/presto-function-server-${PRESTO_VERSION}-executable.jar"
    fi

    # Update PRESTO_VERSION to match actual built version for later steps
    PRESTO_VERSION="$ACTUAL_VERSION"

    echo "✓ Presto built successfully (version: ${PRESTO_VERSION})"
else
    echo "✓ Presto package already exists at ${PRESTO_BUILD_DIR}"
    # Detect version from existing tarball
    EXISTING_TAR=$(ls -1 "${PRESTO_BUILD_DIR}"/presto-server-*.tar.gz 2>/dev/null | head -1)
    if [[ -n "$EXISTING_TAR" ]]; then
        PRESTO_VERSION=$(basename "$EXISTING_TAR" | sed 's/presto-server-//' | sed 's/.tar.gz//')
        echo "✓ Detected existing version: ${PRESTO_VERSION}"
    fi
fi

# Step 3: Install Presto server
echo ""
echo "============================================"
echo "Step 3/8: Installing Presto server..."
echo "============================================"

# Find the tarball (use detected version or search)
PRESTO_TAR="${PRESTO_BUILD_DIR}/presto-server-${PRESTO_VERSION}.tar.gz"
if [[ ! -f "$PRESTO_TAR" ]]; then
    # Try to find any presto-server tarball
    PRESTO_TAR=$(ls -1 "${PRESTO_BUILD_DIR}"/presto-server-*.tar.gz 2>/dev/null | head -1)
    if [[ -z "$PRESTO_TAR" ]]; then
        echo "ERROR: No presto-server tarball found in ${PRESTO_BUILD_DIR}"
        exit 1
    fi
    echo "Using tarball: $PRESTO_TAR"
fi

mkdir -p "$PRESTO_HOME"
tar --strip-components=1 -C "$PRESTO_HOME" -zxf "$PRESTO_TAR"
echo "✓ Presto server extracted to $PRESTO_HOME"

# Step 4: Install CLI and function server
echo ""
echo "============================================"
echo "Step 4/8: Installing Presto CLI..."
echo "============================================"

# Find CLI jar (try specific version first, then any)
CLI_JAR="${PRESTO_BUILD_DIR}/presto-cli-${PRESTO_VERSION}-executable.jar"
if [[ ! -f "$CLI_JAR" ]]; then
    CLI_JAR=$(ls -1 "${PRESTO_BUILD_DIR}"/presto-cli-*-executable.jar 2>/dev/null | head -1)
fi

if [[ -f "$CLI_JAR" ]]; then
    cp "$CLI_JAR" /opt/presto-cli
    chmod +x /opt/presto-cli
    ln -sf /opt/presto-cli /usr/local/bin/presto-cli
    echo "✓ Presto CLI installed"
else
    echo "Warning: Presto CLI not found, skipping..."
fi

# Find function server jar
FUNC_JAR="${PRESTO_BUILD_DIR}/presto-function-server-${PRESTO_VERSION}-executable.jar"
if [[ ! -f "$FUNC_JAR" ]]; then
    FUNC_JAR=$(ls -1 "${PRESTO_BUILD_DIR}"/presto-function-server-*-executable.jar 2>/dev/null | head -1)
fi

if [[ -f "$FUNC_JAR" ]]; then
    cp "$FUNC_JAR" /opt/presto-remote-function-server
    chmod +x /opt/presto-remote-function-server
    echo "✓ Presto function server installed"
fi

# Step 5: Set up directories
echo ""
echo "============================================"
echo "Step 5/8: Creating directories..."
echo "============================================"
mkdir -p $PRESTO_HOME/etc/catalog
mkdir -p /var/lib/presto/data
mkdir -p /usr/lib/presto/utils
echo "✓ Directories created"

# Step 6: Download JMX Prometheus agent
echo ""
echo "============================================"
echo "Step 6/8: Downloading JMX Prometheus agent..."
echo "============================================"
if [[ ! -f "/usr/lib/presto/utils/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar" ]]; then
    curl -o "/usr/lib/presto/utils/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar" \
        "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROMETHEUS_JAVAAGENT_VERSION}/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar"
    ln -sf "/usr/lib/presto/utils/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar" \
        /usr/lib/presto/utils/jmx_prometheus_javaagent.jar
    echo "✓ JMX agent downloaded"
else
    echo "✓ JMX agent already exists"
fi

# Step 7: Copy configuration files
echo ""
echo "============================================"
echo "Step 7/8: Setting up configuration..."
echo "============================================"

# Copy config files if they exist in the docker directory
if [[ -d "${PRESTO_DOCKER_DIR}/etc" ]]; then
    if [[ -f "${PRESTO_DOCKER_DIR}/etc/config.properties.example" ]]; then
        cp "${PRESTO_DOCKER_DIR}/etc/config.properties.example" "$PRESTO_HOME/etc/config.properties"
        echo "✓ Copied config.properties"
    fi

    if [[ -f "${PRESTO_DOCKER_DIR}/etc/jvm.config.example" ]]; then
        cp "${PRESTO_DOCKER_DIR}/etc/jvm.config.example" "$PRESTO_HOME/etc/jvm.config"
        echo "✓ Copied jvm.config"
    fi

    if [[ -f "${PRESTO_DOCKER_DIR}/etc/node.properties" ]]; then
        cp "${PRESTO_DOCKER_DIR}/etc/node.properties" "$PRESTO_HOME/etc/node.properties"
        echo "✓ Copied node.properties"
    fi

    if [[ -d "${PRESTO_DOCKER_DIR}/etc/catalog" ]]; then
        cp -r "${PRESTO_DOCKER_DIR}/etc/catalog/"* "$PRESTO_HOME/etc/catalog/" 2>/dev/null || true
        echo "✓ Copied catalog configurations"
    fi
else
    echo "Warning: Docker config directory not found at ${PRESTO_DOCKER_DIR}/etc"
    echo "You'll need to manually configure Presto in $PRESTO_HOME/etc/"
fi

# Step 8: Cleanup image to make it smaller and faster to load
echo ""
echo "============================================"
echo "Step 8/8: Cleanup image..."
echo "============================================"
yum remove -y cuda*
rm -rf /opt/rh
rm -rf /usr/local


# Copy entrypoint if it exists
if [[ -f "${PRESTO_DOCKER_DIR}/entrypoint.sh" ]]; then
    cp "${PRESTO_DOCKER_DIR}/entrypoint.sh" /opt/entrypoint.sh
    chmod +x /opt/entrypoint.sh
    echo "✓ Copied entrypoint.sh"
fi

echo ""
echo "============================================"
echo "Coordinator Setup Complete!"
echo "============================================"
echo "  Presto Home: $PRESTO_HOME"
echo "  Data Directory: /var/lib/presto/data"
echo "  CLI: /opt/presto-cli (linked to /usr/local/bin/presto-cli)"
echo ""
echo "To start the coordinator:"
echo "  $PRESTO_HOME/bin/launcher start"
echo ""
echo "Or if entrypoint.sh is available:"
echo "  /opt/entrypoint.sh"
echo "============================================"
