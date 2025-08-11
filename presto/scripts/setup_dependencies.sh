#!/bin/bash

# Setup script to install dependencies needed for TPC-H benchmarking
set -e

echo "Installing dependencies for TPC-H benchmarking..."

# Check if we're running in a container or on the host
if [[ -f /.dockerenv ]] || [[ $(cat /proc/1/cgroup 2>/dev/null | grep -c docker) -gt 0 ]]; then
    echo "Running in container environment"
    package_manager="apt-get"
    update_cmd="apt-get update"
    install_cmd="apt-get install -y"
    
    # Check if dnf is available (CentOS/RHEL/Fedora)
    if command -v dnf >/dev/null 2>&1; then
        package_manager="dnf"
        update_cmd="dnf update -y"
        install_cmd="dnf install -y"
    # Check if yum is available
    elif command -v yum >/dev/null 2>&1; then
        package_manager="yum"
        update_cmd="yum update -y"
        install_cmd="yum install -y"
    fi
else
    echo "Running on host system"
    # Detect package manager
    if command -v apt-get >/dev/null 2>&1; then
        package_manager="apt-get"
        update_cmd="apt-get update"
        install_cmd="apt-get install -y"
    elif command -v dnf >/dev/null 2>&1; then
        package_manager="dnf"
        update_cmd="dnf update -y"
        install_cmd="dnf install -y"
    elif command -v yum >/dev/null 2>&1; then
        package_manager="yum"
        update_cmd="yum update -y"
        install_cmd="yum install -y"
    else
        echo "Error: No supported package manager found (apt-get, dnf, or yum)"
        exit 1
    fi
fi

# Check what packages need to be installed
packages_to_install=()

if ! command -v jq >/dev/null 2>&1; then
    packages_to_install+=(jq)
fi

if ! command -v bc >/dev/null 2>&1; then
    packages_to_install+=(bc)
fi

if ! command -v curl >/dev/null 2>&1; then
    packages_to_install+=(curl)
fi

# Install packages if needed
if [ ${#packages_to_install[@]} -gt 0 ]; then
    echo "Installing packages: ${packages_to_install[*]}..."
    $update_cmd
    $install_cmd "${packages_to_install[@]}"
    echo "✅ Packages installed successfully"
else
    echo "✅ All required packages are already installed"
fi

# Verify installation
missing_packages=()
if ! command -v jq >/dev/null 2>&1; then
    missing_packages+=(jq)
fi
if ! command -v bc >/dev/null 2>&1; then
    missing_packages+=(bc)
fi
if ! command -v curl >/dev/null 2>&1; then
    missing_packages+=(curl)
fi

if [ ${#missing_packages[@]} -eq 0 ]; then
    echo "✅ Setup complete! All dependencies installed:"
    echo "  - jq version: $(jq --version)"
    echo "  - bc version: $(bc --version | head -1)"
    echo "  - curl version: $(curl --version | head -1)"
else
    echo "❌ Failed to install packages: ${missing_packages[*]}"
    exit 1
fi
