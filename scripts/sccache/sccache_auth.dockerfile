FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install basic dependencies
RUN <<EOF
apt-get update && apt-get install -y \
    curl \
    wget \
    ca-certificates \
    gnupg \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*
EOF

# Install GitHub CLI
RUN <<EOF
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null
apt-get update
apt-get install gh -y
rm -rf /var/lib/apt/lists/*
EOF

# Install gh-nv-gha-aws plugin manually
RUN <<EOF
NV_GHA_AWS_VERSION="0.1.1"
ARCH=$(dpkg --print-architecture)
if [ "$ARCH" = "amd64" ]; then ARCH="amd64"; elif [ "$ARCH" = "arm64" ]; then ARCH="arm64"; fi
mkdir -p /root/.local/share/gh/extensions/gh-nv-gha-aws
wget --no-hsts -q -O /root/.local/share/gh/extensions/gh-nv-gha-aws/gh-nv-gha-aws \
    "https://github.com/nv-gha-runners/gh-nv-gha-aws/releases/download/v${NV_GHA_AWS_VERSION}/gh-nv-gha-aws_v${NV_GHA_AWS_VERSION}_linux-${ARCH}"
chmod 0755 /root/.local/share/gh/extensions/gh-nv-gha-aws/gh-nv-gha-aws
EOF

# Create plugin manifest
RUN <<EOF
cat > /root/.local/share/gh/extensions/gh-nv-gha-aws/manifest.yml << 'MANIFEST'
owner: nv-gha-runners
name: gh-nv-gha-aws
host: github.com
tag: v0.1.1
ispinned: false
path: $HOME/.local/share/gh/extensions/gh-nv-gha-aws/gh-nv-gha-aws
MANIFEST
EOF

# Create output directory for credentials
RUN mkdir -p /output
