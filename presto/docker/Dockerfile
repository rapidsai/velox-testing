# Extend the official Presto image to include jq for TPC-H benchmarking
FROM prestodb/presto:latest

# Install jq and bc for JSON processing and calculations in benchmark scripts
USER root
RUN dnf update -y && dnf install -y jq bc && dnf clean all

# Switch back to presto user (if the base image uses one)
# The official prestodb/presto image runs as root, so this is optional
# USER presto

# Keep the same entrypoint from the base image
