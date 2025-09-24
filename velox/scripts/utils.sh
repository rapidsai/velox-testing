#!/bin/bash

# Shared utility functions for Velox build and test scripts

# Helper function to get BUILD_TYPE from container environment
get_build_type_from_container() {
    local compose_file=$1
    local container_name=$2
    
    docker compose -f "$compose_file" run --rm "${container_name}" bash -c "echo \$BUILD_TYPE"
}