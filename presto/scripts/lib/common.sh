#!/bin/bash

# Common environment setup for Presto scripts
# Call setup_presto_env to initialize environment variables

setup_presto_env() {
  # export CCACHE_DIR if set
  export CCACHE_DIR="${CCACHE_DIR:-}"

  # Set default NO_SUBMODULES behavior
  # Default: fetch submodules (NO_SUBMODULES="")
  # To skip submodules, set NO_SUBMODULES to "true", "1", "yes" (case insensitive)
  case "${NO_SUBMODULES:-}" in
    true|TRUE|1|yes|YES)
      export NO_SUBMODULES="--no-submodules"
      ;;
    *)
      export NO_SUBMODULES=""
      ;;
  esac

  # Optional: Print current configuration for debugging
  if [[ "${PRESTO_DEBUG:-}" == "true" ]]; then
    echo "CCACHE_DIR: $CCACHE_DIR (default: /ccache)"
    echo "NO_SUBMODULES: $NO_SUBMODULES (default: fetch submodules)"
  fi
}