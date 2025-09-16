# script which shares common variables and functions for the velox build and test scripts

# container name
CONTAINER_NAME="velox-adapters-build"

NUM_THREADS=${NUM_THREADS:-$(($(nproc) * 3 / 4))}

# expected output directory
EXPECTED_OUTPUT_DIR="/opt/velox-build/release"
EXPECTED_OUTPUT_LIB_DIR="${EXPECTED_OUTPUT_DIR}/lib"

COMPOSE_FILE="../docker/docker-compose.adapters.yml"

# -----------------------------
# Docker runtime helper methods
# -----------------------------

# Returns 0 if docker has 'nvidia' runtime registered, else 1
docker_runtime_nvidia_available() {
  local runtimes
  runtimes=$(docker info --format '{{json .Runtimes}}' 2>/dev/null || true)
  [[ "$runtimes" == *"\"nvidia\""* ]]
}

# Returns 0 if a GPU appears present on the host, else 1
gpu_present() {
  if command -v nvidia-smi >/dev/null 2>&1; then
    nvidia-smi -L >/dev/null 2>&1 && return 0
  fi
  [[ -e /dev/nvidia0 || -e /dev/nvidiactl ]] && return 0
  return 1
}

# Resolve docker runtime from a mode: cpu|gpu|auto
# Echoes: runc or nvidia; returns non-zero on invalid mode or unavailable GPU runtime
resolve_docker_runtime() {
  local mode="$1"
  case "${mode,,}" in
    cpu)
      echo "runc"
      ;;
    gpu)
      if docker_runtime_nvidia_available && gpu_present; then
        echo "nvidia"
      else
        echo "ERROR: Requested GPU runtime but 'nvidia' runtime or GPU is unavailable." >&2
        return 1
      fi
      ;;
    auto)
      if docker_runtime_nvidia_available && gpu_present; then
        echo "nvidia"
      else
        echo "runc"
      fi
      ;;
    *)
      echo "ERROR: Invalid docker runtime mode: '${mode}'. Use cpu|gpu|auto." >&2
      return 1
      ;;
  esac
}

# Sets and exports DOCKER_RUNTIME based on mode (cpu|gpu|auto)
# Usage: set_docker_runtime_from_mode auto
set_docker_runtime_from_mode() {
  local mode="$1"
  local resolved
  if ! resolved=$(resolve_docker_runtime "$mode"); then
    return 1
  fi
  export DOCKER_RUNTIME="$resolved"
}
