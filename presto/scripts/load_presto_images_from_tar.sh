#!/bin/bash
#
# Load Presto images from tar.gz files and retag with the current user.
#
# Supports local paths or rsync-style remote paths (user@host:/path/file.tar.gz).
#
# Example:
#   ./load_presto_images_from_tar.sh \
#     --coordinator user@host:/path/presto-coordinator.tar.gz \
#     --worker-gpu user@host:/path/presto-native-worker-gpu.tar.gz \
#     --worker-cpu user@host:/path/presto-native-worker-cpu.tar.gz
#

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
USER_NAME="${USER:-$(whoami)}"

usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [options]

Options:
  --coordinator <path>   tar.gz for presto-coordinator image
  --worker-gpu <path>    tar.gz for presto-native-worker-gpu image
  --worker-cpu <path>    tar.gz for presto-native-worker-cpu image
  --worker-java <path>   tar.gz for presto-java-worker image (optional)
  -h, --help             Show this help

Paths may be local files or rsync-style remote paths (user@host:/path/file.tar.gz).
EOF
}

is_remote_path() {
  [[ "$1" =~ ^[^/]+:.+ ]]
}

expand_tilde() {
  local path="$1"
  if [[ "$path" == "~/"* ]]; then
    echo "${HOME}/${path#~/}"
  else
    echo "$path"
  fi
}

load_and_retag() {
  local tar_path="$1"
  local target_repo="$2"
  local user_tag="$3"

  if [[ ! -f "$tar_path" ]]; then
    echo "ERROR: File not found: $tar_path" >&2
    exit 1
  fi

  echo "Loading image from ${tar_path}..."
  local output
  if command -v pv >/dev/null 2>&1; then
    if [[ "$tar_path" == *.tar.gz || "$tar_path" == *.tgz ]]; then
      output=$(pv "$tar_path" | gzip -dc | docker load)
    else
      output=$(pv "$tar_path" | docker load)
    fi
  else
    if [[ "$tar_path" == *.tar.gz || "$tar_path" == *.tgz ]]; then
      output=$(gzip -dc "$tar_path" | docker load)
    else
      output=$(docker load < "$tar_path")
    fi
  fi

  echo "${output}"

  local first_tag=""
  local matched_tag=""
  local image_id=""
  while IFS= read -r line; do
    if [[ "$line" == "Loaded image: "* ]]; then
      local tag="${line#Loaded image: }"
      if [[ -z "$first_tag" ]]; then
        first_tag="$tag"
      fi
      if [[ "$tag" == "${target_repo}:"* || "$tag" == "${target_repo}" ]]; then
        matched_tag="$tag"
      fi
    elif [[ "$line" == "Loaded image ID: "* ]]; then
      image_id="${line#Loaded image ID: }"
    fi
  done <<< "$output"

  local source_image="${matched_tag:-$first_tag}"
  if [[ -z "$source_image" ]]; then
    source_image="$image_id"
  fi

  if [[ -z "$source_image" ]]; then
    echo "ERROR: Could not determine loaded image tag/ID for ${tar_path}" >&2
    exit 1
  fi

  local new_tag="${target_repo}:${user_tag}"
  echo "Tagging ${source_image} -> ${new_tag}"
  docker tag "$source_image" "$new_tag"
}

declare -A ROLE_PATHS=()
declare -A ROLE_IMAGES=()
ROLE_IMAGES[coordinator]="presto-coordinator"
ROLE_IMAGES[worker_gpu]="presto-native-worker-gpu"
ROLE_IMAGES[worker_cpu]="presto-native-worker-cpu"
ROLE_IMAGES[worker_java]="presto-java-worker"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --coordinator)
      ROLE_PATHS[coordinator]="$2"
      shift 2
      ;;
    --worker-gpu)
      ROLE_PATHS[worker_gpu]="$2"
      shift 2
      ;;
    --worker-cpu)
      ROLE_PATHS[worker_cpu]="$2"
      shift 2
      ;;
    --worker-java)
      ROLE_PATHS[worker_java]="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ${#ROLE_PATHS[@]} -eq 0 ]]; then
  echo "ERROR: At least one image tarball must be provided." >&2
  usage
  exit 1
fi

TMP_DIR="$(mktemp -d -p /tmp presto_image_load_XXXXXX)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

declare -A REMOTE_GROUPS=()
declare -A REMOTE_MAP=()
declare -A LOCAL_PATHS=()

for role in "${!ROLE_PATHS[@]}"; do
  raw_path="$(expand_tilde "${ROLE_PATHS[$role]}")"
  if is_remote_path "$raw_path"; then
    prefix="${raw_path%%:*}"
    rpath="${raw_path#*:}"
    REMOTE_GROUPS["$prefix"]+="${rpath} "
    REMOTE_MAP["$role"]="${prefix}:${rpath}"
  else
    LOCAL_PATHS["$role"]="$raw_path"
  fi
done

need_rsync=0
if [[ ${#REMOTE_GROUPS[@]} -gt 0 ]]; then
  need_rsync=1
fi

local_copy_roles=()
for role in "${!LOCAL_PATHS[@]}"; do
  src="${LOCAL_PATHS[$role]}"
  if [[ "$src" != "${TMP_DIR}/"* ]]; then
    local_copy_roles+=("$role")
  fi
done

if [[ ${#local_copy_roles[@]} -gt 0 ]]; then
  need_rsync=1
fi

if [[ $need_rsync -eq 1 ]]; then
  if ! command -v rsync >/dev/null 2>&1; then
    echo "ERROR: rsync is required for remote or cross-disk local copies." >&2
    exit 1
  fi
fi

for prefix in "${!REMOTE_GROUPS[@]}"; do
  local_dir="${TMP_DIR}/remote_${prefix//[^a-zA-Z0-9]/_}"
  mkdir -p "$local_dir"
  read -r -a paths <<< "${REMOTE_GROUPS[$prefix]}"
  echo "Fetching from ${prefix} via rsync..."
  rsync -av --info=progress2 "${paths[@]/#/${prefix}:}" "$local_dir/"
  for role in "${!REMOTE_MAP[@]}"; do
    if [[ "${REMOTE_MAP[$role]}" == "${prefix}:"* ]]; then
      rpath="${REMOTE_MAP[$role]#*:}"
      LOCAL_PATHS["$role"]="${local_dir}/$(basename "$rpath")"
    fi
  done
done

for role in "${local_copy_roles[@]}"; do
  src="${LOCAL_PATHS[$role]}"
  dest="${TMP_DIR}/local_${role}_$(basename "$src")"
  echo "Copying local image tarball to ${dest}..."
  rsync -a --info=progress2 "$src" "$dest"
  LOCAL_PATHS["$role"]="$dest"
done

for role in coordinator worker_gpu worker_cpu worker_java; do
  if [[ -n "${LOCAL_PATHS[$role]:-}" ]]; then
    load_and_retag "${LOCAL_PATHS[$role]}" "${ROLE_IMAGES[$role]}" "$USER_NAME"
  fi
done

echo "Done. Images are tagged with :${USER_NAME}"
