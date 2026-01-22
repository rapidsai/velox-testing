#!/usr/bin/env bash
set -euo pipefail

HOST_UID=${HOST_UID:-0}
HOST_GID=${HOST_GID:-0}
HOST_USER=${HOST_USER:-hostuser}
HOST_HOME=${HOST_HOME:-/home/${HOST_USER}}

# If UID/GID not provided (or zero), derive from mounted source dir
if [ "${HOST_UID}" = "0" ] || [ "${HOST_GID}" = "0" ]; then
  if stat /workspace/velox >/dev/null 2>&1; then
    HOST_UID=$(stat -c %u /workspace/velox)
    HOST_GID=$(stat -c %g /workspace/velox)
  fi
fi

cmd=("$@")
if [ ${#cmd[@]} -eq 0 ]; then
  cmd=(tail -f /dev/null)
fi

# Ensure sudo is available
if ! command -v sudo >/dev/null 2>&1; then
  dnf install -y sudo
fi
# Ensure cmake is available system-wide and point /usr/local/bin/cmake to it
dnf install -y cmake ninja-build git || true
if [ -x /usr/bin/cmake ]; then
  rm -f /usr/local/bin/cmake
  ln -s /usr/bin/cmake /usr/local/bin/cmake
fi

# Create group/user if missing (allow out-of-range UID/GID with -o); do not exit on failure
set +e
groupadd -o -g "${HOST_GID}" hostgroup 2>/dev/null
groupadd -o -g "${HOST_GID}" "${HOST_USER}" 2>/dev/null
if ! getent group "${HOST_USER}" >/dev/null 2>&1; then
  echo "${HOST_USER}:x:${HOST_GID}:" >> /etc/group
fi
useradd -o -u "${HOST_UID}" -g "${HOST_GID}" -m -d "${HOST_HOME}" -s /bin/bash "${HOST_USER}" 2>/dev/null
if ! getent passwd "${HOST_USER}" >/dev/null 2>&1; then
  echo "${HOST_USER}:x:${HOST_UID}:${HOST_GID}:${HOST_USER}:${HOST_HOME}:/bin/bash" >> /etc/passwd
fi
set -e

mkdir -p "${HOST_HOME}"
chown -R "${HOST_UID}:${HOST_GID}" "${HOST_HOME}"

# Ensure hostuser can access root-installed tools (e.g., uv-installed cmake under /root/.local)
if [ -d /root/.local ]; then
  chown -R "${HOST_UID}:${HOST_GID}" /root/.local || true
  chmod -R g+rx /root/.local || true
fi

# Passwordless sudo for the mapped user
echo "${HOST_USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${HOST_USER}
chmod 440 /etc/sudoers.d/${HOST_USER}

# Path setup: mirror host absolute paths and build dir symlink
if [ -n "${HOST_VELOX_ABS:-}" ] && [ "${HOST_VELOX_ABS}" != "/workspace/velox" ]; then
  mkdir -p "$(dirname "${HOST_VELOX_ABS}")"
  if [ -e "${HOST_VELOX_ABS}" ] && [ ! -L "${HOST_VELOX_ABS}" ]; then
    echo "Skipping symlink for HOST_VELOX_ABS; target exists: ${HOST_VELOX_ABS}"
  else
    ln -sfn /workspace/velox "${HOST_VELOX_ABS}" || true
  fi
fi
if [ -n "${BUILD_BASE_DIR:-}" ]; then
  mkdir -p /workspace/velox/velox-build
  if [ -e /opt/velox-build ] && [ ! -L /opt/velox-build ]; then
    echo "Skipping symlink for /opt/velox-build; target exists"
  else
    ln -sfn /workspace/velox/velox-build /opt/velox-build || true
  fi
fi

# Final fallback: ensure hostuser exists before exec
if ! getent passwd "${HOST_USER}" >/dev/null 2>&1; then
  echo "${HOST_USER}:x:${HOST_UID}:${HOST_GID}:${HOST_USER}:${HOST_HOME}:/bin/bash" >> /etc/passwd
fi
if ! getent group "${HOST_USER}" >/dev/null 2>&1; then
  echo "${HOST_USER}:x:${HOST_GID}:" >> /etc/group
fi
mkdir -p "${HOST_HOME}"
chown -R "${HOST_UID}:${HOST_GID}" "${HOST_HOME}"

# Optional ccls setup (disabled by default unless explicitly enabled)
# if { [ "${ENABLE_CCLS:-false}" = "true" ] || [ "${ENABLE_CCLS:-false}" = "ON" ]; } && [ -x /usr/local/bin/ccls_bootstrap.sh ]; then
#   su - "${HOST_USER}" -c "ENABLE_CCLS=${ENABLE_CCLS} HOST_UID=${HOST_UID} HOST_GID=${HOST_GID} HOST_USER=${HOST_USER} HOST_HOME=${HOST_HOME} /usr/local/bin/ccls_bootstrap.sh" || true
# fi

exec su - "${HOST_USER}" -c "exec ${cmd[*]}"
