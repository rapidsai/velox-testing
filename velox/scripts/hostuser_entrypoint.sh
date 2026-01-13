#!/usr/bin/env bash
set -euo pipefail

HOST_UID=${HOST_UID:-0}
HOST_GID=${HOST_GID:-0}
HOST_USER=${HOST_USER:-hostuser}
HOST_HOME=${HOST_HOME:-/home/${HOST_USER}}

cmd=("$@")
if [ ${#cmd[@]} -eq 0 ]; then
  cmd=(tail -f /dev/null)
fi

# Ensure sudo is available
if ! command -v sudo >/dev/null 2>&1; then
  dnf install -y sudo
fi

# Create group/user if missing
if ! getent group "${HOST_GID}" >/dev/null 2>&1; then
  groupadd -g "${HOST_GID}" "${HOST_USER}"
fi
if ! getent passwd "${HOST_UID}" >/dev/null 2>&1; then
  useradd -u "${HOST_UID}" -g "${HOST_GID}" -m -d "${HOST_HOME}" -s /bin/bash "${HOST_USER}"
fi

mkdir -p "${HOST_HOME}"
chown -R "${HOST_UID}:${HOST_GID}" "${HOST_HOME}"

# Passwordless sudo for the mapped user
echo "${HOST_USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${HOST_USER}
chmod 440 /etc/sudoers.d/${HOST_USER}

exec su - "${HOST_USER}" -c "exec ${cmd[*]}"
