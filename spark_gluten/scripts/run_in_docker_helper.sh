#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Helper script executed *inside* the Docker container by run_in_docker.
#
# Expected environment variables:
#   SETUP_CMD               – shell snippet to run first (e.g. enable gcc-toolset)
#   CONTAINER_GLUTEN_JARS   – explicit JAR path(s); when empty, JARs are
#                             auto-discovered from /opt/gluten/jars/
#   REUSE_VENV              – "true" to reuse an existing Python venv
#   VENV_DIR                – Python virtual environment directory name
#   OUTPUT_DIR              – output directory to chown back to the host user
#   HOST_UID, HOST_GID      – host user/group ids for chown
#
# Positional arguments are forwarded to pytest.

set -e

# Run optional setup command (e.g. enable gcc-toolset).
if [[ -n "${SETUP_CMD}" ]]; then
  eval "${SETUP_CMD}"
fi

# Resolve Gluten JARs.
if [[ -n "${CONTAINER_GLUTEN_JARS}" ]]; then
  GLUTEN_JARS="${CONTAINER_GLUTEN_JARS}"
else
  GLUTEN_JARS=$(ls -1 /opt/gluten/jars/gluten-*.jar 2>/dev/null | paste -sd, -)
fi

if [[ -z "${GLUTEN_JARS}" ]]; then
  echo "Error: No Gluten JAR files found"
  exit 1
fi

# Set up Python virtual environment.
SCRIPT_DIR="$(pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../../scripts/py_env_functions.sh"

if [[ "${REUSE_VENV}" != "true" ]]; then
  trap 'delete_python_virtual_env "$VENV_DIR"' EXIT
fi

TEST_DIR=$(readlink -f "${SCRIPT_DIR}/../testing")

if [[ ! -d "${VENV_DIR}" || "${REUSE_VENV}" != "true" ]]; then
  init_python_virtual_env "${VENV_DIR}"
  pip install --disable-pip-version-check -q -r "${TEST_DIR}/requirements.txt"
else
  activate_python_virtual_env "${VENV_DIR}"
fi

pytest --static-gluten-jar-path "${GLUTEN_JARS}" "$@"
EXIT_CODE=$?

chown -R "${HOST_UID}:${HOST_GID}" "${VENV_DIR}/" "${OUTPUT_DIR}/" 2>/dev/null || true
exit "${EXIT_CODE}"
