#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Helper script executed *inside* the Docker container by run_test_in_docker.
#
# Expected environment variables:
#   REUSE_VENV              – "true" to reuse an existing Python venv
#   VENV_DIR                – Python virtual environment directory name
#   OUTPUT_DIR              – output directory to chown back to the host user
#   HOST_UID, HOST_GID      – host user/group ids for chown
#
# Positional arguments are forwarded to pytest.

set -e

# shellcheck disable=SC1091
source /opt/rh/gcc-toolset-14/enable 2>/dev/null || true

# Log file for Spark/Velox warnings and other stderr output.
LOG_FILE="${OUTPUT_DIR}/spark_warnings.log"
mkdir -p "${OUTPUT_DIR}"

# Discover Gluten JARs from /opt/gluten/jars/.
GLUTEN_JARS=$(find /opt/gluten/jars/ -maxdepth 1 -name 'gluten-*.jar' -print 2>/dev/null | paste -sd, -)

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
  # Allow overriding PySpark version (e.g. for Spark 3.4 JAR compatibility).
  if [[ -n "${PYSPARK_VERSION}" ]]; then
    echo "Overriding PySpark to version ${PYSPARK_VERSION}"
    pip install --disable-pip-version-check -q "pyspark==${PYSPARK_VERSION}"
  fi
else
  activate_python_virtual_env "${VENV_DIR}"
fi

echo "Warnings/stderr redirected to ${LOG_FILE}"
pytest "$@" --gluten-jar-path "${GLUTEN_JARS}" 2>"${LOG_FILE}"
EXIT_CODE=$?

if [[ -s "${LOG_FILE}" ]]; then
  echo "Warning log saved to ${LOG_FILE} ($(wc -l < "${LOG_FILE}") lines)"
fi

chown -R "${HOST_UID}:${HOST_GID}" "${VENV_DIR}/" "${OUTPUT_DIR}/" 2>/dev/null || true
exit "${EXIT_CODE}"
