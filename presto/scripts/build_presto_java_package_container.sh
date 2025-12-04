#!/bin/bash
set -euo pipefail

resolve_artifact() {
  local __resultvar=$1
  local search_root=$2
  shift 2
  local match=""
  for pattern in "$@"; do
    match=$(find "${search_root}" -maxdepth 5 -type f -name "${pattern}" -print -quit 2>/dev/null || true)
    if [[ -n "${match}" ]]; then
      printf -v "${__resultvar}" '%s' "${match}"
      return 0
    fi
  done
  return 1
}

echo "Building Presto Java from source with PRESTO_VERSION: ${PRESTO_VERSION}..."
./mvnw clean install --no-transfer-progress -DskipTests -pl \!presto-docs -pl \!presto-openapi -Dair.check.skip-all=true

echo "Copying artifacts with version ${PRESTO_VERSION}..."
SERVER_TARBALL=""
if ! resolve_artifact SERVER_TARBALL "presto-server/target" "presto-server-${PRESTO_VERSION}.tar.gz" "presto-server-*.tar.gz"; then
  echo 'ERROR: presto-server tarball not found'
  echo 'DEBUG: Listing available artifacts under presto-server/target'
  find presto-server/target -maxdepth 3 -type f -print 2>/dev/null || true
  exit 1
fi
echo "INFO: Using server tarball at ${SERVER_TARBALL}"
cp "${SERVER_TARBALL}" "docker/presto-server-${PRESTO_VERSION}.tar.gz"
if [[ ! -s "docker/presto-server-${PRESTO_VERSION}.tar.gz" ]]; then
  echo 'ERROR: Copied presto-server tarball is empty'
  exit 1
fi

FUNCTION_SERVER_JAR=""
if ! resolve_artifact FUNCTION_SERVER_JAR "presto-function-server/target" "presto-function-server-*-executable.jar" "presto-function-server-executable.jar"; then
  echo 'ERROR: presto-function-server executable jar not found'
  exit 1
fi
echo "INFO: Using function server jar at ${FUNCTION_SERVER_JAR}"
cp "${FUNCTION_SERVER_JAR}" "docker/presto-function-server-${PRESTO_VERSION}-executable.jar"

CLI_JAR=""
if ! resolve_artifact CLI_JAR "presto-cli/target" "presto-cli-*-executable.jar"; then
  echo 'ERROR: presto-cli executable jar not found'
  exit 1
fi
echo "INFO: Using CLI jar at ${CLI_JAR}"
cp "${CLI_JAR}" "docker/presto-cli-${PRESTO_VERSION}-executable.jar"
chmod +r "docker/presto-cli-${PRESTO_VERSION}-executable.jar"

echo "Build complete! Artifacts copied with version ${PRESTO_VERSION}"

