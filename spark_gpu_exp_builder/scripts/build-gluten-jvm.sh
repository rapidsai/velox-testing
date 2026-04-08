#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Build Gluten JVM (Maven) — produces the bundle JAR.
# Direct Maven invocation, no wrapper scripts.
#
# Required env vars:
#   GLUTEN_DIR       — path to gluten source root
#
# Optional env vars:
#   SPARK_VERSION    — 3.3|3.4|3.5|4.0|4.1 (default: 3.5)
#   MAVEN_SETTINGS   — path to custom Maven settings.xml (uses -s flag)

set -exu

: "${GLUTEN_DIR:?ERROR: GLUTEN_DIR is not set}"

SPARK_VERSION="${SPARK_VERSION:-3.5}"

echo "=== Building Gluten JVM (Spark ${SPARK_VERSION}) ==="

cd "$GLUTEN_DIR"

# ── Downgrade maven-shade-plugin to avoid ASM bug ─────────────────────────────
# maven-shade-plugin 3.5.x–3.6.x has an ASM bug that throws
# ArrayStoreException / NullPointerException when rewriting certain classes
# (protobuf builders, Arrow vector impls, eclipse-collections maps).
# Arrow upstream pins to 3.2.4 for this reason.  We patch the parent pom
# in-place before building and restore it afterwards.
SHADE_PLUGIN_VERSION="3.2.4"
PARENT_POM="$GLUTEN_DIR/pom.xml"
SHADE_PATCHED=false

if grep -q '<artifactId>maven-shade-plugin</artifactId>' "$PARENT_POM"; then
  CURRENT_SHADE_VER=$(grep -A1 'maven-shade-plugin' "$PARENT_POM" \
    | grep '<version>' | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
  if [ "$CURRENT_SHADE_VER" != "$SHADE_PLUGIN_VERSION" ]; then
    echo "  Patching maven-shade-plugin: ${CURRENT_SHADE_VER} → ${SHADE_PLUGIN_VERSION}"
    # Create backup
    cp "$PARENT_POM" "${PARENT_POM}.shade-bak"
    # Replace the version on the line following maven-shade-plugin
    sed -i "/<artifactId>maven-shade-plugin<\/artifactId>/{n;s|<version>${CURRENT_SHADE_VER}</version>|<version>${SHADE_PLUGIN_VERSION}</version>|}" "$PARENT_POM"
    SHADE_PATCHED=true
  fi
fi

restore_shade_version() {
  if [ "$SHADE_PATCHED" = true ] && [ -f "${PARENT_POM}.shade-bak" ]; then
    mv "${PARENT_POM}.shade-bak" "$PARENT_POM"
    echo "  Restored maven-shade-plugin version in pom.xml"
  fi
}
trap restore_shade_version EXIT

# Maven command: use `mvns` wrapper (from prebuild image, auto-appends -s if
# /opt/maven-settings/settings.xml exists). If MAVEN_SETTINGS is explicitly
# set, override with `mvn -s` directly.
if [ -n "${MAVEN_SETTINGS:-}" ] && [ -f "${MAVEN_SETTINGS}" ]; then
  MVN_CMD="mvn -s ${MAVEN_SETTINGS}"
  echo "  Using explicit Maven settings: ${MAVEN_SETTINGS}"
elif command -v mvns &>/dev/null; then
  MVN_CMD="mvns"
else
  MVN_CMD="mvn"
fi

major=$(echo "$SPARK_VERSION" | cut -d'.' -f1)
if [ "$major" -ge 4 ]; then
  MVN_PROFILES="-Pbackends-velox -Pspark-${SPARK_VERSION} -Pjava-17 -Pscala-2.13"
else
  MVN_PROFILES="-Pbackends-velox -Pspark-${SPARK_VERSION}"
fi

# shellcheck disable=SC2086
${MVN_CMD} clean install ${MVN_PROFILES} -DskipTests

echo "=== Gluten JVM build complete ==="
# Show the output JAR
ls -lh "$GLUTEN_DIR"/package/target/gluten-velox-bundle-spark*.jar 2>/dev/null || true
