# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Self-contained runtime image for Spark + Gluten GPU acceleration.
#
# Stage 1 (build): runs builder.sh --mode=direct to build the full pipeline
# Stage 2 (runtime): copies artifacts, installs Spark + Python, sets ENV vars
#
# Build:
#   docker build \
#     --build-context gluten=/path/to/gluten \
#     --build-context velox=/path/to/velox \
#     --build-arg CUDA_ARCH="80;86" \
#     -t gluten:spark-gpu-runtime \
#     -f docker/runtime.dockerfile .
#
# Run:
#   docker run --rm -it --gpus all gluten:spark-gpu-runtime spark-shell
#
# Run with custom build output (replace entire gluten-deploy):
#   docker run --rm -it --gpus all \
#     -v /path/to/my/gluten-deploy:/opt/gluten-deploy \
#     gluten:spark-gpu-runtime spark-shell

# ══════════════════════════════════════════════════════════════════════════════
# Stage 1: Build Gluten + Velox via builder.sh
# ══════════════════════════════════════════════════════════════════════════════
ARG PREBUILD_IMAGE=artifactory.nvidia.com/sw-spark-docker/gluten-builder:latest
FROM ${PREBUILD_IMAGE} AS builder

ARG CUDA_ARCH="75;80;86;89;90"
ARG SPARK_VERSION=3.5
ARG ENABLE_HDFS=ON
ARG ENABLE_S3=OFF

# Copy source trees.
COPY --from=gluten . /opt/gluten
COPY --from=velox  . /opt/velox

# Copy builder scripts.
COPY scripts/ /opt/spark_experimental/scripts/

# Optional Maven settings override. The prebuild image has /usr/local/bin/mvns
# wrapper that auto-appends -s if /opt/maven-settings/settings.xml exists.
# Stage the same dir here so runtime builds also pick it up.
COPY .docker-maven-settings/ /opt/maven-settings/

# Run builder.sh in direct mode. All settings come from ARGs above.
RUN bash /opt/spark_experimental/scripts/builder.sh \
      --mode=direct \
      --gluten_dir=/opt/gluten \
      --velox_dir=/opt/velox \
      --cuda_arch="${CUDA_ARCH}" \
      --spark_version="${SPARK_VERSION}" \
      --enable_hdfs="${ENABLE_HDFS}" \
      --enable_s3="${ENABLE_S3}" \
      --output_dir=/opt/gluten-deploy

# ══════════════════════════════════════════════════════════════════════════════
# Stage 2: Runtime
# ══════════════════════════════════════════════════════════════════════════════
FROM ${PREBUILD_IMAGE} AS runtime

ARG SPARK_VERSION=3.5
# Resolve full Spark release version (e.g. 3.5 -> 3.5.5).
ARG SPARK_FULL_VERSION=3.5.5
ARG HADOOP_PROFILE=hadoop3

# ── Copy build artifacts from stage 1 ────────────────────────────────────────
# Layout (same as builder.sh output):
#   /opt/gluten-deploy/
#     gluten-velox-bundle-*.jar
#     libs/          ← native .so files
#
# Mount the entire directory to replace with a custom build:
#   docker run -v /my/build:/opt/gluten-deploy ...
COPY --from=builder /opt/gluten-deploy /opt/gluten-deploy

# ── Install Apache Spark ─────────────────────────────────────────────────────
RUN set -ex \
  && curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_FULL_VERSION}/spark-${SPARK_FULL_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
     | tar xz -C /opt \
  && ln -s /opt/spark-${SPARK_FULL_VERSION}-bin-${HADOOP_PROFILE} /opt/spark

# ── Install Python 3 + pyspark ───────────────────────────────────────────────
RUN set -ex \
  && dnf install -y python3 python3-pip --allowerasing \
  && python3 -m pip install --no-cache-dir pyspark==${SPARK_FULL_VERSION} \
  && dnf clean all

# ── Entrypoint: resolve JARs + write spark-defaults dynamically ──────────────
# At container start, discovers whatever JAR + libs are in /opt/gluten-deploy
# (baked-in or volume-mounted) and writes spark-defaults.conf accordingly.
RUN cat > /opt/entrypoint.sh <<'ENTRY'
#!/bin/bash
set -e

DEPLOY_DIR=/opt/gluten-deploy
SPARK_CONF=/opt/spark/conf/spark-defaults.conf

# Find the Gluten bundle JAR.
GLUTEN_JAR=$(find "$DEPLOY_DIR" -maxdepth 1 -name 'gluten-velox-bundle-*.jar' -o -name 'gluten-*.jar' 2>/dev/null | head -1)

# (Re)generate spark-defaults.conf so it reflects the current mount content.
mkdir -p /opt/spark/conf
cat > "$SPARK_CONF" <<CONF
spark.plugins                                           org.apache.gluten.GlutenPlugin
spark.jars                                              ${GLUTEN_JAR:-}
spark.driver.extraLibraryPath                           ${DEPLOY_DIR}/libs
spark.executor.extraLibraryPath                         ${DEPLOY_DIR}/libs
spark.memory.offHeap.enabled                            true
spark.memory.offHeap.size                               20g
spark.gluten.sql.columnar.cudf                          true
spark.shuffle.manager                                   org.apache.spark.shuffle.sort.ColumnarShuffleManager
spark.gluten.sql.columnar.forceShuffledHashJoin         true
CONF

exec "$@"
ENTRY
RUN chmod +x /opt/entrypoint.sh

# ── Environment variables ────────────────────────────────────────────────────
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV GLUTEN_DEPLOY_DIR=/opt/gluten-deploy
ENV GPU_LIBS=/opt/gluten-deploy/libs
ENV LD_LIBRARY_PATH="/opt/gluten-deploy/libs:${LD_LIBRARY_PATH}"

# Resolve JAVA_HOME from the base image's JDK.
RUN for _jd in /usr/lib/jvm/java-*-openjdk* /usr/lib/jvm/java-*/; do \
      [ -f "$_jd/include/jni.h" ] && echo "JAVA_HOME=$_jd" \
        && echo "export JAVA_HOME=$_jd" >> /etc/profile.d/java.sh \
        && break; \
    done

WORKDIR /opt/spark
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["bash"]
