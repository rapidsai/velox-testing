# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Lightweight test image with Python 3.12 and JDK 21 for running
# integration tests / benchmarks with a statically-linked Gluten JAR.

FROM ubuntu:24.04

RUN apt update && apt install -y --no-install-recommends \
        openjdk-21-jdk-headless \
        python3.12 \
        python3.12-venv \
        python3-pip \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt install -y --no-install-recommends gnupg && \
    echo "deb http://developer.download.nvidia.com/devtools/repos/ubuntu$(. /etc/os-release && echo "$VERSION_ID" | tr -d .)/$(dpkg --print-architecture) /" \
        | tee /etc/apt/sources.list.d/nvidia-devtools.list && \
    apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub && \
    apt update && \
    apt install -y nsight-systems-cli && \
    rm -rf /var/lib/apt/lists/* && \
    NSYS_BIN="$(ls -d /opt/nvidia/nsight-systems-cli/*/target-linux-x64/nsys | sort -V | tail -1)" && \
    ln -sf "${NSYS_BIN}" /usr/local/bin/nsys

RUN ARCH=$(dpkg --print-architecture) && \
    ln -sf /usr/lib/jvm/java-21-openjdk-${ARCH} /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ARG SPARK_VERSION=3.5.5
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        | tar -xz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark && \
    curl -fsSL -o "/opt/spark/jars/spark-connect_2.12-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar"

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY launch_spark_connect_server.sh /opt/spark/
