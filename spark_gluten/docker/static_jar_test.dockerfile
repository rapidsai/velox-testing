# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Lightweight test image with Python 3.12 and JDK 21 for running
# integration tests / benchmarks with a statically-linked Gluten JAR.

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
        python3.12 \
        python3.12-venv \
        python3-pip \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
