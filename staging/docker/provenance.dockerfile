# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

ARG BASE_IMAGE=invalid
FROM ${BASE_IMAGE}

ARG IMAGE_ROLE
ARG ARCHITECTURE
ARG CUDA_VERSION
ARG BUILD_CREATED
ARG PRESTO_SHA
ARG PRESTO_REPOSITORY
ARG PRESTO_REF
ARG VELOX_SHA
ARG VELOX_REPOSITORY
ARG VELOX_REF
ARG VELOX_TESTING_SHA
ARG VELOX_TESTING_REPOSITORY
ARG VELOX_TESTING_REF
ARG WORKFLOW_FILE
ARG WORKFLOW_RUN_ID
ARG WORKFLOW_RUN_ATTEMPT
ARG WORKFLOW_RUN_URL

LABEL org.opencontainers.image.created=${BUILD_CREATED} \
      org.opencontainers.image.revision=${VELOX_TESTING_SHA} \
      org.opencontainers.image.source="https://github.com/${VELOX_TESTING_REPOSITORY}" \
      org.opencontainers.image.url=${WORKFLOW_RUN_URL} \
      velox-testing.image.role=${IMAGE_ROLE} \
      velox-testing.image.architecture=${ARCHITECTURE} \
      velox-testing.image.cuda=${CUDA_VERSION} \
      velox-testing.presto.sha=${PRESTO_SHA} \
      velox-testing.presto.branch=${PRESTO_REF} \
      velox-testing.presto.ref=${PRESTO_REF} \
      velox-testing.presto.repository=${PRESTO_REPOSITORY} \
      velox-testing.velox.sha=${VELOX_SHA} \
      velox-testing.velox.branch=${VELOX_REF} \
      velox-testing.velox.ref=${VELOX_REF} \
      velox-testing.velox.repository=${VELOX_REPOSITORY} \
      velox-testing.workflow.sha=${VELOX_TESTING_SHA} \
      velox-testing.workflow.ref=${VELOX_TESTING_REF} \
      velox-testing.workflow.repository=${VELOX_TESTING_REPOSITORY} \
      velox-testing.workflow.file=${WORKFLOW_FILE} \
      velox-testing.workflow.run-id=${WORKFLOW_RUN_ID} \
      velox-testing.workflow.run-attempt=${WORKFLOW_RUN_ATTEMPT}

# Keep the existing flat Presto/Velox keys for benchmark tooling compatibility,
# and add the workflow/build fields needed to reproduce the image exactly.
RUN mkdir -p /opt/velox-testing && \
    python3 -c 'import json, sys; keys = ["schema_version", "image_role", "architecture", "cuda_version", "build_created", "presto_sha", "presto_branch", "presto_ref", "presto_repo", "velox_sha", "velox_branch", "velox_ref", "velox_repo", "velox_testing_sha", "velox_testing_ref", "velox_testing_repo", "workflow_file", "workflow_run_id", "workflow_run_attempt", "workflow_run_url"]; json.dump(dict(zip(keys, sys.argv[1:])), open("/opt/velox-testing/provenance.json", "w"), indent=2, sort_keys=True)' \
      "1" "${IMAGE_ROLE}" "${ARCHITECTURE}" "${CUDA_VERSION}" "${BUILD_CREATED}" \
      "${PRESTO_SHA}" "${PRESTO_REF}" "${PRESTO_REF}" "${PRESTO_REPOSITORY}" \
      "${VELOX_SHA}" "${VELOX_REF}" "${VELOX_REF}" "${VELOX_REPOSITORY}" \
      "${VELOX_TESTING_SHA}" "${VELOX_TESTING_REF}" "${VELOX_TESTING_REPOSITORY}" \
      "${WORKFLOW_FILE}" "${WORKFLOW_RUN_ID}" "${WORKFLOW_RUN_ATTEMPT}" "${WORKFLOW_RUN_URL}"
