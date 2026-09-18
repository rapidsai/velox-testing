ARG BASE_IMAGE=invalid
FROM ${BASE_IMAGE} AS basic

ARG PRESTO_SHA
ARG PRESTO_BRANCH
ARG PRESTO_REPOSITORY
ARG VELOX_SHA
ARG VELOX_BRANCH
ARG VELOX_REPOSITORY
LABEL velox-testing.presto.sha=${PRESTO_SHA} \
      velox-testing.presto.branch=${PRESTO_BRANCH} \
      velox-testing.presto.repository=${PRESTO_REPOSITORY} \
      velox-testing.velox.sha=${VELOX_SHA} \
      velox-testing.velox.branch=${VELOX_BRANCH} \
      velox-testing.velox.repository=${VELOX_REPOSITORY}
# Build the JSON with python so values are properly escaped (branch/repo can
# contain characters that would break a raw printf, e.g. a double-quote).
RUN mkdir -p /opt/velox-testing && \
    python3 -c 'import json, sys; keys = ["presto_sha", "presto_branch", "presto_repo", "velox_sha", "velox_branch", "velox_repo"]; json.dump(dict(zip(keys, sys.argv[1:])), open("/opt/velox-testing/provenance.json", "w"))' \
    "${PRESTO_SHA}" "${PRESTO_BRANCH}" "${PRESTO_REPOSITORY}" \
    "${VELOX_SHA}" "${VELOX_BRANCH}" "${VELOX_REPOSITORY}"

FROM basic AS extended

ARG IMAGE_ROLE
ARG ARCHITECTURE
ARG CUDA_VERSION
ARG BUILD_CREATED
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
      velox-testing.presto.ref=${PRESTO_BRANCH} \
      velox-testing.velox.ref=${VELOX_BRANCH} \
      velox-testing.workflow.sha=${VELOX_TESTING_SHA} \
      velox-testing.workflow.ref=${VELOX_TESTING_REF} \
      velox-testing.workflow.repository=${VELOX_TESTING_REPOSITORY} \
      velox-testing.workflow.file=${WORKFLOW_FILE} \
      velox-testing.workflow.run-id=${WORKFLOW_RUN_ID} \
      velox-testing.workflow.run-attempt=${WORKFLOW_RUN_ATTEMPT}

RUN python3 -c 'import json, sys; path = "/opt/velox-testing/provenance.json"; data = json.load(open(path)); keys = ["image_role", "architecture", "cuda_version", "build_created", "presto_ref", "velox_ref", "velox_testing_sha", "velox_testing_ref", "velox_testing_repo", "workflow_file", "workflow_run_id", "workflow_run_attempt", "workflow_run_url"]; data.update(zip(keys, sys.argv[1:])); data["schema_version"] = "1"; json.dump(data, open(path, "w"), indent=2, sort_keys=True)' \
    "${IMAGE_ROLE}" "${ARCHITECTURE}" "${CUDA_VERSION}" "${BUILD_CREATED}" \
    "${PRESTO_BRANCH}" "${VELOX_BRANCH}" "${VELOX_TESTING_SHA}" \
    "${VELOX_TESTING_REF}" "${VELOX_TESTING_REPOSITORY}" "${WORKFLOW_FILE}" \
    "${WORKFLOW_RUN_ID}" "${WORKFLOW_RUN_ATTEMPT}" "${WORKFLOW_RUN_URL}"
