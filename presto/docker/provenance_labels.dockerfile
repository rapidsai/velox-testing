ARG BASE_IMAGE=invalid
FROM ${BASE_IMAGE}

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
