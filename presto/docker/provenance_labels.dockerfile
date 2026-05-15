ARG BASE_IMAGE
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
