FROM docker@sha256:0135662b510037ea581d99c2e5929c5e01185139c0b86986a418bd4da0b98a44 AS docker_cli

FROM python@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2

COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker_cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates coreutils curl findutils gawk grep jq procps sed util-linux

COPY benchmark_data_tools/cleanroom_controller_requirements.lock /requirements/lock.txt
RUN python -m pip install --no-cache-dir -r /requirements/lock.txt

CMD ["sleep", "infinity"]
