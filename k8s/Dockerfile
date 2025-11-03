FROM --platform=linux/${TARGETARCH} registry.access.redhat.com/ubi10/ubi-minimal:latest AS base

ARG TARGETARCH

RUN mkdir /install
WORKDIR /install
RUN microdnf install -y \
        python3 \
        python3-devel \
        python3-pip \
        python3-setuptools \
        gcc \
        gcc-c++ \
        make \
        wget \
        ca-certificates \
    && microdnf clean all

ENV PATH=/root/.local/bin:$PATH

COPY . /build/

# General requirements
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
RUN pip3 install --ignore-installed --user poetry==2.2.1

# Build medusa itself so we can add the executables in the final image
RUN cd /build && poetry build && poetry install

# Could be python:slim, but we have a .sh entrypoint
FROM --platform=linux/${TARGETARCH} registry.access.redhat.com/ubi10/ubi-minimal:latest

# Reuse the architecture argument
ARG TARGETARCH

## add user
RUN microdnf install -y shadow-utils \
    && groupadd -r cassandra --gid=999 && useradd -r -g cassandra --uid=999 --create-home cassandra \
    && microdnf remove -y shadow-utils \
    && microdnf clean all

# wget could happen in the build-phase
RUN microdnf install -y python3 python3-setuptools wget jq findutils \
    && microdnf clean all

# Download the the latest grpc_health_probe binary build for the correct architecture
RUN curl -s https://api.github.com/repos/grpc-ecosystem/grpc-health-probe/releases/latest \
    | jq --arg TARGETARCH $TARGETARCH -r ' .assets[] | select(.name | contains("linux")) | select(.name | contains($TARGETARCH)) | .browser_download_url' \
    | xargs -I {} wget -qO/bin/grpc_health_probe {} &&  chmod +x /bin/grpc_health_probe

USER cassandra
WORKDIR /home/cassandra

ENV DEBUG_VERSION 1
ENV DEBUG_SLEEP 0
ENV PATH=/home/cassandra/.local/bin:/home/cassandra/google-cloud-sdk/bin:/home/cassandra/bin:$PATH
ENV PYTHONPATH=/home/cassandra

COPY --from=base --chown=cassandra:cassandra /root/.local /home/cassandra/.local
COPY --from=base --chown=cassandra:cassandra /build/.venv /home/cassandra/.venv
COPY --from=base --chown=cassandra:cassandra /build/pyproject.toml /home/cassandra/pyproject.toml
COPY --chown=cassandra:cassandra medusa /home/cassandra/medusa
COPY --chown=cassandra:cassandra k8s/docker-entrypoint.sh /home/cassandra

RUN mkdir -p /home/cassandra/bin
COPY --chown=cassandra:cassandra k8s/medusa.sh /home/cassandra/bin/medusa

# Avoid Click locale errors when running medusa directly
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

WORKDIR /home/cassandra

ENTRYPOINT ["/home/cassandra/docker-entrypoint.sh"]