FROM --platform=linux/${TARGETARCH} ubuntu:24.04 AS base

ARG TARGETARCH

RUN mkdir /install
WORKDIR /install

RUN apt-get update && \
    apt-get install -y software-properties-common curl gnupg

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
        debhelper \
        python3-all \
        python3-all-dev \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-venv \
        build-essential \
        devscripts \
        equivs \
        wget \
        apt-transport-https \
        ca-certificates

ENV PATH=/root/.local/bin:$PATH

COPY . /build/

# General requirements
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
# RUN python3 -m pip install -U pip --break-system-packages && pip3 install --ignore-installed --user poetry==1.8.5 --break-system-packages
RUN pip3 install --ignore-installed --user poetry==1.8.5 --break-system-packages

# Build medusa itself so we can add the executables in the final image
RUN cd /build && poetry build && poetry install

# Could be python:slim, but we have a .sh entrypoint
FROM --platform=linux/${TARGETARCH} ubuntu:24.04

# Reuse the architecture argument
ARG TARGETARCH

## add user
RUN groupadd -r cassandra --gid=999 && useradd -r -g cassandra --uid=999 --create-home cassandra

# wget could happen in the build-phase
RUN apt-get update && apt-get install -y python3 python3-setuptools wget curl jq \
    && rm -rf /var/lib/apt/lists/*

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