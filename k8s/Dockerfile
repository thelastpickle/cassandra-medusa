FROM ubuntu:18.04 as base

RUN mkdir /install
WORKDIR /install

RUN apt-get update && \
    apt-get install -y software-properties-common curl gnupg

RUN add-apt-repository ppa:kalon33/gamesgiroll -y

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
        debhelper \
        dh-python \
        python3-all \
        python3-all-dev \
        python3-dev \
        python-dev \
        python-pip \
        python3-pip \
        python3-setuptools \
        python3-venv \
        build-essential \
        devscripts \
        dh-virtualenv \
        equivs \
        wget \
        apt-transport-https \
        ca-certificates

ENV PATH=/root/.local/bin:$PATH

COPY . /build/

# General requirements
RUN python3 -m pip install -U pip && pip3 install --ignore-installed --user \
    -r /build/requirements.txt \
    -r /build/requirements-grpc-runtime.txt

# Amazon S3
RUN pip3 install --ignore-installed --user -r /build/requirements-s3.txt

# Azure
RUN pip3 install --ignore-installed --user -r /build/requirements-azure.txt \
     && pip3 install --ignore-installed --user azure-cli

# Build medusa itself so we can add the executables in the final image
RUN pip3 install --ignore-installed --user /build

# Could be python:slim, but we have a .sh entrypoint
FROM ubuntu:18.04

## add user
RUN groupadd -r cassandra --gid=999 && useradd -r -g cassandra --uid=999 --create-home cassandra

COPY --from=base /install /usr/local

# wget could happen in the build-phase
RUN apt-get update && apt-get install -y python3 python3-setuptools wget \
    && rm -rf /var/lib/apt/lists/*

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

USER cassandra
WORKDIR /home/cassandra

# GCP sdk
COPY --chown=cassandra:cassandra k8s/clean-cloud-sdk.sh /home/cassandra/clean-cloud-sdk.sh
RUN wget -q https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz \
    && tar -zxvf google-cloud-sdk.tar.gz \
    && google-cloud-sdk/install.sh --quiet \
    && chmod +x clean-cloud-sdk.sh && ./clean-cloud-sdk.sh
ENV DEBUG_VERSION 1
ENV DEBUG_SLEEP 0
ENV PATH=/home/cassandra/.local/bin:/home/cassandra/google-cloud-sdk/bin:$PATH

COPY --from=base --chown=cassandra:cassandra /root/.local /home/cassandra/.local

COPY --chown=cassandra:cassandra medusa /home/cassandra/medusa
COPY --chown=cassandra:cassandra k8s/docker-entrypoint.sh /home/cassandra

# Avoid Click locale errors when running medusa directly
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

WORKDIR /home/cassandra

ENTRYPOINT ["/home/cassandra/docker-entrypoint.sh"]
