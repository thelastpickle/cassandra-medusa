FROM ubuntu:18.04 as base

RUN mkdir /install
WORKDIR /install

RUN apt-get update && \
    apt-get install -y software-properties-common curl gnupg

RUN add-apt-repository ppa:kalon33/gamesgiroll -y

COPY requirements.txt /requirements.txt
COPY requirements-grpc-runtime.txt /requirements-grpc-runtime.txt
COPY requirements-s3.txt /requirements-s3.txt 
ENV PATH=/root/.local/bin:$PATH

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

# Swap to non-root user for the rest of the build
#RUN addgroup --system medusa && adduser --system --group medusa
#USER medusa
#WORKDIR /home/medusa

RUN python3 -m pip install -U pip && pip3 install --ignore-installed --user -r /requirements.txt \
    && pip3 install --ignore-installed --user -r /requirements-grpc-runtime.txt \
    && pip3 install --ignore-installed --user -r /requirements-s3.txt

# Could be python:slim, but we have a .sh entrypoint
FROM ubuntu:18.04

## add user
RUN addgroup --system medusa && adduser --system --group medusa
# RUN chown -R medusa:medusa /app && chmod -R 755 /app

COPY --from=base /install /usr/local

# wget could happen in the build-phase
RUN apt-get update && apt-get install -y python3 wget

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe && \
    groupadd -r cassandra --gid=999 && useradd -r -g cassandra --uid=999 cassandra

USER medusa
WORKDIR /home/medusa

ENV DEBUG_VERSION 1
ENV DEBUG_SLEEP 0
ENV PATH=/home/medusa/.local/bin:$PATH

COPY --from=base --chown=medusa:medusa /root/.local /home/medusa/.local

COPY medusa /home/medusa/app/medusa
COPY k8s/docker-entrypoint.sh /home/medusa/app

WORKDIR /home/medusa/app

ENTRYPOINT ["/home/medusa/app/docker-entrypoint.sh"]
