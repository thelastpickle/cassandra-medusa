FROM --platform=linux/${TARGETARCH} python:3.12-alpine AS base

ARG TARGETARCH

RUN mkdir /install
WORKDIR /install

# Install build dependencies
RUN apk add --no-cache \
    make \
    cmake \
    gcc \
    g++ \
    musl-dev \
    zlib-dev \
    openssl-dev \
    linux-headers

ENV PATH=/root/.local/bin:$PATH

COPY . /build/

# General requirements
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
RUN pip install --no-cache-dir poetry==1.8.5

# Install ssh-python separately first
RUN pip install --no-cache-dir ssh-python==1.1.0

# Build medusa itself so we can add the executables in the final image
RUN cd /build && poetry build && poetry install

# Could be python:slim, but we have a .sh entrypoint
FROM --platform=linux/${TARGETARCH} python:3.12-alpine

# Reuse the architecture argument
ARG TARGETARCH

## add user
RUN addgroup -S cassandra && adduser -S cassandra -G cassandra

# Install runtime dependencies
RUN apk add --no-cache \
    wget \
    curl \
    jq \
    tzdata \
    && rm -rf /var/cache/apk/*

# Download the the latest grpc_health_probe binary build for the correct architecture
RUN curl -s https://api.github.com/repos/grpc-ecosystem/grpc-health-probe/releases/latest \
    | jq --arg TARGETARCH $TARGETARCH -r ' .assets[] | select(.name | contains("linux")) | select(.name | contains($TARGETARCH)) | .browser_download_url' \
    | xargs -I {} wget -qO/bin/grpc_health_probe {} &&  chmod +x /bin/grpc_health_probe

USER cassandra
WORKDIR /home/cassandra

ENV DEBUG_VERSION=1
ENV DEBUG_SLEEP=0
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