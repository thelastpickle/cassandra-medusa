FROM  ubuntu:18.04 as base

RUN mkdir /install
WORKDIR /install

RUN apt-get update && \
    apt-get install -y software-properties-common curl gnupg

RUN add-apt-repository ppa:kalon33/gamesgiroll -y

RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

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
        ca-certificates \
        awscli \
        && mkdir -p /usr/local/gcloud \
        && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
        && /usr/local/gcloud/google-cloud-sdk/install.sh --quiet

COPY requirements.txt /requirements.txt
COPY requirements-grpc.txt /requirements-grpc.txt

RUN pip3 install -r /requirements.txt && \
    pip3 install -r /requirements-grpc.txt

FROM base

ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin
ENV DEBUG_VERSION 1
ENV DEBUG_SLEEP 0

COPY --from=base /install /usr/local

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe && \
    groupadd -r cassandra --gid=999 && useradd -r -g cassandra --uid=999 cassandra

COPY build/lib/medusa /app/medusa
COPY k8s/docker-entrypoint.sh /app

WORKDIR /app

ENTRYPOINT ["/app/docker-entrypoint.sh"]
