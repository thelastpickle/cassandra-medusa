# Copyright 2019 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
ARG EXTRA_PACKAGES="python-dev python-pip"
ARG BUILD_IMAGE=ubuntu:18.04
FROM ${BUILD_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_IMAGE
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
ENV PIP_DEFAULT_TIMEOUT 100
RUN echo "Acquire::http::Pipeline-Depth 0;" >> /etc/apt/apt.conf

# add a repo that contains a backport of dh-virtualenv 1.1 for focal, bionic, xenial
RUN apt-get update && \
    apt-get install -y software-properties-common

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        debhelper \
        gdebi-core \
        gcc \
        dh-python \
        python3-all \
        python3-all-dev \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-venv \
        python3-wheel \
        build-essential \
        cmake \
        devscripts \
        equivs \
        libevent-dev \
        wget $EXTRA_PACKAGES

RUN cd /tmp && \
    wget http://mirrors.kernel.org/ubuntu/pool/universe/d/dh-virtualenv/dh-virtualenv_1.0-1_all.deb && \
    gdebi -n dh-virtualenv*.deb && \
    rm dh-virtualenv_*.deb
        

RUN pip3 install greenlet
RUN pip3 install gevent

# Add entrypoint script
COPY packaging/docker-build/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
