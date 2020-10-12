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
ARG BUILD_IMAGE=ubuntu:18.04
FROM ${BUILD_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
ENV PIP_DEFAULT_TIMEOUT 100
RUN echo "Acquire::http::Pipeline-Depth 0;" >> /etc/apt/apt.conf

# add a repo that contains a backport of dh-virtualenv 1.1 for Bionic & xenial
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    case $BUILD_IMAGE in ubuntu*) \
        add-apt-repository ppa:kalon33/gamesgiroll -y ;; \
    esac

# install dependencies
RUN apt-get update \
    && apt-get install -y \
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
        python3-wheel \
        build-essential \
        cmake \
        devscripts \
        dh-virtualenv \
        equivs \
        libevent-dev

RUN pip3 install greenlet
RUN pip3 install gevent

# Add entrypoint script
COPY packaging/docker-build/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
