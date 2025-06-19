<!--
# Copyright 2019 Spotify AB. All rights reserved.
# Copyright 2021 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

![Build Status](https://github.com/thelastpickle/cassandra-medusa/actions/workflows/ci.yml/badge.svg?branch=master)

[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat-square)](https://cloudsmith.io/~thelastpickle/repos/medusa/packages/)

[![codecov](https://codecov.io/gh/thelastpickle/cassandra-medusa/branch/master/graph/badge.svg?token=KTDCRD82NU)](https://codecov.io/gh/thelastpickle/cassandra-medusa)

![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fthelastpickle%2Fcassandra-medusa%2Fmaster%2Fpyproject.toml)

Medusa for Apache Cassandra&trade;
==================================

Medusa is an Apache Cassandra backup system.

Features
--------
Medusa is a command line tool that offers the following features:

* Single node backup
* Single node restore
* Cluster wide in place restore (restoring on the same cluster that was used for the backup)
* Cluster wide remote restore (restoring on a different cluster than the one used for the backup)
* Backup purge
* Support for local storage, Google Cloud Storage (GCS), Azure Blob Storage and AWS S3 (and its compatibles)
* Support for clusters using single tokens or vnodes
* Full or differential backups

Medusa currently does not support (but we would gladly accept help with changing that):

* Cassandra deployments with multiple data folder directories.

Documentation
-------------
* [Installation](docs/Installation.md)
* [Configuration](docs/Configuration.md)
* [Usage](docs/Usage.md)

For user questions and general/dev discussions, please join the #cassandra-medusa channel on the ASF slack at [http://s.apache.org/slack-invite](http://s.apache.org/slack-invite).

Docker images
-------------
You can find the Docker images for Cassandra Medusa at [https://hub.docker.com/r/k8ssandra/medusa](https://hub.docker.com/r/k8ssandra/medusa).

Dependencies
------------

Medusa requires Python 3.9 or newer.

For information on the packaged dependencies of Medusa for Apache Cassandra&reg; and their licenses, check out our [open source report](https://app.fossa.com/reports/cac72e73-1214-4e6d-8476-76567e08db21).


