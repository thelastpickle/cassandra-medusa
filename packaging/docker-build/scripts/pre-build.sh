#!/usr/bin/env bash

set -xe

cd ${WORKDIR}/cassandra-medusa

export DEBFULLNAME="The Last Pickle"
export DEBEMAIL="oss@thelastpickle.com"
MEDUSA_VERSION=$(cat VERSION)
changes_description="Release ${MEDUSA_VERSION} of Medusa - Backups for Apache Cassandra."
dch --package "cassandra-medusa" --newversion "${MEDUSA_VERSION}" "${changes_description}"
dch -r --distribution stable ignored
