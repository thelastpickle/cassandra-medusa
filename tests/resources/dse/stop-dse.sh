#!/usr/bin/env bash

# set -x

DSE_VERSION=$1

if [ -z "$DSE_VERSION" ]; then
  echo "Usage: $0 <dse-version>"
  exit 1
fi

CWD=$(pwd)
ROOT_PATH="${CWD}/resources/dse"
# echo "Root path is ${ROOT_PATH}"

"${ROOT_PATH}/dse-${DSE_VERSION}/bin/dse" cassandra-stop

exit 0
