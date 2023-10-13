#!/usr/bin/env bash

# set -x

DSE_VERSION=$1

if [ -z "$DSE_VERSION" ]; then
  echo "Usage: $0 <dse-version>"
  exit 1
fi

CWD=$(pwd)
ROOT_PATH="${CWD}/resources/dse"

# echo "Downloading DSE version $DSE_VERSION"

curl -X POST \
  -o "${ROOT_PATH}/dse-${DSE_VERSION}-bin.tar.gz" \
  "https://downloads.datastax.com/enterprise/dse-${DSE_VERSION}-bin.tar.gz"
