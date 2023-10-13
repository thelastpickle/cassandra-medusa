#!/usr/bin/env bash

# set -x

DSE_VERSION=$1
KEYSPACE=$2
TABLE=$3

if [ -z "DSE_VERSION" ]; then
  echo "Usage: $0 <keyspace> <table>"
  exit 1
fi

if [ -z "KEYSPACE" ]; then
  echo "Usage: $0 <keyspace> <table>"
  exit 1
fi

if [ -z "TABLE" ]; then
  echo "Usage: $0 <keyspace> <table>"
  exit 1
fi

CWD=$(pwd)
ROOT_PATH="${CWD}/resources/dse"
# echo "Root path is ${ROOT_PATH}"

NODE_ROOT_PATH="${ROOT_PATH}/dse-${DSE_VERSION}"

# echo "Configuring DSE search"
"${NODE_ROOT_PATH}/bin/dsetool" create_core "${KEYSPACE}.${TABLE}" schema="${ROOT_PATH}/solr-schema.xml" solrconfig="${ROOT_PATH}/solr-config.xml" 2>/dev/null >/dev/null

exit 0
