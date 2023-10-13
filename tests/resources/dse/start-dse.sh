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

export CASSANDRA_LOG_DIR="${ROOT_PATH}/dse-6.8.38/cassandra/logs"

# -s enables search and graph
"${ROOT_PATH}/dse-6.8.38/bin/dse" cassandra -f -s 2>/dev/null >/dev/null  &

# wait for DSE to start but time out after 60 seconds
while ! nc -z localhost 9042 2>/dev/null >/dev/null; do
  sleep 1
  ((t++)) && ((t==60)) && echo "DSE failed to start" && exit 1
done

exit 0