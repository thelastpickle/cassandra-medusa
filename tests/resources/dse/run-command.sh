#!/usr/bin/env bash

# set -x

# first argument is the DSE version
DSE_VERSION=$1

# discard the first argument and look at the next one. that's our command to run
shift
COMMAND=$1

# get all the other arguments as options
shift
ARGUMENTS=$@

if [ -z "$DSE_VERSION" ]; then
  echo "Usage: $0 <dse-version>"
  exit 1
fi

CWD=$(pwd)
ROOT_PATH="${CWD}/resources/dse"
# echo "Root path is ${ROOT_PATH}"

"${ROOT_PATH}/dse-${DSE_VERSION}/bin/${COMMAND}" ${ARGUMENTS}

exit 0
