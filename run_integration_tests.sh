# Copyright 2019 Spotify AB. All rights reserved.
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

#!/usr/bin/env bash
set -x
# Keep the following rm for the sake of running the integration tests in CI
rm -Rf .python-version

SCENARIO=""
STORAGE_TAGS=""
LOCAL="yes"
S3="no"
GCS="no"
AZURE="no"
IBM="no"
LOGGING_FLAGS=""

while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "run_integration_tests.sh: Run the integration test suite for Medusa"
      echo " "
      echo " "
      echo "options:"
      echo "-h, --help                                  show brief help"
      echo "-t, --test=1                                Test scenario to run (1, 2, 3, etc...). If not provided, all tests will run"
      echo "--no-local                                  Don't run the tests with the local storage backend"
      echo "--s3                                        Include S3 in the storage backends"
      echo "--gcs                                       Include GCS in the storage backends"
      echo "--azure                                     Include Azure in the storage backends"
      echo "--ibm                                       Include IBM in the storage backends"
      echo "--cassandra-version                         Cassandra version to test"
      echo "-v                                          Verbose output (logging won't be captured by behave)"
      exit 0
      ;;
    -t)
      shift
      if test $# -gt 0; then
        SCENARIO="--tags=@$1"
      else
        echo "no test scenario specified"
        exit 1
      fi
      shift
      ;;
    --test*)
      TEST=`echo $1 | sed -e 's/^[^=]*=//g'`
      SCENARIO="--tags=@${TEST}"
      shift
      ;;
    --no-local)
      LOCAL="no"
      shift
      ;;
    --s3)
      S3="yes"
      shift
      ;;
    --gcs)
      GCS="yes"
      shift
      ;;
    --azure)
      AZURE="yes"
      shift
      ;;
    --ibm)
      IBM="yes"
      shift
      ;;
    -v)
      LOGGING="--no-capture --no-capture-stderr --format=plain"
      shift
      ;;
    --cassandra-version*)
      CASSANDRA_VERSION=`echo $1 | sed -e 's/^[^=]*=//g'`
      shift
      ;;
    *)
      break
      ;;
  esac
done

export LOCAL_JMX=yes
export PYTHONWARNINGS="ignore"
pip3 install -r requirements.txt
pip3 install -r requirements-grpc.txt
pip3 install -r requirements-test.txt
cd tests/integration
if [ "$LOCAL" == "yes" ]
then
    STORAGE_TAGS="@local"
fi

if [ "$S3" == "yes" ]
then
    if [ "$STORAGE_TAGS" == "" ]
    then
        STORAGE_TAGS="@s3"
    else
        STORAGE_TAGS="${STORAGE_TAGS},@s3"
    fi
fi

if [ "$GCS" == "yes" ]
then
    if [ "$STORAGE_TAGS" == "" ]
    then
        STORAGE_TAGS="@gcs"
    else
        STORAGE_TAGS="${STORAGE_TAGS},@gcs"
    fi
fi

if [ "$AZURE" == "yes" ]
then
    if [ "$STORAGE_TAGS" == "" ]
    then
        STORAGE_TAGS="@azure"
    else
        STORAGE_TAGS="${STORAGE_TAGS},@azure"
    fi
fi

if [ "$IBM" == "yes" ]
then
    if [ "$STORAGE_TAGS" == "" ]
    then
        STORAGE_TAGS="@ibm"
    else
        STORAGE_TAGS="${STORAGE_TAGS},@ibm"
    fi
fi

if [[ -z "$CASSANDRA_VERSION" ]]; then
   echo "Cassandra version is not set, using default."
   CASSANDRA_VERSION_FLAG=""
else
   echo "Cassandra version is set to ${CASSANDRA_VERSION}"
   CASSANDRA_VERSION_FLAG="-D cassandra-version=${CASSANDRA_VERSION}"
fi

PYTHONPATH=../.. behave --stop $SCENARIO --tags=$STORAGE_TAGS $LOGGING $CASSANDRA_VERSION_FLAG
