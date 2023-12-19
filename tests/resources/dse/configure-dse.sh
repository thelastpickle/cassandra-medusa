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

${ROOT_PATH}/download-dse.sh ${DSE_VERSION}

# echo "Extracting dse version $DSE_VERSION"
rm -rf "${ROOT_PATH}/dse-${DSE_VERSION}" || true
tar xf "${ROOT_PATH}/dse-${DSE_VERSION}-bin.tar.gz" -C "${ROOT_PATH}" 2>/dev/null >/dev/null

NODE_ROOT_PATH="${ROOT_PATH}/dse-${DSE_VERSION}"
NODE_JVM_SERVER_OPTIONS="${NODE_ROOT_PATH}/resources/cassandra/conf/jvm-server.options"
NODE_CASSANDRA_YAML="${NODE_ROOT_PATH}/resources/cassandra/conf/cassandra.yaml"

# a folder where all the node's disk presence (data, commitlog, caches, etc) will go
# this is meant to substitute /var/lib/cassandra
# echo "Preparing directories for DSE to use"
NODE_CASSANDRA_DIR="${NODE_ROOT_PATH}/cassandra"
mkdir -p ${NODE_CASSANDRA_DIR}
rm -rf ${NODE_CASSANDRA_DIR}/*

# jvm settings
# echo "Tweaking JVM settings"

if [[ "$OSTYPE" == "darwin"* ]]; then
        # encode ' as \x27 to avoid having them escaped when used in the command later
        SED_INPLACE_OPTION="-I \x27\x27"
else
        SED_INPLACE_OPTION="-i"
fi

sed ${SED_INPLACE_OPTION} -e "s/-Xss256k/-Xss1024k/" ${NODE_JVM_SERVER_OPTIONS}

# cassandra.yaml settings
# echo "Tweaking cassandra.yaml"
sed ${SED_INPLACE_OPTION} -e 's#/var/lib/cassandra#'"${NODE_CASSANDRA_DIR}"'#' ${NODE_CASSANDRA_YAML}

# logging settings
# echo "Tweaking logging settings"
sed ${SED_INPLACE_OPTION} -e 's#<appender-ref ref="STDOUT" />#<!-- <appender-ref ref="STDOUT" /> -->#' ${NODE_ROOT_PATH}/resources/cassandra/conf/logback.xml
sed ${SED_INPLACE_OPTION} -e 's#<appender-ref ref="STDOUT" />#<!-- <appender-ref ref="STDOUT" /> -->#' ${NODE_ROOT_PATH}/resources/spark/conf/logback-spark-executor.xml
sed ${SED_INPLACE_OPTION} -e 's#<appender-ref ref="STDOUT" />#<!-- <appender-ref ref="STDOUT" /> -->#' ${NODE_ROOT_PATH}/demos/portfolio_manager/resources/WEB-INF/classes/logback.xml
