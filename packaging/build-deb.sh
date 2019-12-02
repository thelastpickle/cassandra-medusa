#!/usr/bin/env bash

set -xe

cd docker-build
docker-compose build
docker-compose run cassandra-medusa-builder
