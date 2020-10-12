#!/usr/bin/env bash

set -e

case $1 in
  ""|bionic|xenial|buster|stretch)
    suites=("${1:-bionic}")
    ;;
  all)
    suites=(bionic xenial buster stretch)
    ;;
  *)
    echo "Unknown distribution suite - allowed values: 'all', 'bionic', 'xenial', 'buster', 'stretch'"
    exit 1
    ;;
esac

cd docker-build

for suite in "${suites[@]}"
do
  docker-compose build "cassandra-medusa-builder-${suite}"
  docker-compose run "cassandra-medusa-builder-${suite}"
done
