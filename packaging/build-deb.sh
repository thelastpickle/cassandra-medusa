#!/usr/bin/env bash

set -e

case $1 in
  ""|bionic|buster|focal|bullseye)
    suites=("${1:-bionic}")
    ;;
  all)
    suites=(focal bionic buster bullseye)
    ;;
  *)
    echo "Unknown distribution suite - allowed values: 'all', 'bionic', 'buster', 'focal', 'bullseye'"
    exit 1
    ;;
esac

cd docker-build

for suite in "${suites[@]}"
do
  docker-compose build "cassandra-medusa-builder-${suite}"
  docker-compose run "cassandra-medusa-builder-${suite}"
done
