#!/usr/bin/env bash

set -e

case $1 in
  ""|jammy|bullseye|bookworm)
    suites=("${1:-bionic}")
    ;;
  all)
    suites=(jammy bullseye bookworm)
    ;;
  *)
    echo "Unknown distribution suite - allowed values: 'all', 'jammy', 'bullseye', 'bookworm'"
    exit 1
    ;;
esac

cd docker-build

for suite in "${suites[@]}"
do
  docker-compose build "cassandra-medusa-builder-${suite}"
  docker-compose run "cassandra-medusa-builder-${suite}"
done
