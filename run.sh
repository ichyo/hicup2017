#!/bin/sh

set -eu

docker build -t hicup .
docker run --rm -p 8080:80 -v "$(pwd)/data:/tmp/data" hicup
