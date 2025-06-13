#!/usr/bin/env bash

# The container tag should closely match what is used in `docker/Dockerfile` in vmpooler-deployment
docker run -it --rm \
  -v $(pwd):/app \
  jruby:9.4.3.0-jdk11 \
  /bin/bash -c 'apt-get update -qq && apt-get install -y --no-install-recommends make git netbase && cd /app && gem install bundler && bundle install --jobs 3; echo "LOCK_FILE_UPDATE_EXIT_CODE=$?"'
