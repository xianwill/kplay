#!/bin/bash

#
# Uses docker to run the `kplay` application against a local Kafka.
#
# Prerequisite:
# You must first run `docker build -t kplay .` from the repo root to build the image.
#
# This example also shows usage of
# * queue-timeout
# * queue-size-pause-count
# * queue-size-pause-millis
#


set -eu

scriptpath="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Run kplay

# Message file will be mounted in the docker container.
message_file=example.json
message_count=1000000
message_rate=1000000
progress_interval=20
topic=example
bootstrap_servers=localhost:9092

if [[ ! -f $scriptpath/example.json ]]; then
  echo "Unzipping example.json"
  tar -xzvf $scriptpath/example.json.tar.gz -C $scriptpath/
fi

docker run --rm -it --network=host \
  -v "$scriptpath/$message_file:/$message_file" \
  -e "RUST_LOG=debug" \
  -e "RUST_BACKTRACE=1" \
  kplay \
  -f "/$message_file" \
  -c $message_count \
  -r $message_rate \
  -p $progress_interval \
  -t $topic \
  -b $bootstrap_servers \
  --queue-timeout-seconds 30 \
  --queue-size-pause-count 100 \
  --queue-size-pause-millis 100

