#!/bin/bash

#
# Runs the `kafka-player-rs` application against a local Kafka.
#

set -eu

scriptpath="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

message_file=$scriptpath/example.json
message_count=10000
message_rate=1000000
topic=example
bootstrap_servers=localhost:9092

if [[ ! -f $scriptpath/example.json ]]; then
  echo "Unzipping example.json"
  tar -xzvf $scriptpath/example.json.tar.gz -C $scriptpath/
fi

echo "Invoking debug binary"

$scriptpath/../../target/debug/kafka-player-rs \
  -f $message_file \
  -c $message_count \
  -r $message_rate \
  -t $topic \
  -b $bootstrap_servers

