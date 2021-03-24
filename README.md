# kplay

Plays a file onto a Kafka topic - one line == one message.

Run `--help` to see a full list of options. The primary useful option is `--message-rate`, which can be used to simulate real production workloads.

## Build

`cargo build`

## Local Kafka

A docker-compose file is included for local development for spinning up a single broker wurstmeister/kafka cluster (https://hub.docker.com/r/wurstmeister/kafka/) exposed to localhost. Topics required for specific scenarios may be added to the `KAFKA_CREATE_TOPICS` environment variable in `docker-compose.yml`. The required format is described in https://github.com/wurstmeister/kafka-docker#automatically-create-topics.

## Example

The example folder of this repository contains a data file called `example.json` and a launch script called `play-example.sh` to play the data file onto a local Kafka. Run the example with the following steps:

```
# start the kafka docker containers in the background - this will also create a topic called `example` on the cluster.
docker-compose up -d

# play the example data file onto the local kafka
./examples/movies/play-example.sh
```

Once kplay starts, run kafkacat in another terminal to verify your messages are coming in.

```
kafkacat -C -b localhost:9092 -t example
```

The example plays 10,000 messages at a rate of one message every two seconds.

The `example.json` data file is an abridged and slightly transformed (into a line delimited format) version of American movies from https://github.com/jdorfman/awesome-json-datasets#movies.

