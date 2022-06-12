# kplay

Plays a file onto a Kafka topic - one line == one message.

Run `--help` to see a full list of options. 

The primary useful option is `--rate`. The original intention of kplay was to simulate production workloads (with realistic messages-per-second rate) in dev environments.

## Build

`cargo build`

## Local Kafka

A docker-compose file is included for local development for spinning up a single broker wurstmeister/kafka cluster (https://hub.docker.com/r/wurstmeister/kafka/) exposed to localhost. Topics required for specific scenarios may be added to the `KAFKA_CREATE_TOPICS` environment variable in `docker-compose.yml`. The required format is described in https://github.com/wurstmeister/kafka-docker#automatically-create-topics.

## Docker

Build an image locally:

```
docker build -t kplay .
```

Run kplay help.
```
docker run --rm -it kplay --help
```

See `examples/movies/play-example-docker.sh` for a more thorough example of starting a kplay docker container.

## Example

The example folder of this repository contains a data file called `example.json` and a few example scripts for launching kplay. Before running any of the scripts:

Start the local docker Kafka in the background (the compose file automatically creates a topic called `example` on the cluster).

```
docker-compose up -d
```

Run kafkacat in another terminal so you can see your messages are coming in after you start kplay.

```
kafkacat -C -b localhost:9092 -t example
```

Run `cargo build` then:

```
./examples/movies/play-example.sh
```

You should see progress updates from kplay and consumed messages from kcat.

The `example.json` data file is an abridged and slightly transformed (into a line delimited format) version of American movies from https://github.com/jdorfman/awesome-json-datasets#movies.


