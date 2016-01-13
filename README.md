# kafka-perf-test
A command-line performance test tool for Apache Kafka

![Build Status](https://travis-ci.org/jkorab/kafka-perf-test.svg)

This tool runs an arbitrary number of consumers and producers concurrently against a Kafka cluster.
The tool currently runs against Kafka 0.9.0 only.

To run:

    $ mvn clean install
    $ java -jar target/kafka-perf-test.jar -c src/test/resources/test-profiles/producer.json

The tool accepts a single argument, which is the location of a config file that details
the behaviour of the producers and consumers (collectively referred to as clients). The config file can be either
JSON or YAML; the right parser is auto-detected based on config file extension (YAML = `.yaml` or `.yml`).

Since Kafka clients each need their own Kafka config (a bag of properties for which you need
to look at [the manual](http://kafka.apache.org/documentation.html#configuration)),
the config format supports overrides in `config` blocks at each level to keep the file nice and DRY - and to keep you sane :)
Lower-lever config is merged with, and in the process overrides, config defined at a higher-level.

See the [Users Guide] (docs/UsersGuide.md) for a description of using this tool, and its configuration.

Example (pseudo-)JSON config (doesn't really do comments):

    {
      // see TestProfileDefinition for all properties
      "config" : {
        // Kafka config; applies to producers and consumers
        "bootstrap.servers" : "tcp://localhost:9092"
      },
      "producers" : {
        "config" : {
          // Kafka config; applies to all producers
          "request.required.acks": "ackFromLeader",
          "producer.type": "sync",
          "key.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
          "value.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
          "batch.size": "0",
          "timeout.ms" : "10000"
        },
        "instances" : [
          {
            // see ProducerDefinition for all properties
            "config" : {
              // Kafka config; just for this producer
              "timeout.ms" : "5000"
            },
            "topic" : "foo",
            "messagesToSend" : "1000",
            "sendBlocking": "false",
            "messageSize" : "100000"
          }
        ]
      },
      "consumers" : {
        "config" : {
            // Kafka config; applies to all consumers
            "key.deserializer" : "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "value.deserializer" : "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "enable.auto.commit" : "true",
            "auto.commit.interval.ms" : "1000",
            "auto.offset.reset" : "earliest"
        },
        "instances" : [
          {
            // see ConsumerDefinition for all properties
            "config" : {
              // Kafka config; just for this consumer
              "timeout.ms" : "5000",
              "group.id" : "foo1" // every consumer must define a unique one of these
            },
            "topic" : "foo"
          }
        ]
      }
    }

Example YAML config:
 
    # see TestProfileDefinition for all properties
    config:
      # Kafka config; applies to producers and consumers
      bootstrap.servers: "tcp://localhost:9092"
    maxDuration: 30
    concurrent: true
    autogenerateTopic: true
    producers:
      config:
        # Kafka config; applies to all producers
        request.timeout.ms: "10000"
        key.serializer: "org.apache.kafka.common.serialization.ByteArraySerializer"
        value.serializer: "org.apache.kafka.common.serialization.ByteArraySerializer"
        batch.size: "0"
        acks: "1"
        max.block.ms: "10000"
      instances:
        # see ProducerDefinition for all properties
      - config:
          # Kafka config; just for this producer
          timeout.ms: 5000
        messagesToSend: 1000
        messageSize: 1000
        sendBlocking: false
    consumers:
      config:
        # Kafka config; applies to all consumers
        key.deserializer: "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        value.deserializer: "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        enable.auto.commit: "true"
        auto.commit.interval.ms: "1000"
        auto.offset.reset: "earliest"
      instances:
        # see ConsumerDefinition for all properties
      - config:
          # Kafka config; just for this consumer
          group.id: "bar"
        messagesToReceive: 10000
        pollTimeout: 1000
        reportReceivedEvery: 1000
        receiveDelay: 0

