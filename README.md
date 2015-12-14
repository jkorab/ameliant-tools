# kafka-perf-test
A command-line performance test tool for Apache Kafka

This tool runs an arbitrary number of consumers and producers concurrently against a Kafka cluster.
The tool currently runs against Kafka 0.9.0 only.

To run:

    $ mvn clean install
    $ java -jar target/kafka-perf-test.jar -c src/test/resources/test-profiles/producer.json

The tool accepts a single argument, which is the location of a JSON config file that details
the behaviour of the producers and consumers (collectively referred to as clients).

Since Kafka clients each need their own Kafka config (a bag of properties for which you need
to look at [the manual](http://kafka.apache.org/documentation.html#configuration)),
the config format supports overrides to keep the file nice and DRY - and to keep you sane :)

Lower-lever config is merged with, and in the process overrides, config defined at a higher-level.

Sample config is as follows (pseudo-JSON, as it normally doesn't support comments):

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

Example config are located in `src/test/resources/test-profiles`.