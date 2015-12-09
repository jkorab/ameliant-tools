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
      "configs" : { // defines Kafka config properties
        "global" : { // applies to both consumers and producers
          "bootstrap.servers" : "tcp://localhost:2181"
        },
        "producers" : { // applies to all producers
          "key.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
          "value.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
          "batch.size": "0",
          "timeout.ms" : "10000"
        },
        "consumers" : { // applies to all consumers
          // ...
        }
      },
      "producers" : [
        {
          "configs" : { // overrides the producers config above
            "timeout.ms" : "5000"
          },
          // test tool config for this producer
          // see ProducerDefinition for full set of flags (all have sensible defaults)
          "topic" : "foo", // mandatory
          "messagesToSend" : "1000",
          "sendBlocking": "false",
          "messageSize" : "100000"
        },
        {
          // another producer
          "topic" : "bar",
          "messagesToSend" : "200",
          "sendBlocking": "true",
          "messageSize" : "10000"
        }
      ],
      "consumers" : [
        {
          "configs" : { // overrides the consumers config above
            "timeout.ms" : "5000"
          },
          // see ConsumerDefinition for full set of flags
          "topic" : "foo", // mandatory
          "consumerGroupId", "bar" // mandatory
        }
      ]
    }

Example configs are located in `src/test/resources/test-profiles`.

Known issues:

1. The test will not shutdown gracefully if producers are sending messages synchronously (`"producer.type" : "sync`) and are blocked. The producer API does not define timeouts - it's in an obscure flag. 