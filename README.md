# kafka-perf-test
A command-line performance test tool for Apache Kafka


To run:

    $ mvn clean install
    $ java -jar target/kafka-perf-test.jar -c src/test/resources/test-profiles/producer.json

Known issues:

1. The test will not shutdown gracefully if producers are sending messages synchronously (`"producer.type" : "sync`) and are blocked. The producer API does not define timeouts - it's in an obscure flag. 