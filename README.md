# ameliant-tools
![Build Status](https://travis-ci.org/jkorab/ameliant-tools.svg)

A set of tools to ease working with Zookeeper and Kafka.

1. [kafka-perf-tool] (kafka/kafka-perf-tool/README.md) - a load testing tool for Apache Kafka; formerly `kafka-perf-test`
1. [kafka-listener] (kafka/kafka-listener) - a listener abstraction for Kafka consumers that aims towards simplifying 
reliable once-one consumption; inspired by Spring's `DefaultMessageListenerContainer` 
1. [kafka-test-dsl] (kafka/kafka-test-dsl) - a fluent DSL for embedding Kafka servers in JUnit tests
1. [zookeeper-test-dsl] (zookeeper/zookeeper) - a fluent DSL for embedding Zookeepers servers in JUnit tests
1. [tools-support] (tools-support) - support library for common activities, such as finding available ports
