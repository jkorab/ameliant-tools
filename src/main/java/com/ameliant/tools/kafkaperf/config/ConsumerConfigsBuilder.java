package com.ameliant.tools.kafkaperf.config;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building up Kafka consumer config maps.
 * @author jkorab
 */
public class ConsumerConfigsBuilder {

    private final Map<String, Object> consumerConfigs;

    public ConsumerConfigsBuilder() {
        consumerConfigs = new HashMap<>();
    }

    // Copy constructor
    private ConsumerConfigsBuilder(ConsumerConfigsBuilder builder, String key, Object value) {
        consumerConfigs = new HashMap<>();
        consumerConfigs.putAll(builder.consumerConfigs);
        consumerConfigs.put(key, value);
    }

    public ConsumerConfigsBuilder bootstrapServersConfig(String bootstrapServersConfig) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
    }

    public ConsumerConfigsBuilder zookeeperSessionTimeoutMs(long zookeeperSessionTimeoutMs) {
        return new ConsumerConfigsBuilder(this, "zookeeper.session.timeout.ms", Long.toString(zookeeperSessionTimeoutMs));
    }

    public ConsumerConfigsBuilder zookeeperSyncTimeMs(long zookeeperSyncTimeMs) {
        return new ConsumerConfigsBuilder(this, "zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
    }

    public ConsumerConfigsBuilder groupId(String groupId) {
        return new ConsumerConfigsBuilder(this, "group.id", groupId);
    }

    public ConsumerConfigsBuilder autoCommitIntervalMs(long autoCommitIntervalMs) {
        return new ConsumerConfigsBuilder(this, "auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));
    }

    public Map<String, Object> build() {
        return consumerConfigs;
    }

    public ConsumerConfigsBuilder keyDeserializer(Class<? extends Deserializer> keyDeserializerClass) {
        Validate.notNull(keyDeserializerClass, "keyDeserializerClass is null");
        return new ConsumerConfigsBuilder(this, "key.deserializer", keyDeserializerClass.getCanonicalName());
    }

    public ConsumerConfigsBuilder valueDeserializer(Class<? extends Deserializer> valueDeserializerClass) {
        Validate.notNull(valueDeserializerClass, "valueDeserializerClass is null");
        return new ConsumerConfigsBuilder(this, "value.deserializer", valueDeserializerClass.getCanonicalName());
    }

    public enum PartitionAssignmentStrategy {
        range, roundrobin;
    }

    public ConsumerConfigsBuilder partitionAssignmentStrategy(PartitionAssignmentStrategy partitionAssignmentStrategy) {
        return new ConsumerConfigsBuilder(this, "partition.assignment.strategy", partitionAssignmentStrategy.name());
    }

}
