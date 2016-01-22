package com.ameliant.tools.kafka.testdsl.config;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.serialization.Deserializer;

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

    public ConsumerConfigsBuilder bootstrapServers(String bootstrapServersConfig) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
    }

    public ConsumerConfigsBuilder groupId(String groupId) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public ConsumerConfigsBuilder autoCommitIntervalMs(long autoCommitIntervalMs) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Long.toString(autoCommitIntervalMs));
    }

    public ConsumerConfigsBuilder enableAutoCommit(boolean enableAutoCommit) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));
    }

    public ConsumerConfigsBuilder sessionTimeoutMs(long sessionTimeoutMs) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Long.toString(sessionTimeoutMs));
    }

    public Map<String, Object> build() {
        return consumerConfigs;
    }

    public ConsumerConfigsBuilder keyDeserializer(Class<? extends Deserializer> keyDeserializerClass) {
        Validate.notNull(keyDeserializerClass, "keyDeserializerClass is null");
        return new ConsumerConfigsBuilder(this, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getCanonicalName());
    }

    public ConsumerConfigsBuilder valueDeserializer(Class<? extends Deserializer> valueDeserializerClass) {
        Validate.notNull(valueDeserializerClass, "valueDeserializerClass is null");
        return new ConsumerConfigsBuilder(this, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getCanonicalName());
    }

    public ConsumerConfigsBuilder partitionAssignmentStrategy(Class<? extends PartitionAssignor> partitionAssignmentStrategy) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy.getCanonicalName());
    }

    public enum OffsetReset {
        earliest, latest, none;
    }

    public ConsumerConfigsBuilder autoOffsetReset(OffsetReset offsetReset) {
        return new ConsumerConfigsBuilder(this, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset.toString());
    }
}
