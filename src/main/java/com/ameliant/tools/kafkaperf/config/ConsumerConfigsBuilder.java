package com.ameliant.tools.kafkaperf.config;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
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

    public ConsumerConfigsBuilder zookeeperSessionTimeoutMs(long zookeeperSessionTimeoutMs) {
        return new ConsumerConfigsBuilder(this, "zookeeper.session.timeout.ms", Long.toString(zookeeperSessionTimeoutMs));
    }

    public ConsumerConfigsBuilder zookeeperSyncTimeMs(long zookeeperSyncTimeMs) {
        return new ConsumerConfigsBuilder(this, "zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
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
        return new ConsumerConfigsBuilder(this, "key.deserializer", keyDeserializerClass.getCanonicalName());
    }

    public ConsumerConfigsBuilder valueDeserializer(Class<? extends Deserializer> valueDeserializerClass) {
        Validate.notNull(valueDeserializerClass, "valueDeserializerClass is null");
        return new ConsumerConfigsBuilder(this, "value.deserializer", valueDeserializerClass.getCanonicalName());
    }

    public ConsumerConfigsBuilder partitionAssignmentStrategy(Class<? extends PartitionAssignor> partitionAssignmentStrategy) {
        return new ConsumerConfigsBuilder(this, "partition.assignment.strategy", partitionAssignmentStrategy.getCanonicalName());
    }

    public enum OffsetReset {
        earliest, latest, none;
    }

    public ConsumerConfigsBuilder autoOffsetReset(OffsetReset offsetReset) {
        return new ConsumerConfigsBuilder(this, "auto.offset.reset", offsetReset.toString());
    }
}
