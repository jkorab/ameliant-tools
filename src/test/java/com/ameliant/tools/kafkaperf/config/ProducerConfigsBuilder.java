package com.ameliant.tools.kafkaperf.config;


import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building up Kafka producer config maps.
 * @author jkorab
 */
public class ProducerConfigsBuilder {

    private final Map<String, Object> producerConfigs;

    public ProducerConfigsBuilder() {
        producerConfigs = new HashMap<>();
    }

    // Copy constructor
    private ProducerConfigsBuilder(ProducerConfigsBuilder builder, String key, Object value) {
        producerConfigs = new HashMap<>();
        producerConfigs.putAll(builder.producerConfigs);
        producerConfigs.put(key, value);
    }

    public ProducerConfigsBuilder bootstrapServers(String bootstrapServers) {
        return new ProducerConfigsBuilder(this, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public enum RequestRequiredAcks {
        noAck(0),
        ackFromLeader(1),
        ackFromInSyncReplicas(-1);

        private int flag;

        RequestRequiredAcks(int flag) {
            this.flag = flag;
        }

        public int getFlag() {
            return flag;
        }
    }

    public ProducerConfigsBuilder requestRequiredAcks(RequestRequiredAcks requestRequiredAcks) {
        // TODO check if still valid
        return new ProducerConfigsBuilder(this, "request.required.acks", requestRequiredAcks.getFlag());
    }

    public enum ProducerType {
        sync, async;
    }

    public ProducerConfigsBuilder producerType(ProducerType producerType) {
        // TODO check if still valid
        return new ProducerConfigsBuilder(this, "producer.type", producerType.toString());
    }

    public ProducerConfigsBuilder valueSerializer(Class serializerClass) {
        return new ProducerConfigsBuilder(this, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass.getCanonicalName());
    }

    public ProducerConfigsBuilder keySerializer(Class serializerClass) {
        return new ProducerConfigsBuilder(this, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClass.getCanonicalName());
    }

    public ProducerConfigsBuilder batchSize(int batchSize) {
        return new ProducerConfigsBuilder(this, ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(batchSize));
    }

    public Map<String, Object> build() {
        return producerConfigs;
    }
}
