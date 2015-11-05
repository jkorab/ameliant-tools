package com.ameliant.tools.kafkaperf.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building up Kafka producer config maps.
 * @author jkorab
 */
public class ProducerConfigsBuilder {

    public final static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public final static String REQUEST_REQUIRED_ACKS = "request.required.acks";
    public final static String PRODUCER_TYPE = "producer.type";
    public final static String VALUE_SERIALIZER = "value.serializer";
    public final static String KEY_SERIALIZER = "key.serializer";
    public final static String BATCH_SIZE = "batch.size";


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
        return new ProducerConfigsBuilder(this, BOOTSTRAP_SERVERS, bootstrapServers);
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
        return new ProducerConfigsBuilder(this, REQUEST_REQUIRED_ACKS, requestRequiredAcks.getFlag());
    }

    public enum ProducerType {
        sync, async;
    }

    public ProducerConfigsBuilder producerType(ProducerType producerType) {
        return new ProducerConfigsBuilder(this, PRODUCER_TYPE, producerType.toString());
    }

    public ProducerConfigsBuilder valueSerializerClass(Class serializerClass) {
        return new ProducerConfigsBuilder(this, VALUE_SERIALIZER, serializerClass.getCanonicalName());
    }

    public ProducerConfigsBuilder keySerializerClass(Class serializerClass) {
        return new ProducerConfigsBuilder(this, KEY_SERIALIZER, serializerClass.getCanonicalName());
    }

    public ProducerConfigsBuilder batchSize(int batchSize) {
        return new ProducerConfigsBuilder(this, BATCH_SIZE, Integer.toString(batchSize));
    }

    public Map<String, Object> build() {
        return producerConfigs;
    }
}
