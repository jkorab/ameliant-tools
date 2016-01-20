package com.ameliant.tools.kafka.listener;

import com.ameliant.tools.kafka.perftool.config.*;
import com.ameliant.tools.kafka.perftool.drivers.ProducerDriver;
import com.ameliant.tools.kafka.testdsl.config.ConsumerConfigsBuilder;
import com.ameliant.tools.kafka.testdsl.EmbeddedKafkaBroker;
import com.ameliant.tools.kafka.testdsl.config.ProducerConfigsBuilder;
import com.ameliant.tools.zookeeper.testdsl.EmbeddedZooKeeper;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author jkorab
 */
public class KafkaMessageListenerContainerTest {
    private final String TOPIC = "foo";

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .numPartitions(3)
            .build();

    @Test
    public void testReceive_errorHandling() throws Exception {
        int messagesToSend = 1000;
        preloadTopic(TOPIC, messagesToSend);

        Properties configs = props(getConsumerConfigs());
        AtomicInteger messagesReceived = new AtomicInteger();
        AtomicInteger exceptionsHandled = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(messagesToSend);

        MemoryOffsetStore offsetStore = new MemoryOffsetStore();

        KafkaMessageListenerContainer.Builder builder = new KafkaMessageListenerContainer.Builder<byte[], byte[]>()
                .kafkaConfig(configs)
                .offsetStore(offsetStore)
                .topic(TOPIC)
                .messageListener((key, value) -> {
                    try {
                        if (messagesReceived.incrementAndGet() == 500) {
                            throw new IllegalArgumentException("Boom!");
                        }
                    } finally {
                        latch.countDown();
                    }
                })
                .exceptionHandler((tuple, exception) -> {
                    exceptionsHandled.incrementAndGet();
                    assertTrue(exception instanceof IllegalArgumentException);
                });

        try (KafkaMessageListenerContainer<byte[], byte[]> container = builder.build()) {
            container.init();
            if (!latch.await(100, TimeUnit.SECONDS)) {
                fail("Timeout expired waiting on latch");
            }

            assertEquals(messagesToSend, messagesReceived.get());
            assertEquals(1, exceptionsHandled.get());
        }
    }

    @Test
    public void testReceive_shutdownResumption() throws Exception {
        int messagesToSend = 1000;
        preloadTopic(TOPIC, messagesToSend);

        Properties configs = props(getConsumerConfigs());
        AtomicInteger messagesReceived = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(messagesToSend);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        MemoryOffsetStore offsetStore = new MemoryOffsetStore(); // shared between container instances

        try (KafkaMessageListenerContainer<byte[], byte[]> container = new KafkaMessageListenerContainer.Builder<byte[], byte[]>()
                .kafkaConfig(configs)
                .offsetStore(offsetStore)
                .topic(TOPIC)
                .messageListener((key, value) -> {
                    try {
                        if (messagesReceived.incrementAndGet() == 500) {
                            shutdownLatch.countDown();
                        }
                    } finally {
                        latch.countDown();
                    }
                }).build()) {
            container.init();
            if (!shutdownLatch.await(10, TimeUnit.SECONDS)) {
                fail("Timeout expired waiting on shutdownLatch");
            }
        }
        try (KafkaMessageListenerContainer<byte[], byte[]> container = new KafkaMessageListenerContainer.Builder<byte[], byte[]>()
                .kafkaConfig(configs)
                .offsetStore(offsetStore)
                .topic(TOPIC)
                .messageListener((key, value) -> latch.countDown()).build()) {
            container.init();
            if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("Timeout expired waiting on latch");
            }
        }
    }

    // TODO test dropping of messages during repartitioning

    private Properties props(Map<String, Object> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }

    private Map<String, Object> getConsumerConfigs() {
        Map<String, Object> configs = new ConsumerConfigsBuilder()
                .groupId("bar")
                .bootstrapServers(broker.getConnectionString())
                .sessionTimeoutMs(30000)
                .keyDeserializer(ByteArrayDeserializer.class)
                .valueDeserializer(ByteArrayDeserializer.class)
                .build();
        return configs;
    }


    private void preloadTopic(String topic, int messagesToSend) {
        Map<String, Object> producerConfigs = getProducerConfigs();

        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfig(producerConfigs);
        producerDefinition.setTopic(topic);
        producerDefinition.setMessageSize(1024);
        producerDefinition.setMessagesToSend(messagesToSend);
        producerDefinition.setSendBlocking(true);
        producerDefinition.setPartitioningStrategy(PartitioningStrategy.roundRobin);

        ProducerDriver driver = new ProducerDriver(producerDefinition);
        driver.run();
    }

    private Map<String, Object> getProducerConfigs() {
        return new ProducerConfigsBuilder()
                .bootstrapServers(broker.getConnectionString())
                .requestRequiredAcks(ProducerConfigsBuilder.RequestRequiredAcks.ackFromLeader)
                .producerType(ProducerConfigsBuilder.ProducerType.sync)
                .keySerializer(ByteArraySerializer.class)
                .valueSerializer(ByteArraySerializer.class)
                .batchSize(0)
                .build();
    }


}