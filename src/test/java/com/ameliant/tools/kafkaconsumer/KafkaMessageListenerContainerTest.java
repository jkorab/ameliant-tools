package com.ameliant.tools.kafkaconsumer;

import com.ameliant.tools.kafkaperf.config.ConsumerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ProducerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import com.ameliant.tools.kafkaperf.drivers.ProducerDriver;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import com.ameliant.tools.kafkaperf.resources.kafka.EmbeddedKafkaBroker;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static java.lang.String.copyValueOf;
import static java.lang.String.format;
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
            //.logFlushIntervalMessages(1)
            .build();

    @Test
    public void testReceive_errorHandling() throws Exception {
        int messagesToSend = 1000;
        preloadTopic(TOPIC, messagesToSend);

        Properties configs = props(getConsumerConfigs());
        AtomicInteger messagesReceived = new AtomicInteger();
        AtomicInteger exceptionsHandled = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(messagesToSend);

        // TODO clean up API
        BiConsumer<byte[], byte[]> messageListener = (byte[] key, byte[] value) -> {
            try {
                if (messagesReceived.incrementAndGet() == 500) {
                    throw new IllegalArgumentException("Boom!");
                }
            } finally {
                latch.countDown();
            }
        };

        try (KafkaMessageListenerContainer<byte[], byte[]> container = new KafkaMessageListenerContainer<>(
                configs,
                new MemoryOffsetStore(),
                TOPIC,
                messageListener)) {
            container.setExceptionHandler((tuple, exception) -> {
                exceptionsHandled.incrementAndGet();
                assertTrue(exception instanceof IllegalArgumentException);
            });
            container.start();
            if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("Timeout expired waiting on latch");
            }

            assertEquals(messagesToSend, messagesReceived.get());
            assertEquals(1, exceptionsHandled.get());
        }

    }

    // TODO add test for resumption - shutdown and restart after 500

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