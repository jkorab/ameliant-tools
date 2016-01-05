package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ConsumerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ConsumerDefinition;
import com.ameliant.tools.kafkaperf.config.ProducerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import com.ameliant.tools.kafkaperf.resources.EmbeddedKafkaBroker;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author jkorab
 */
public class ProducerPartitioningTest {

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker.Builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .build();

        @Test
    public void testSend_partitions() throws InterruptedException {
        Map<String, Object> producerConfigs = getProducerConfigs();

        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfig(producerConfigs);
        producerDefinition.setTopic("foo");
        producerDefinition.setMessageSize(1024);
        producerDefinition.setMessagesToSend(100);
        producerDefinition.setSendBlocking(true);
        producerDefinition.setUniqueKeyCount(2);

        int numWorkers = 3;
        CountDownLatch latch = new CountDownLatch(numWorkers);

        ProducerDriver producer = new ProducerDriver(producerDefinition, latch);
        ConsumerDriver consumer1 = createConsumerDriver(latch, "foo", "bar", 50);
        ConsumerDriver consumer2 = createConsumerDriver(latch, "foo", "bar", 50);

        ExecutorService executorService = Executors.newFixedThreadPool(numWorkers);
        try {
            // fill up the topic
            executorService.submit(producer);
            executorService.submit(consumer1);
            executorService.submit(consumer2);

            if (!latch.await(15, TimeUnit.SECONDS)) {
                String consumerResults = Arrays.asList(consumer1, consumer2).stream()
                        .map(consumer -> consumer.getMessagesExpected() + ":" + consumer.getMessagesReceived())
                        .reduce("", (results, expectedVsActual) -> {
                            String retval = results;
                            retval += (results == "") ? expectedVsActual : ", " + expectedVsActual;
                            return retval;
                        });
                throw new RuntimeException("Consumers did not receive expected message counts: [" + consumerResults + "]");
            }
        } finally {
            executorService.shutdownNow();
        }

    }


    private Map<String, Object> getProducerConfigs() {
        return new ProducerConfigsBuilder()
                    .bootstrapServers("127.0.0.1:" + broker.getPort())
                    .requestRequiredAcks(ProducerConfigsBuilder.RequestRequiredAcks.ackFromLeader)
                    .producerType(ProducerConfigsBuilder.ProducerType.sync)
                    .keySerializer(ByteArraySerializer.class)
                    .valueSerializer(ByteArraySerializer.class)
                    .batchSize(0)
                    .build();
    }
    private ConsumerDriver createConsumerDriver(CountDownLatch latch, String topic, String groupId, int messageCount) {
        Map<String, Object> configs = new ConsumerConfigsBuilder()
                .groupId(groupId)
                .bootstrapServers("127.0.0.1:" + broker.getPort())
                .enableAutoCommit(true)
                .autoCommitIntervalMs(1000)
                .sessionTimeoutMs(30000)
                .keyDeserializer(ByteArrayDeserializer.class)
                .valueDeserializer(ByteArrayDeserializer.class)
                .autoOffsetReset(ConsumerConfigsBuilder.OffsetReset.earliest)
                .build();

        ConsumerDefinition consumerDefinition = new ConsumerDefinition();
        consumerDefinition.setConfig(configs);
        consumerDefinition.setTopic(topic);
        consumerDefinition.setMessagesToReceive(messageCount);
        return new ConsumerDriver(consumerDefinition, latch);
    }

}
