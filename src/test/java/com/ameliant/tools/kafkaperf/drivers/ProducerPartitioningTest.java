package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.*;
import com.ameliant.tools.kafkaperf.resources.EmbeddedKafkaBroker;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import com.ameliant.tools.kafkaperf.coordination.AwaitsStartup;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author jkorab
 */
public class ProducerPartitioningTest {

    public static final String TOPIC = "foo";
    public static final String GROUP_ID = "bar";
    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker.Builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .numPartitions(3)
            .build();

    @Test
    public void testSend_roundRobin() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 1;

        verifyRoundRobinDistribution(numProducers, numConsumers);
    }

    @Test
    public void testSend_roundRobin_multiProducer() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 2;

        verifyRoundRobinDistribution(numProducers, numConsumers);
    }

    private void verifyRoundRobinDistribution(int numProducers, int numConsumers)
            throws InterruptedException {
        int numMessages = 10 * numConsumers * numProducers;

        CountDownLatch startUpLatch = new CountDownLatch(numConsumers);
        CountDownLatch shutDownLatch = new CountDownLatch(numConsumers + numProducers);

        List<ProducerDriver> producerDrivers =
                createProducerDrivers(shutDownLatch, numProducers, numMessages / numProducers, PartitioningStrategy.roundRobin);

        // a round-robin strategy will result in 3 consumers each receiving the same number of messages
        // despite there only being 2 keys (naive distribution)
        List<ConsumerDriver> consumerDrivers =
                createConsumerDrivers(shutDownLatch, numConsumers, numMessages / numConsumers);

        ExecutorService executorService = Executors.newFixedThreadPool(producerDrivers.size() + consumerDrivers.size());
        try {
            // fill up the topic
            producerDrivers.stream()
                    .forEach(producer -> executorService.submit(new AwaitsStartup(producer, startUpLatch)));

            consumerDrivers.stream()
                    .forEach(consumer -> {
                        consumer.setConsumerRebalanceListener(waitForPartitionAssignment(startUpLatch));
                        executorService.submit(consumer);
                    });

            if (!shutDownLatch.await(10, TimeUnit.SECONDS)) {
                String consumerResults = getConsumerResults(consumerDrivers);
                fail("Consumers did not receive expected message counts: [" + consumerResults + "]");
            }

            int consumersWithoutMessages = getConsumersWithoutMessages(consumerDrivers);
            assertThat(consumersWithoutMessages, equalTo(0));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testSend_sticky() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 1;

        verifyStickyDistribution(numProducers, numConsumers);
    }

    @Test
    public void testSend_sticky_multiProducer() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 2;

        // a consistent key hashing function will mean that regardless of the number of partitioners used,
        // the messages will end up in the right partition
        verifyStickyDistribution(numProducers, numConsumers);
    }

    private void verifyStickyDistribution(int numProducers, int numConsumers) throws InterruptedException {
        int numMessages = 10 * numConsumers * numProducers;

        CountDownLatch startUpLatch = new CountDownLatch(numConsumers);
        CountDownLatch shutDownLatch = new CountDownLatch(numConsumers + numProducers);

        List<ProducerDriver> producerDrivers =
                createProducerDrivers(shutDownLatch, numProducers, numMessages / numProducers, PartitioningStrategy.sticky);

        // a sticky strategy will result in 2/3 consumers receiving half of the numMessages
        // as only 2 keys are sent out
        int oneHalfMessageCount = numMessages / 2;
        List<ConsumerDriver> consumerDrivers = createConsumerDrivers(shutDownLatch, numConsumers, oneHalfMessageCount);

        ExecutorService executorService = Executors.newFixedThreadPool(producerDrivers.size() + consumerDrivers.size());
        try {
            // fill up the topic
            producerDrivers.stream()
                    .forEach(producer -> executorService.submit(new AwaitsStartup(producer, startUpLatch)));

            consumerDrivers.stream()
                    .forEach(consumer -> {
                        consumer.setConsumerRebalanceListener(waitForPartitionAssignment(startUpLatch));
                        executorService.submit(consumer);
                    });

            if (!shutDownLatch.await(10, TimeUnit.SECONDS)) {
                // one of the consumers will expire
                long messagesReceived = getMessagesReceived(consumerDrivers);
                assertThat((int) messagesReceived, equalTo(numMessages));

                int consumersWithoutMessages = getConsumersWithoutMessages(consumerDrivers);
                assertThat(consumersWithoutMessages, equalTo(1));
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private List<ProducerDriver> createProducerDrivers(CountDownLatch latch, int numDrivers, int messagesToSend, PartitioningStrategy partitioningStrategy) {
        List<ProducerDriver> producerDrivers = new ArrayList<>(numDrivers);
        for (int i = 0; i < numDrivers; i++) {
            ProducerDefinition producerDefinition = getProducerDefinition(messagesToSend);
            producerDefinition.setPartitioningStrategy(partitioningStrategy);
            producerDrivers.add(new ProducerDriver(producerDefinition, latch));
        }
        return producerDrivers;
    }

    private ProducerDefinition getProducerDefinition(int numMessages) {
        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfig(getProducerConfigs());
        producerDefinition.setTopic(TOPIC);
        producerDefinition.setMessagesToSend(numMessages);
        producerDefinition.setSendBlocking(true);
        {
            KeyAllocationStrategyDefinition strategyDefinition =
                    new KeyAllocationStrategyDefinition(KeyAllocationType.fair, 2);
            producerDefinition.setKeyAllocationStrategy(strategyDefinition);
        }
        return producerDefinition;
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

    private ConsumerRebalanceListener waitForPartitionAssignment(CountDownLatch latch) {
        return new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                latch.countDown();
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

        };
    }

    private List<ConsumerDriver> createConsumerDrivers(CountDownLatch latch, int numDrivers, int expectedMessages) {
        List<ConsumerDriver> consumerDrivers = new ArrayList<>(numDrivers);
        for (int i = 0; i < numDrivers; i++) {
            consumerDrivers.add(createConsumerDriver(latch, TOPIC, GROUP_ID, expectedMessages));
        }
        return consumerDrivers;
    }

    private ConsumerDriver createConsumerDriver(CountDownLatch latch, String topic, String groupId, int messageCount) {
        Map<String, Object> configs = getConsumerConfigs(groupId);

        ConsumerDefinition consumerDefinition = new ConsumerDefinition();
        consumerDefinition.setConfig(configs);
        consumerDefinition.setTopic(topic);
        consumerDefinition.setMessagesToReceive(messageCount);
        return new ConsumerDriver(consumerDefinition, latch);
    }

    private Map<String, Object> getConsumerConfigs(String groupId) {
        return new ConsumerConfigsBuilder()
                    .groupId(groupId)
                    .bootstrapServers("127.0.0.1:" + broker.getPort())
                    .enableAutoCommit(true)
                    .autoCommitIntervalMs(1000)
                    .sessionTimeoutMs(30000)
                    .keyDeserializer(ByteArrayDeserializer.class)
                    .valueDeserializer(ByteArrayDeserializer.class)
                    .autoOffsetReset(ConsumerConfigsBuilder.OffsetReset.earliest)
                    .build();
    }

    private String getConsumerResults(List<ConsumerDriver> consumerDrivers) {
        return consumerDrivers.stream()
                .map(consumer -> consumer.getMessagesExpected() + ":" + consumer.getMessagesReceived())
                .reduce("", (results, expectedVsActual) ->
                    (results == "") ? expectedVsActual : results + ", " + expectedVsActual
                );
    }

    private int getConsumersWithoutMessages(List<ConsumerDriver> consumerDrivers) {
        return (int) consumerDrivers.stream()
                .filter(consumer -> consumer.getMessagesReceived() == 0)
                .count();
    }

    private long getMessagesReceived(List<ConsumerDriver> consumerDrivers) {
        return consumerDrivers.stream()
                .map(consumer -> consumer.getMessagesReceived())
                .reduce(0l, (total, received) -> total + received);
    }

}
