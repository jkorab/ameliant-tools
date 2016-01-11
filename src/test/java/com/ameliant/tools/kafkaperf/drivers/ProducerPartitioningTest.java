package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.*;
import com.ameliant.tools.kafkaperf.resources.EmbeddedKafkaBroker;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import com.ameliant.tools.kafkaperf.coordination.AwaitsStartup;
import com.ameliant.tools.kafkaperf.coordination.SignalsStartup;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
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
        int numMessages = 90;
        ProducerDefinition producerDefinition = getProducerDefinition(numMessages);
        producerDefinition.setPartitioningStrategy(PartitioningStrategy.roundRobin);

        int numConsumers = 3;
        CountDownLatch startUpLatch = new CountDownLatch(numConsumers);
        CountDownLatch latch = new CountDownLatch(numConsumers + 1);

        ProducerDriver producer = new ProducerDriver(producerDefinition, latch);
        // a round-robin strategy will result in 3 consumers each receiving the same number of messages
        // despite there only being 2 keys (naive distribution)
        int oneThirdMessageCount = numMessages / 3;
        List<ConsumerDriver> consumerDrivers = createConsumerDrivers(latch, numConsumers, oneThirdMessageCount);

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers + 1);
        try {
            // fill up the topic
            executorService.submit(new AwaitsStartup(producer, startUpLatch));
            consumerDrivers.stream()
                    .forEach(consumer -> executorService.submit(new SignalsStartup(consumer, startUpLatch)));

            if (!latch.await(10, TimeUnit.SECONDS)) {
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
        int numMessages = 90;
        ProducerDefinition producerDefinition = getProducerDefinition(numMessages);
        producerDefinition.setPartitioningStrategy(PartitioningStrategy.sticky);

        int numConsumers = 3;
        CountDownLatch startUpLatch = new CountDownLatch(numConsumers);
        CountDownLatch latch = new CountDownLatch(numConsumers + 1);

        ProducerDriver producer = new ProducerDriver(producerDefinition, latch);
        // a sticky strategy will result in 2/3 consumers receiving half of the numMessages
        // as only 2 keys are sent out
        int oneHalfMessageCount = numMessages / 2;
        List<ConsumerDriver> consumerDrivers = createConsumerDrivers(latch, numConsumers, oneHalfMessageCount);

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers + 1);
        try {
            // fill up the topic
            executorService.submit(new AwaitsStartup(producer, startUpLatch));
            consumerDrivers.stream()
                    .forEach(consumer -> executorService.submit(new SignalsStartup(consumer, startUpLatch)));

            if (!latch.await(10, TimeUnit.SECONDS)) {
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

    private List<ConsumerDriver> createConsumerDrivers(CountDownLatch latch, int numDrivers, int expectedMessages) {
        List<ConsumerDriver> consumerDrivers = new ArrayList<>(numDrivers);
        for (int i = 0; i < numDrivers; i++) {
            consumerDrivers.add(createConsumerDriver(latch, TOPIC, GROUP_ID, expectedMessages));
        }
        return consumerDrivers;
    }

    private String getConsumerResults(List<ConsumerDriver> consumerDrivers) {
        return consumerDrivers.stream()
                .map(consumer -> consumer.getMessagesExpected() + ":" + consumer.getMessagesReceived())
                .reduce("", (results, expectedVsActual) -> {
                    String retval = results;
                    retval += (results == "") ? expectedVsActual : ", " + expectedVsActual;
                    return retval;
                });
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
