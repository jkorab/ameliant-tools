package com.ameliant.tools.kafka.performance.drivers;

import com.ameliant.tools.kafka.performance.config.*;
import com.ameliant.tools.kafka.performance.coordination.AwaitsStartup;
import com.ameliant.tools.kafka.performance.resources.kafka.EmbeddedKafkaBroker;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author jkorab
 */
public class DistributionValidator {

    private EmbeddedKafkaBroker broker;
    DistributionValidator(EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    void validateRoundRobinDistribution(DistributionRun... runs)
            throws InterruptedException {
        validateDistribution(PartitioningStrategy.roundRobin, runs);
    }

    void validateStickyDistribution(DistributionRun... runs)
            throws InterruptedException {
        validateDistribution(PartitioningStrategy.sticky, runs);
    }

    void validateDistribution(PartitioningStrategy partitioningStrategy,
                                      DistributionRun[] runs)
            throws InterruptedException {

        final Map<String, Integer> topicMessagesToSend = getTopicMessagesToSend(runs);
        final Map<String, DistributionRun> topicRuns = new HashMap<>();
        for (DistributionRun run : runs) {
            topicRuns.put(run.getTopic(), run);
        }

        int totalConsumers = Arrays.stream(runs)
                .map(DistributionRun::getNumConsumers)
                .reduce(0, (i, total) -> total + i);

        int consumersThatWillTimeout = Arrays.stream(runs)
                .map(DistributionRun::getExpectedConsumersWithoutMessages)
                .reduce(0, (i, total) -> total + i);

        int totalProducers = Arrays.stream(runs)
                .map(DistributionRun::getNumProducers)
                .reduce(0, (i, total) -> total + i);

        CountDownLatch startUpLatch = new CountDownLatch(totalConsumers);
        CountDownLatch shutDownLatch = new CountDownLatch(totalConsumers - consumersThatWillTimeout + totalProducers);

        ExecutorService executorService = Executors.newFixedThreadPool(totalConsumers + totalProducers);

        try {
            Map<String, List<ConsumerDriver>> topicConsumerDrivers = new HashMap<>();

            for (DistributionRun run : runs) {
                int numMessages = run.getMessagesToSend();
                List<ProducerDriver> producerDrivers =
                        createProducerDrivers(shutDownLatch,
                                run.getTopic(),
                                run.getNumProducers(),
                                numMessages / run.getNumProducers(), // evenly share out the sends
                                run.getUniqueKeys(),
                                partitioningStrategy);

                List<ConsumerDriver> consumerDrivers =
                        createConsumerDrivers(shutDownLatch,
                                run.getTopic(),
                                run.getGroupId(),
                                run.getNumConsumers(),
                                numMessages / (run.getNumConsumers() - run.getExpectedConsumersWithoutMessages())); // evenly share out the receives
                topicConsumerDrivers.put(run.getTopic(), consumerDrivers);

                // fill up the topic
                producerDrivers.stream()
                        .forEach(producer -> executorService.submit(new AwaitsStartup(producer, startUpLatch)));

                consumerDrivers.stream()
                        .forEach(consumer -> {
                            consumer.setConsumerRebalanceListener(waitForPartitionAssignment(startUpLatch));
                            executorService.submit(consumer);
                        });
            }

            // some consumers will flick the latch, others will continue polling

            if (!shutDownLatch.await(30, TimeUnit.SECONDS)) {
                topicConsumerDrivers.forEach((topic, consumerDrivers) -> {
                    consumerDrivers.forEach(ConsumerDriver::requestShutdown);

                    long messagesReceived = getMessagesReceived(consumerDrivers);
                    int messagesSent = topicMessagesToSend.get(topic);
                    if (messagesSent != messagesReceived) {
                        String consumerResults = getConsumerResults(consumerDrivers);
                        fail("Consumers for " + topic + " did not receive " + messagesSent
                                        + " messages, actual: " + messagesReceived + ". Details [" + consumerResults + "]");
                    }

                    int consumersWithoutMessages = getConsumersWithoutMessages(consumerDrivers);
                    DistributionRun run = topicRuns.get(topic);
                    assertEquals(run.getExpectedConsumersWithoutMessages(), consumersWithoutMessages);
                });
            }

        } finally {
            executorService.shutdownNow();
        }
    }

    private Map<String, Integer> getTopicMessagesToSend(DistributionRun[] runs) {
        final Map<String, Integer> topicMessagesToSend = new HashMap<>();
        for (DistributionRun run: runs) {
            topicMessagesToSend.put(run.getTopic(), run.getMessagesToSend());
        }
        return topicMessagesToSend;
    }

    private List<ProducerDriver> createProducerDrivers(CountDownLatch latch, String topic, int numDrivers, int messagesToSend, int uniqueKeys, PartitioningStrategy partitioningStrategy) {
        List<ProducerDriver> producerDrivers = new ArrayList<>(numDrivers);
        for (int i = 0; i < numDrivers; i++) {
            ProducerDefinition producerDefinition = getProducerDefinition(topic, messagesToSend, uniqueKeys);
            producerDefinition.setPartitioningStrategy(partitioningStrategy);
            producerDrivers.add(new ProducerDriver(producerDefinition, latch));
        }
        return producerDrivers;
    }

    private ProducerDefinition getProducerDefinition(String topic, int numMessages, int uniqueKeys) {
        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfig(getProducerConfigs());
        producerDefinition.setTopic(topic);
        producerDefinition.setMessagesToSend(numMessages);
        producerDefinition.setSendBlocking(true);
        {
            KeyAllocationStrategyDefinition strategyDefinition =
                    new KeyAllocationStrategyDefinition(KeyAllocationType.fair, uniqueKeys);
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

    private List<ConsumerDriver> createConsumerDrivers(CountDownLatch latch, String topic, String groupId, int numDrivers, int expectedMessages) {
        List<ConsumerDriver> consumerDrivers = new ArrayList<>(numDrivers);
        for (int i = 0; i < numDrivers; i++) {
            // each driver expects the same number of messages
            consumerDrivers.add(createConsumerDriver(latch, topic, groupId, expectedMessages));
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
                    (results.equals("")) ? expectedVsActual : results + ", " + expectedVsActual
                );
    }

    private int getConsumersWithoutMessages(List<ConsumerDriver> consumerDrivers) {
        return (int) consumerDrivers.stream()
                .filter(consumer -> consumer.getMessagesReceived() == 0)
                .count();
    }

    private long getMessagesReceived(List<ConsumerDriver> consumerDrivers) {
        return consumerDrivers.stream()
                .map(ConsumerDriver::getMessagesReceived)
                .reduce(0l, (total, received) -> total + received);
    }

}
