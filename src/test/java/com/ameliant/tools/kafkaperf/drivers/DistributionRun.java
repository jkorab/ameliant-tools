package com.ameliant.tools.kafkaperf.drivers;

/**
 * @author jkorab
 */
public class DistributionRun {
    private final String topic;
    private final String groupId;
    private final int numProducers;
    private final int numConsumers;
    private int expectedConsumersWithoutMessages = 0;
    private int uniqueKeys = 2;

    public DistributionRun(String topic, int numProducers, int numConsumers) {
        this.topic = topic;
        this.groupId = "GROUP_ID";
        this.numProducers = numProducers;
        this.numConsumers = numConsumers;
    }

    public DistributionRun uniqueKeys(int uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
        return this;
    }

    public DistributionRun expectedConsumersWithoutMessages(int expectedConsumersWithoutMessages) {
        this.expectedConsumersWithoutMessages = expectedConsumersWithoutMessages;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getNumProducers() {
        return numProducers;
    }

    public int getNumConsumers() {
        return numConsumers;
    }

    public int getExpectedConsumersWithoutMessages() {
        return expectedConsumersWithoutMessages;
    }

    public int getMessagesToSend() {
        return 10 * numConsumers * numProducers;
    }

    public int getUniqueKeys() {
        return uniqueKeys;
    }
}
