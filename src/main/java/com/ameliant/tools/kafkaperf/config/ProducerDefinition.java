package com.ameliant.tools.kafkaperf.config;

import org.apache.commons.lang.Validate;

/**
 * @author jkorab
 */
public class ProducerDefinition extends ConfigurableWithParent {

    private long messagesToSend = 10000;
    private int messageSize = 1024;
    private boolean sendBlocking = false;
    private int uniqueKeyCount = 1; // how many unique keys should be used for partitioning
    private KeyAllocationStrategyDefinition keyAllocationStrategyDefinition =
            new KeyAllocationStrategyDefinition(KeyAllocationType.fair, 1);

    @Override
    public String toString() {
        String mergedConfig = getKafkaConfig().entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .reduce("", (joined, configEntry) ->
                        (joined.equals("")) ? configEntry : joined + ", " + configEntry);

        return "ProducerDefinition{" +
                "topic='" + getTopic() + '\'' +
                ", messagesToSend=" + messagesToSend +
                ", messageSize=" + messageSize +
                ", sendBlocking=" + sendBlocking +
                ", mergedConfig={" + mergedConfig + "}" +
                '}';
    }

    public boolean isSendBlocking() {
        return sendBlocking;
    }

    public void setSendBlocking(boolean sendBlocking) {
        this.sendBlocking = sendBlocking;
    }

    public long getMessagesToSend() {
        return messagesToSend;
    }

    public void setMessagesToSend(long messagesToSend) {
        this.messagesToSend = messagesToSend;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public int getUniqueKeyCount() {
        return uniqueKeyCount;
    }

    public void setUniqueKeyCount(int uniqueKeyCount) {
        Validate.isTrue(uniqueKeyCount > 0, "uniqueKeyCount must be greater than 0");
        this.uniqueKeyCount = uniqueKeyCount;
    }

    public KeyAllocationStrategyDefinition getKeyAllocationStrategy() {
        return keyAllocationStrategyDefinition;
    }

    public void setKeyAllocationStrategy(KeyAllocationStrategyDefinition keyAllocationStrategyDefinition) {
        this.keyAllocationStrategyDefinition = keyAllocationStrategyDefinition;
    }
}
