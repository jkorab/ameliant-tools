package com.ameliant.tools.kafkaperf.config;

import java.util.Map;

/**
 * @author jkorab
 */
public class ProducerDefinition {

    /**
     * A map of Kafka config properties.
     */
    private Map<String, Object> configs;

    private boolean sendBlocking = false;
    private long messagesToSend = 10000;
    private String topic;
    private int messageSize = 1024;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isSendBlocking() {
        return sendBlocking;
    }

    public void setSendBlocking(boolean sendBlocking) {
        this.sendBlocking = sendBlocking;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, Object> configs) {
        this.configs = configs;
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
}
