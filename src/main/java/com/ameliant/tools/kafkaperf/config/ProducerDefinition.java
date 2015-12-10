package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ProducerDefinition {

    /**
     * A map of Kafka config properties.
     */
    private Map<String, Object> config = new HashMap<>();

    private boolean sendBlocking = false;
    private long messagesToSend = 10000;
    private String topic;
    private int messageSize = 1024;

    @JsonBackReference
    private ProducersDefinition producersDefinition;

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

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

    public ProducersDefinition getProducersDefinition() {
        return producersDefinition;
    }

    public void setProducersDefinition(ProducersDefinition producersDefinition) {
        this.producersDefinition = producersDefinition;
    }

    @JsonIgnore
    public Map<String, Object> getMergedConfig() {
        return ConfigMerger.merge(producersDefinition.getConfig(), config);
    }
}
