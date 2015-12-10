package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ConsumerDefinition {

    /**
     * A map of Kafka config properties.
     */
    private Map<String, Object> config = new HashMap<>();

    private String topic;
    private long messagesToReceive = 10000;
    private String consumerGroupId;
    private long pollTimeout = 1000;
    private int reportReceivedEvery = 1000;

    private long testRunTimeout = Long.MAX_VALUE;

    @JsonBackReference
    private ConsumersDefinition consumersDefinition;

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public long getMessagesToReceive() {
        return messagesToReceive;
    }

    public void setMessagesToReceive(long messagesToReceive) {
        this.messagesToReceive = messagesToReceive;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getTestRunTimeout() {
        return testRunTimeout;
    }

    public void setTestRunTimeout(long testRunTimeout) {
        this.testRunTimeout = testRunTimeout;
    }

    public int getReportReceivedEvery() {
        return reportReceivedEvery;
    }

    public void setReportReceivedEvery(int reportReceivedEvery) {
        this.reportReceivedEvery = reportReceivedEvery;
    }

    public ConsumersDefinition getConsumersDefinition() {
        return consumersDefinition;
    }

    public void setConsumersDefinition(ConsumersDefinition consumersDefinition) {
        this.consumersDefinition = consumersDefinition;
    }

    @JsonIgnore
    public Map<String, Object> getMergedConfig() {
        return ConfigMerger.merge(consumersDefinition.getMergedConfig(), config);
    }

}
