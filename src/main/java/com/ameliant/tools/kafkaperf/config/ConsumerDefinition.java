package com.ameliant.tools.kafkaperf.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ConsumerDefinition {

    /**
     * A map of Kafka config properties.
     */
    private Map<String, Object> configs = new HashMap<>();

    private String topic;
    private long messagesToReceive = 10000;
    private String consumerGroupId;
    private long pollTimeout = 1000;
    private int reportReceivedEvery = 1000;

    private long testRunTimeout = Long.MAX_VALUE;

    // Required for Jackson
    public ConsumerDefinition() {}

    // Copy constructor
    private ConsumerDefinition(Map<String, Object> configs, String consumerGroupId,
                              long messagesToReceive, long pollTimeout,
                              int reportReceivedEvery, long testRunTimeout, String topic) {
        this.configs = configs;
        this.consumerGroupId = consumerGroupId;
        this.messagesToReceive = messagesToReceive;
        this.pollTimeout = pollTimeout;
        this.reportReceivedEvery = reportReceivedEvery;
        this.testRunTimeout = testRunTimeout;
        this.topic = topic;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, Object> configs) {
        this.configs = configs;
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

    public ConsumerDefinition withParentConfigs(Map<String, Object> parentConfigs) {
        Map<String, Object> mergedConfig = ConfigMerger.merge(parentConfigs, configs);

        return new ConsumerDefinition(mergedConfig, consumerGroupId,
                messagesToReceive, pollTimeout,
                reportReceivedEvery, testRunTimeout, topic);
    }

}
