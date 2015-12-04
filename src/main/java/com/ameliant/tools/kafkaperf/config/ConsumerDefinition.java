package com.ameliant.tools.kafkaperf.config;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author jkorab
 */
public class ConsumerDefinition {

    private String topic;
    private long messagesToReceive = 10000;
    private String zookeeperConnect;
    private String consumerGroupId;
    private Map<String, Object> configs;
    private long pollTimeout = 30 * 1000;

    private long testRunTimeout = Long.MAX_VALUE;

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

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
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

}
