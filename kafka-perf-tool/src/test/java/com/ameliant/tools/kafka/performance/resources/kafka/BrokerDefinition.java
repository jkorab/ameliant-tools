package com.ameliant.tools.kafka.performance.resources.kafka;

import com.ameliant.tools.kafka.performance.util.AvailablePortFinder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author jkorab
 */
class BrokerDefinition {

    private int brokerId = 1;
    private String hostname = "localhost";
    private String zookeeperConnect;
    private int port = AvailablePortFinder.getNextAvailable();

    private int numPartitions = 1;
    private Long logFlushIntervalMessages;
    private Properties properties = new Properties();

    private List<TopicDefinition> topicDefinitions = new ArrayList<>();

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public List<TopicDefinition> getTopicDefinitions() {
        return topicDefinitions;
    }

    public void setTopicDefinitions(List<TopicDefinition> topicDefinitions) {
        this.topicDefinitions = topicDefinitions;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public Long getLogFlushIntervalMessages() {
        return logFlushIntervalMessages;
    }

    public void setLogFlushIntervalMessages(Long logFlushIntervalMessages) {
        this.logFlushIntervalMessages = logFlushIntervalMessages;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}

