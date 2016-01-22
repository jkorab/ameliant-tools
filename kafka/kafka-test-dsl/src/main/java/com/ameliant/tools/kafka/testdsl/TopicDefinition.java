package com.ameliant.tools.kafka.testdsl;

import java.util.Properties;

/**
 * @author jkorab
 */
class TopicDefinition {
    private String name;
    private int partitions = 1;
    private int replicationFactor = 1;
    private Properties properties = new Properties();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Properties getProperties() {
        return properties;
    }

}
