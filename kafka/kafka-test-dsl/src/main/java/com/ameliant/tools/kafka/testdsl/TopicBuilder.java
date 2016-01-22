package com.ameliant.tools.kafka.testdsl;

/**
 * @author jkorab
 */
public class TopicBuilder {
    private BrokerBuilder parent;
    private TopicDefinition topicDefinition;

    TopicBuilder(BrokerBuilder parent, String name) {
        this.parent = parent;
        this.topicDefinition = new TopicDefinition();
        topicDefinition.setName(name);
    }

    public TopicBuilder replicationFactor(int replicationFactor) {
        topicDefinition.setReplicationFactor(replicationFactor);
        return this;
    }

    public TopicBuilder partitions(int partitions) {
        topicDefinition.setPartitions(partitions);
        return this;
    }

    public TopicBuilder property(Object key, Object value) {
        topicDefinition.getProperties().put(key, value);
        return this;
    }

    public BrokerBuilder end() {
        parent.brokerDefinition.getTopicDefinitions().add(topicDefinition);
        return parent;
    }
}
