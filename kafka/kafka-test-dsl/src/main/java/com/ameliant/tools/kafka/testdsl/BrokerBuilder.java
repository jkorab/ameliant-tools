package com.ameliant.tools.kafka.testdsl;

import kafka.server.KafkaConfig;
import org.apache.commons.lang.Validate;

import java.util.Properties;

/**
 * @author jkorab
 */
public class BrokerBuilder {

    final BrokerDefinition brokerDefinition;

    BrokerBuilder() {
        this.brokerDefinition = new BrokerDefinition();
    }

    public BrokerBuilder brokerId(int brokerId) {
        brokerDefinition.setBrokerId(brokerId);
        return this;
    }

    public BrokerBuilder hostname(String hostname) {
        brokerDefinition.setHostname(hostname);
        return this;
    }

    public BrokerBuilder port(int port) {
        brokerDefinition.setPort(port);
        return this;
    }

    public BrokerBuilder logFlushIntervalMessages(long logFlushIntervalMessages) {
        brokerDefinition.setLogFlushIntervalMessages(logFlushIntervalMessages);
        return this;
    }

    public BrokerBuilder zookeeperConnect(String zookeeperConnect) {
        brokerDefinition.setZookeeperConnect(zookeeperConnect);
        return this;
    }

    public BrokerBuilder numPartitions(int numPartitions) {
        Validate.isTrue(numPartitions > 0, "numPartitions must be greater than 0");
        brokerDefinition.setNumPartitions(numPartitions);
        return this;
    }

    public TopicBuilder addTopic(String name) {
        return new TopicBuilder(this, name);
    }

    public EmbeddedKafkaBroker build() {
        Properties props = brokerDefinition.getProperties();
        props.setProperty(KafkaConfig.HostNameProp(), brokerDefinition.getHostname());
        props.setProperty(KafkaConfig.PortProp(), Integer.toString(brokerDefinition.getPort()));
        props.setProperty(KafkaConfig.BrokerIdProp(), Integer.toString(brokerDefinition.getBrokerId()));
        props.setProperty(KafkaConfig.ZkConnectProp(), brokerDefinition.getZookeeperConnect());
        props.setProperty(KafkaConfig.NumPartitionsProp(), Integer.toString(brokerDefinition.getNumPartitions()));

        Long logFlushIntervalMessages = brokerDefinition.getLogFlushIntervalMessages();
        if (logFlushIntervalMessages != null) {
            props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), logFlushIntervalMessages.toString());
        }

        return new EmbeddedKafkaBroker(brokerDefinition);
    }

}
