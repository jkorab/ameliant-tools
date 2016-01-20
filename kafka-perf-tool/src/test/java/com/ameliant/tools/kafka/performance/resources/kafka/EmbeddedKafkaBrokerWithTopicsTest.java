package com.ameliant.tools.kafka.performance.resources.kafka;

import com.ameliant.tools.kafka.performance.resources.EmbeddedZooKeeper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author jkorab
 */
public class EmbeddedKafkaBrokerWithTopicsTest {

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .addTopic("goat")
                .partitions(1)
                .replicationFactor(1)
                .property("flush.messages", "1")
            .end()
            .addTopic("cheese")
                .partitions(3)
            .end()
            .build();

    @Test
    public void testLifecycle() {
        Assert.assertTrue(true);
    }
}
