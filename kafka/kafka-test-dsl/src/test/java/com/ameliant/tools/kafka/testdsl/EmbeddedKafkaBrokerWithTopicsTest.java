package com.ameliant.tools.kafka.testdsl;

import com.ameliant.tools.zookeeper.testdsl.EmbeddedZooKeeper;
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
            .topic("goat")
                .partitions(1)
                .replicationFactor(1)
                .property("flush.messages", "1")
            .end()
            .topic("cheese")
                .partitions(3)
            .end()
            .build();

    @Test
    public void testLifecycle() {
        Assert.assertTrue(true);
    }
}
