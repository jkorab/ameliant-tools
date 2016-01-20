package com.ameliant.tools.kafka.performance.resources.kafka;

import com.ameliant.tools.kafka.performance.resources.EmbeddedZooKeeper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author jkorab
 */
public class EmbeddedKafkaBrokerTest {

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .build();

    @Test
    public void testLifecycle() {
        Assert.assertTrue(true);
    }
}
