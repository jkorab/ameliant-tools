package com.ameliant.tools.kafkaperf.resources;

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
    public EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker.Builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .build();

    @Test
    public void testLifecycle() {
        Assert.assertTrue(true);
    }
}
