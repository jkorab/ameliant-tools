package com.ameliant.tools.kafka.performance.resources;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author jkorab
 */
public class EmbeddedZookeeperTest {

    @Rule
    public EmbeddedZooKeeper zookeeper = new EmbeddedZooKeeper();

    @Test
    public void testPortAssignment() {
        assertThat(zookeeper.getPort(), greaterThan(0));
    }

}