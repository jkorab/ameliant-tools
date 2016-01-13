package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import com.ameliant.tools.kafkaperf.resources.kafka.EmbeddedKafkaBroker;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author jkorab
 */
public class ProducerPartitioningTest {

    public static final String FOO = "foo";
    public static final String BAR = "bar";
    public static final String BAZ = "baz";

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            .addTopic(FOO)
                .partitions(3)
            .end()
            .addTopic(BAR)
                .partitions(1)
            .end()
            .addTopic(BAZ)
                .partitions(2)
            .end()
            .build();

    @Test
    public void testSend_roundRobin() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 1;

        DistributionValidator validator = new DistributionValidator(broker);
        validator.validateRoundRobinDistribution(new DistributionRun(FOO, numProducers, numConsumers));
    }

    @Test
    public void testSend_roundRobin_multiProducer() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 2;

        DistributionValidator validator = new DistributionValidator(broker);
        validator.validateRoundRobinDistribution(new DistributionRun(FOO, numProducers, numConsumers));
    }

    @Test
    public void testSend_roundRobin_multiTopic() throws InterruptedException {
        DistributionValidator validator = new DistributionValidator(broker);
        validator.validateRoundRobinDistribution(
                new DistributionRun(FOO, 1, 3)
                        .uniqueKeys(2)
                        .expectedConsumersWithoutMessages(0),
                new DistributionRun(BAR, 1, 1)
                        .uniqueKeys(2)
                        .expectedConsumersWithoutMessages(0)
        );
    }

    @Test
    public void testSend_sticky() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 1;

        DistributionValidator validator = new DistributionValidator(broker);
        validator.validateStickyDistribution(new DistributionRun(FOO, numProducers, numConsumers)
                .expectedConsumersWithoutMessages(1));
    }

    @Test
    public void testSend_sticky_multiProducer() throws InterruptedException {
        int numConsumers = 3;
        int numProducers = 2;

        DistributionValidator validator = new DistributionValidator(broker);
        // two consumers should receive half of the messages each
        validator.validateStickyDistribution(new DistributionRun(FOO, numProducers, numConsumers)
                .expectedConsumersWithoutMessages(1));
    }

    @Test
    public void testSend_sticky_multiTopic() throws InterruptedException {
        DistributionValidator validator = new DistributionValidator(broker);
        validator.validateStickyDistribution(
                new DistributionRun(FOO, 1, 3)
                        .uniqueKeys(2)
                        .expectedConsumersWithoutMessages(1), // evenly partition - one consumer gets nothing (2 keys)
                new DistributionRun(BAR, 1, 2)
                        .uniqueKeys(2)
                        .expectedConsumersWithoutMessages(1), // one consumer blocked
                new DistributionRun(BAZ, 1, 2)
                        .uniqueKeys(2).expectedConsumersWithoutMessages(0) // evenly partitioned
        );
    }

}
