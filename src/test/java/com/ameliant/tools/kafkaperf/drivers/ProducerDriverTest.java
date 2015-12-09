package com.ameliant.tools.kafkaperf.drivers;

import static java.lang.String.format;
import com.ameliant.tools.kafkaperf.config.ProducerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import com.ameliant.tools.kafkaperf.resources.EmbeddedKafkaBroker;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

/**
 * @author jkorab
 */
public class ProducerDriverTest {

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker.Builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            //.logFlushIntervalMessages(1)
            .build();

    @Test
    public void testSend() {
        Map<String, Object> producerConfigs = new ProducerConfigsBuilder()
                .bootstrapServers(format("127.0.0.1:%s", broker.getPort()))
                .requestRequiredAcks(ProducerConfigsBuilder.RequestRequiredAcks.ackFromLeader)
                .producerType(ProducerConfigsBuilder.ProducerType.sync)
                .keySerializer(ByteArraySerializer.class)
                .valueSerializer(ByteArraySerializer.class)
                .batchSize(0)
                .build();

        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfigs(producerConfigs);
        producerDefinition.setTopic("foo");
        producerDefinition.setMessageSize(1024);
        producerDefinition.setMessagesToSend(10000);
        producerDefinition.setSendBlocking(true);

        ProducerDriver driver = new ProducerDriver(producerDefinition);
        driver.run();
    }

}