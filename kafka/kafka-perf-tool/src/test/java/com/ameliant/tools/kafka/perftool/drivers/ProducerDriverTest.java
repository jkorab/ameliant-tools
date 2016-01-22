package com.ameliant.tools.kafka.perftool.drivers;

import static java.lang.String.format;
import com.ameliant.tools.kafka.testdsl.config.ProducerConfigsBuilder;
import com.ameliant.tools.kafka.perftool.config.ProducerDefinition;
import com.ameliant.tools.kafka.testdsl.EmbeddedKafkaBroker;
import com.ameliant.tools.zookeeper.testdsl.EmbeddedZooKeeper;
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
    public EmbeddedKafkaBroker broker = EmbeddedKafkaBroker.builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            //.logFlushIntervalMessages(1)
            .build();

    @Test
    public void testSend() {
        Map<String, Object> producerConfigs = getProducerConfigs();

        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfig(producerConfigs);
        producerDefinition.setTopic("foo");
        producerDefinition.setMessageSize(1024);
        producerDefinition.setMessagesToSend(1000);
        producerDefinition.setSendBlocking(true);

        ProducerDriver driver = new ProducerDriver(producerDefinition);
        driver.run();
    }


    private Map<String, Object> getProducerConfigs() {
        return new ProducerConfigsBuilder()
                    .bootstrapServers(format("127.0.0.1:%s", broker.getPort()))
                    .requestRequiredAcks(ProducerConfigsBuilder.RequestRequiredAcks.ackFromLeader)
                    .producerType(ProducerConfigsBuilder.ProducerType.sync)
                    .keySerializer(ByteArraySerializer.class)
                    .valueSerializer(ByteArraySerializer.class)
                    .batchSize(0)
                    .build();
    }

}