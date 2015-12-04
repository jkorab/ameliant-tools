package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ConsumerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ConsumerDefinition;
import com.ameliant.tools.kafkaperf.config.ProducerConfigsBuilder;
import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import com.ameliant.tools.kafkaperf.resources.EmbeddedKafkaBroker;
import com.ameliant.tools.kafkaperf.resources.EmbeddedZooKeeper;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;

/**
 * @author jkorab
 */
public class ConsumerDriverTest {

    @Rule
    public EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper();

    @Rule
    public EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker.Builder()
            .zookeeperConnect("127.0.0.1:" + zooKeeper.getPort())
            //.logFlushIntervalMessages(1)
            .build();

    @Test
    public void testReceive() throws InterruptedException {

        Map<String, Object> configs = new ConsumerConfigsBuilder()
                .groupId("bar")
                .bootstrapServersConfig("127.0.0.1:" + zooKeeper.getPort())
                .keyDeserializer(StringDeserializer.class)
                .valueDeserializer(StringDeserializer.class)
                .partitionAssignmentStrategy(ConsumerConfigsBuilder.PartitionAssignmentStrategy.range)
                .build();

        String topic = "foo";
        int messageCount = 10000;

        ConsumerDefinition consumerDefinition = new ConsumerDefinition();
        consumerDefinition.setConfigs(configs);
        consumerDefinition.setTopic(topic);
        // consumerDefinition.setConsumerGroupId("bar");
        // consumerDefinition.setZookeeperConnect("");
        consumerDefinition.setMessagesToReceive(messageCount);

        // TODO - send in background
        CountDownLatch latch = new CountDownLatch(2);

        // fill up the topic
        ProducerDriver producerDriver = createProducerDriver(latch, topic, messageCount);
        producerDriver.run();

        ConsumerDriver consumerDriver = new ConsumerDriver(consumerDefinition, latch);
        consumerDriver.run();

        latch.await(); // not really needed here
        // TODO what's the impact of sharing a connection?
    }

    public ProducerDriver createProducerDriver(CountDownLatch latch, String topic, int messagesToSend) {
        Map<String, Object> producerConfigs = new ProducerConfigsBuilder()
                .bootstrapServers(format("127.0.0.1:%s", broker.getPort()))
                .requestRequiredAcks(ProducerConfigsBuilder.RequestRequiredAcks.ackFromLeader)
                .producerType(ProducerConfigsBuilder.ProducerType.sync)
                .keySerializerClass(ByteArraySerializer.class)
                .valueSerializerClass(ByteArraySerializer.class)
                .batchSize(0)
                .build();

        ProducerDefinition producerDefinition = new ProducerDefinition();
        producerDefinition.setConfigs(producerConfigs);
        producerDefinition.setTopic(topic);
        producerDefinition.setMessageSize(1024);
        producerDefinition.setMessagesToSend(messagesToSend);
        producerDefinition.setSendBlocking(true);

        return new ProducerDriver(producerDefinition, latch);
    }

}