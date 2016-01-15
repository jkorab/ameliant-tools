package com.ameliant.tools.kafkaconsumer;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import static org.jooq.lambda.tuple.Tuple.tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * @author jkorab
 */
public class KafkaMessageListenerContainer<K, V> implements Runnable, AutoCloseable {

    public static final int DEFAULT_POLL_TIMEOUT = 100;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Properties kafkaConfig;
    private final String groupId;
    private final OffsetStore offsetStore;
    private final String topic;
    private final BiConsumer<K, V> messageListener;
    private BiConsumer<Tuple2<K, V>, Exception> exceptionHandler; // TODO add default

    public KafkaMessageListenerContainer(Properties kafkaConfig,
                                         OffsetStore offsetStore,
                                         String topic,
                                         BiConsumer<K, V> messageListener) {
        Validate.notNull(kafkaConfig, "kafkaConfig is null");
        this.kafkaConfig = kafkaConfig;

        this.groupId = (String) kafkaConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
        Validate.notEmpty(groupId, "groupId is empty");

        this.offsetStore = offsetStore;
        this.topic = topic;
        this.messageListener = messageListener;
    }

    private KafkaConsumer createConsumer(Properties kafkaConfig) {
        log.info("Disabling auto-commit for {}", topic);
        kafkaConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        return new KafkaConsumer(kafkaConfig);
    }

    public void setExceptionHandler(BiConsumer<Tuple2<K, V>, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final CountDownLatch workerShutdownLatch = new CountDownLatch(1);

    public void start() {
        executorService.submit(this);
    }

    @Override
    public void run() {
        try (Consumer consumer = createConsumer(kafkaConfig)) {
            final String groupId = "something";
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                    // TODO could be processing a message!
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                    topicPartitions.stream()
                            .forEach(topicPartition -> {
                                long lastConsumed = offsetStore.getLastConsumed(topicPartition, groupId);
                                log.debug("Seeking {}:{}:{} for {}",
                                        topicPartition.topic(), topicPartition.partition(), lastConsumed, groupId);
                                consumer.seek(topicPartition, lastConsumed);
                            });
                }
            }); // can subscribe to multiple topics

            pollingLoop(consumer, groupId);
        }
    }

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private void pollingLoop(Consumer consumer, String groupId) {
        POLLING_LOOP: while (!shuttingDown.get()) {
            // committed position is the last offset that was saved securely
            ConsumerRecords consumerRecords = consumer.poll(DEFAULT_POLL_TIMEOUT);
            Iterable<ConsumerRecord<K,V>> records = consumerRecords.records(topic);
            for( ConsumerRecord<K,V> consumerRecord : records) {
                if (shuttingDown.get()) {
                    break POLLING_LOOP;
                }
                String recordTopic = consumerRecord.topic();
                int recordPartition = consumerRecord.partition();
                TopicPartition topicPartition = new TopicPartition(recordTopic, recordPartition);

                K key = consumerRecord.key();
                V value = consumerRecord.value();
                try {
                    // TODO introduce some sort of idempotent consumption here
                    // you could end up in a situation where just after polling, the partitions have been reallocated
                    // and another node picks up the message
                    messageListener.accept(key, value);
                    offsetStore.markConsumed(topicPartition, groupId, consumerRecord.offset());
                } catch (Exception ex) {
                    try {
                        exceptionHandler.accept(tuple(key, value), ex);
                    } catch (Exception exh) {
                        // TODO add maxRedeliveryAttempts, redeliveryDelay
                        log.error("Exception caught from dead-letter handler: {}", ex);
                        log.info("Rewinding offset before re-polling");
                        consumer.seek(topicPartition, offsetStore.getLastConsumed(topicPartition, groupId));
                        break; // interrupt the consumption of the polled messages
                    }
                }
            }
            log.debug("Worker has shut down.");
            workerShutdownLatch.countDown();
        }
    }

    @Override
    public void close() throws Exception {
        shuttingDown.set(true);
        if (!workerShutdownLatch.await(10, TimeUnit.SECONDS)) {
            log.warn("Timeout waiting to shut down worker thread");
        }
        executorService.shutdownNow();
    }
}
