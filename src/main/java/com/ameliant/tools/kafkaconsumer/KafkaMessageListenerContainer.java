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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;
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

    private BiConsumer<Tuple2<K, V>, Exception> exceptionHandler =
            (tuple, ex) -> log.error("Caught exception: {}", ex);

    /** Convenience class for fluent instantiation */
    public static class Builder<K, V> {
        private Properties kafkaConfig;
        private OffsetStore offsetStore;
        private String topic;
        private BiConsumer<K, V> messageListener;
        private BiConsumer<Tuple2<K, V>, Exception> exceptionHandler;

        public Builder kafkaConfig(Properties kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public Builder offsetStore(OffsetStore offsetStore) {
            this.offsetStore = offsetStore;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder messageListener(BiConsumer<K, V> messageListener) {
            this.messageListener = messageListener;
            return this;
        }

        public Builder exceptionHandler(BiConsumer<Tuple2<K, V>, Exception> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public KafkaMessageListenerContainer build() {
            KafkaMessageListenerContainer<K, V> container =
                    new KafkaMessageListenerContainer<>(kafkaConfig, offsetStore, topic, messageListener);
            if (exceptionHandler != null) {
                container.setExceptionHandler(exceptionHandler);
            }
            return container;
        }
    }

    public KafkaMessageListenerContainer(Properties kafkaConfig,
                                         OffsetStore offsetStore,
                                         String topic,
                                         BiConsumer<K, V> messageListener) {
        Validate.notNull(kafkaConfig, "kafkaConfig is null");
        this.kafkaConfig = kafkaConfig;

        this.groupId = (String) kafkaConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
        Validate.notEmpty(groupId, "groupId is empty");

        Validate.notNull(offsetStore, "offsetStore is null");
        this.offsetStore = offsetStore;

        Validate.notEmpty(topic, "topic is empty");
        this.topic = topic;

        Validate.notNull(messageListener, "messageListener is null");
        this.messageListener = messageListener;
    }

    public void setExceptionHandler(BiConsumer<Tuple2<K, V>, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final CountDownLatch workerShutdownLatch = new CountDownLatch(1);
    public void init() {
        executorService.submit(this);
    }

    final ConcurrentSkipListSet assignedTopicPartitions = new ConcurrentSkipListSet();

    @Override
    public void run() {
        try (Consumer consumer = createConsumer(kafkaConfig)) {
            final String groupId = "something";
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
                    /*
                     TODO what do you do with the messages that you have just polled? Discard remainder?
                     This looks like a corner case that cannot be satisfactorily addressed using idempotent consumption
                     while maintaining message ordering.
                     If you have two consumers, c1 and c2 processing from a partition [m0,m1,m2,m3], and c2
                     gets the partition, and re-polls the same messages, you get out of order processing.
                     (e.g. c1:m0,m2; c2:m1,m3).
                     */
                     // keep track of TopicPartitions that the consumer is currently assigned to
                    assignedTopicPartitions.removeAll(topicPartitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                    topicPartitions.stream()
                            .forEach(topicPartition -> {
                                Optional<Long> lastConsumed = offsetStore.getLastConsumed(topicPartition, groupId);
                                lastConsumed.ifPresent(cursorPosition -> {
                                    log.debug("Seeking {}:{}:{} for {}",
                                            topicPartition.topic(), topicPartition.partition(), cursorPosition, groupId);
                                    consumer.seek(topicPartition, cursorPosition);
                                });
                                // otherwise will revert to the configured cursor positioning strategy
                                assignedTopicPartitions.add(topicPartition);
                            });
                }
            }); // can subscribe to multiple topics

            pollingLoop(consumer, groupId);
        }
    }

    private KafkaConsumer createConsumer(Properties kafkaConfig) {
        log.info("Disabling auto-commit for {}", topic);
        kafkaConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        return new KafkaConsumer(kafkaConfig);
    }

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private void pollingLoop(Consumer consumer, String groupId) {
        POLLING_LOOP: while (!shuttingDown.get()) {
            // committed position is the last offset that was saved securely
            ConsumerRecords consumerRecords = consumer.poll(DEFAULT_POLL_TIMEOUT);
            // TODO why does polling work in increments if commit has not been called?
            Iterable<ConsumerRecord<K,V>> records = consumerRecords.records(topic);

            RECORD_PROCESSING: for( ConsumerRecord<K,V> consumerRecord : records) {
                if (shuttingDown.get()) {
                    break POLLING_LOOP;
                }
                String topic = consumerRecord.topic();
                int partition = consumerRecord.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);

                long offset = consumerRecord.offset();
                if (!assignedTopicPartitions.contains(topicPartition)) {
                    // another consumer has been assigned to this partition since polling, skip this record
                    log.info("Discarding polled message as no longer assigned to partition {}:{}:{}", topic, partition, offset);
                    continue RECORD_PROCESSING;
                }

                K key = consumerRecord.key();
                V value = consumerRecord.value();
                try {
                    // TODO introduce some sort of idempotent consumption here
                    // you could end up in a situation where just after polling, the partitions have been reallocated
                    // and another node picks up the message

                    /*
                    An idempotent consumer by itself won't
                     */
                    messageListener.accept(key, value);

                    offsetStore.markConsumed(topicPartition, groupId, offset);
                    // doesn't matter if the system crashes at this point, as the offsetStore will be used to seek
                    // to offset at next startup
                    consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));
                } catch (Exception ex) {
                    try {
                        exceptionHandler.accept(tuple(key, value), ex);
                    } catch (Exception exh) {
                        // TODO potentially endless loop; add maxRedeliveryAttempts, redeliveryDelay
                        log.error("Exception caught from dead-letter handler: {}", ex);
                        log.info("Rewinding offset before re-polling");
                        consumer.seek(topicPartition, offsetStore.getLastConsumed(topicPartition, groupId).get());
                        break RECORD_PROCESSING; // interrupt the consumption of the already polled messages
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
