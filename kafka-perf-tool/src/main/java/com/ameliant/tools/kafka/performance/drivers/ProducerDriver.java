package com.ameliant.tools.kafka.performance.drivers;

import com.ameliant.tools.kafka.performance.config.PartitioningStrategy;
import com.ameliant.tools.kafka.performance.config.ProducerDefinition;
import com.ameliant.tools.kafka.performance.drivers.partitioning.KeyAllocationStrategy;
import com.ameliant.tools.kafka.performance.drivers.partitioning.RoundRobinPartitioner;
import com.ameliant.tools.kafka.performance.drivers.partitioning.StickyPartitioner;
import com.ameliant.tools.kafka.performance.util.FileLoader;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author jkorab
 */
public class ProducerDriver extends Driver {

    private final ProducerDefinition producerDefinition;
    private CountDownLatch completionLatch;

    public ProducerDriver(ProducerDefinition producerDefinition) {
        Validate.notNull(producerDefinition, "producerDefinition is null");
        this.producerDefinition = producerDefinition;
    }

    public ProducerDriver(ProducerDefinition producerDefinition, CountDownLatch completionLatch) {
        this(producerDefinition);
        this.completionLatch = completionLatch;
    }

    @Override
    public String toString() {
        return "ProducerDriver{" +
                "producerDefinition=" + producerDefinition +
                '}';
    }

    public void drive() {
        // The producer is thread safe and sharing a single producer instance across threads will generally be
        // faster than having multiple instances.
        // {@see http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html}
        Map<String, Object> kafkaConfig = producerDefinition.getKafkaConfig();
        applyConfigOverrides(kafkaConfig);

        try (KafkaProducer producer = new KafkaProducer(kafkaConfig)) {

            String message = generateMessage(producerDefinition.getMessageLocation(), producerDefinition.getMessageSize());
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            String topic = producerDefinition.getTopic();
            Validate.notEmpty(topic, "topic is empty");
            long messagesToSend = producerDefinition.getMessagesToSend();
            Validate.isTrue(messagesToSend > 0, "messagesToSend must be greater than 0");

            log.info("Producing {} messages to {}", messagesToSend, topic);

            KeyAllocationStrategy keyAllocationStrategy = new KeyAllocationStrategy(producerDefinition.getKeyAllocationStrategy());
            log.debug("KeyAllocationStrategy is {}", keyAllocationStrategy);

            for (int i = 0; i < messagesToSend; i++) {
                if (isShutdownRequested()) {
                    break;
                }

                // keys are used by the partitioning function to assign a partition
                byte[] key = keyAllocationStrategy.getKey(i);
                if (log.isTraceEnabled()) {
                    log.trace("Sending message {} with key {}", i, key);
                }
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, message.getBytes());

                if (producerDefinition.isSendBlocking()) {
                    Future<RecordMetadata> future = producer.send(record);
                    try {
                        // all sends are async, you need to get in order to block
                        traceOffset(future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // callbacks for records being sent to the same partition are guaranteed to execute in order
                    producer.send(record, (recordMetadata, exception) -> {
                        if (exception == null) {
                            traceOffset(recordMetadata);
                        } else {
                            throw new RuntimeException("Error sending to Kafka: " + exception.getMessage());
                        }
                    });
                }
                long sendDelay = producerDefinition.getSendDelay();
                if (sendDelay > 0) {
                    log.trace("Delaying send by {}", sendDelay);
                    Thread.sleep(sendDelay);
                }
            }

            stopWatch.stop();
            if (isShutdownRequested()) {
                log.info("Shutting down");
            } else {
                long runTime = stopWatch.getTime();
                log.info("Done. Producer finished sending {} msgs in {} ms", messagesToSend, runTime);

                double averageThroughput = (1000d / runTime) * messagesToSend;
                log.info("Average throughput: {} msg/s", averageThroughput);
            }

        } catch (InterruptedException e) {
            log.error("Producer interrupted.");
        } finally {
            log.debug("Producer closed");
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private void applyConfigOverrides(Map<String, Object> kafkaConfig) {
        log.debug("Applying config overrides");
        PartitioningStrategy partitioningStrategy = producerDefinition.getPartitioningStrategy();
        applyOverride(kafkaConfig, ProducerConfig.PARTITIONER_CLASS_CONFIG, getPartitionerClassName(partitioningStrategy));
    }

    private void applyOverride(Map<String, Object> kafkaConfig, String property, Optional<String> optionalValue) {
        if (optionalValue.isPresent()) {
            String value = optionalValue.get();
            kafkaConfig.put(property, value);
            log.debug("Overriding {} to {}", property, value);
        } else {
            log.debug("No override applied for {}", property);
        }
    }

    private Optional<String> getPartitionerClassName(PartitioningStrategy partitioningStrategy) {
        if ((partitioningStrategy == null) || (partitioningStrategy == PartitioningStrategy.none)) {
            return Optional.empty();
        } else if (partitioningStrategy.equals(PartitioningStrategy.roundRobin)) {
            return Optional.of(RoundRobinPartitioner.class.getCanonicalName());
        } else if (partitioningStrategy.equals(PartitioningStrategy.sticky)) {
            return Optional.of(StickyPartitioner.class.getCanonicalName());
        } else {
            throw new IllegalArgumentException("Unrecognised partitioningStrategy: " + partitioningStrategy);
        }
    }

    private String generateMessage(String messageLocation, int messageSize) {
        if (messageLocation == null) {
            Validate.isTrue(messageSize > 0, "messageSize must be greater than 0");
            return RandomStringUtils.randomAlphanumeric(messageSize);
        } else {
            log.debug("Loading payload from {}", messageLocation);
            return new FileLoader().loadFileAsString(messageLocation);
        }
    }

    private void traceOffset(RecordMetadata recordMetadata) {
        assert (recordMetadata != null);
        if (log.isTraceEnabled()) {
            log.trace("The [topic:partition:offset] of the record sent was [{}:{}:{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
    }


}
