package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author jkorab
 */
public class ProducerDriver extends Driver {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ProducerDefinition producerDefinition;
    private CountDownLatch latch;

    ProducerDriver(ProducerDefinition producerDefinition) {
        Validate.notNull(producerDefinition, "producerDefinition is null");
        this.producerDefinition = producerDefinition;
    }

    ProducerDriver(ProducerDefinition producerDefinition, CountDownLatch latch) {
        this(producerDefinition);
        this.latch = latch;
    }

    @Override
    public String toString() {
        return "ProducerDriver{" +
                "producerDefinition=" + producerDefinition +
                '}';
    }

    public void run() {
        // The producer is thread safe and sharing a single producer instance across threads will generally be
        // faster than having multiple instances.
        // {@see http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html}
        try (KafkaProducer producer = new KafkaProducer(producerDefinition.getKafkaConfig())) {

            String message = generateMessage(producerDefinition.getMessageSize());
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            String topic = producerDefinition.getTopic();
            Validate.notEmpty(topic, "topic is empty");
            long messagesToSend = producerDefinition.getMessagesToSend();
            Validate.isTrue(messagesToSend > 0, "messagesToSend must be greater than 0");

            log.info("Producing {} messages to {}", messagesToSend, topic);

            int uniqueKeyCount = producerDefinition.getUniqueKeyCount();
            List<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < uniqueKeyCount; i++) {
                // FIXME this allocates into different partitions on each run, tie into #1
                keys.add(RandomStringUtils.randomAlphabetic(8).getBytes());
            }

            for (int i = 0; i < messagesToSend; i++) {
                if (isShuttingDown()) {
                    break;
                }

                // we would use a key (optional second arg) if we were using a partitioning function
                byte[] key = keys.get(i % uniqueKeyCount);
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
            }

            stopWatch.stop();
            if (isShuttingDown()) {
                log.info("Shutting down");
            } else {
                long runTime = stopWatch.getTime();
                log.info("Done. Producer finished sending {} msgs in {} ms", messagesToSend, runTime);

                double averageThroughput = (1000d / runTime) * messagesToSend;
                log.info("Average throughput: {} msg/s", averageThroughput);
            }

        } finally {
            log.debug("Producer closed");
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private String generateMessage(int messageSize) {
        Validate.isTrue(messageSize > 0, "messageSize must be greater than 0");
        return RandomStringUtils.randomAlphanumeric(messageSize);
    }

    private void traceOffset(RecordMetadata recordMetadata) {
        assert (recordMetadata != null);
        if (log.isTraceEnabled()) {
            log.trace("The partition:offset of the record sent was {}:{}",
                    recordMetadata.partition(), recordMetadata.offset());
        }
    }
}
