package com.ameliant.tools.kafka.perftool.drivers;

import com.ameliant.tools.kafka.perftool.config.ConsumerDefinition;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * @author jkorab
 */
public class ConsumerDriver extends Driver {

    private final ConsumerDefinition consumerDefinition;
    private CountDownLatch completionLatch;
    private long recordsFetched = 0;
    private ConsumerRebalanceListener consumerRebalanceListener;

    public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
        this.consumerRebalanceListener = consumerRebalanceListener;
    }

    ConsumerDriver(ConsumerDefinition consumerDefinition) {
        Validate.notNull(consumerDefinition, "consumerDefinition is null");
        this.consumerDefinition = consumerDefinition;
    }

    ConsumerDriver(ConsumerDefinition consumerDefinition, CountDownLatch completionLatch) {
        this(consumerDefinition);
        this.completionLatch = completionLatch;
    }

    @Override
    public void drive() {
        // A Consumer is not thread-safe
        // {@see http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html}
        // {@see http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#multithreaded}
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerDefinition.getKafkaConfig())) {

            String topic = consumerDefinition.getTopic();
            log.info("Subscribing to {}", topic);
            if (consumerRebalanceListener == null) {
                consumer.subscribe(Collections.singletonList(topic));
            } else {
                consumer.subscribe(Collections.singletonList(topic), consumerRebalanceListener);
            }

            long messagesToReceive = consumerDefinition.getMessagesToReceive();
            log.info("Expecting {} messages", messagesToReceive);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            do {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(consumerDefinition.getPollTimeout());
                if (records == null) {
                    throw new IllegalStateException("null ConsumerRecords polled");
                } else {
                    if (records.count() == 0) {
                        try {
                            log.info("No records fetched, pausing");
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("Fetched {} records", records.count());
                        }
                        for (ConsumerRecord<byte[], byte[]> record : records) {
                            recordsFetched += 1;
                            applyReceiveDelay();
                            if (recordsFetched % consumerDefinition.getReportReceivedEvery() == 0) {
                                log.info("Received {} messages", recordsFetched);
                            }
                        }
                    }
                }

                if (isShutdownRequested()) {
                    break;
                }
                stopWatch.split();
            } while ((recordsFetched < messagesToReceive)
                    && (stopWatch.getSplitTime() < consumerDefinition.getTestRunTimeout()));

            stopWatch.stop();
            if (isShutdownRequested()) {
                log.info("Shutting down");
            } else {
                long runTime = stopWatch.getTime();
                log.info("Done. Consumer received {} msgs in {} ms", messagesToReceive, runTime);

                double averageThroughput = (1000d / runTime) * messagesToReceive;
                log.info("Average throughput: {} msg/s", averageThroughput);
            }

        } finally {
            log.debug("Consumer closed");
            if (completionLatch != null) {
                completionLatch.countDown();
            }
        }
    }

    private void applyReceiveDelay() {
        int receiveDelay = consumerDefinition.getReceiveDelay();
        if (receiveDelay > 0) {
            try {
                Thread.sleep(receiveDelay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public long getMessagesReceived() {
        return recordsFetched;
    }

    public long getMessagesExpected() {
        return consumerDefinition.getMessagesToReceive();
    }
}
