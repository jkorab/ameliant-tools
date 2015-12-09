package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ConsumerDefinition;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

/**
 * @author jkorab
 */
public class ConsumerDriver implements Runnable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ConsumerDefinition consumerDefinition;
    private CountDownLatch latch;

    ConsumerDriver(ConsumerDefinition consumerDefinition) {
        Validate.notNull(consumerDefinition, "consumerDefinition is null");
        this.consumerDefinition = consumerDefinition;
    }

    ConsumerDriver(ConsumerDefinition consumerDefinition, CountDownLatch latch) {
        this(consumerDefinition);
        this.latch = latch;
    }

    @Override
    public void run() {
        Map<String, Object> configs = consumerDefinition.getConfigs();
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs);

        String topic = consumerDefinition.getTopic();
        log.info("Subscribing to {}", topic);
        consumer.subscribe(Collections.singletonList(topic));

        long recordsFetched = 0;
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
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        recordsFetched += 1;
                        if (recordsFetched % consumerDefinition.getReportReceivedEvery() == 0) {
                            log.info("Received {} messages", recordsFetched);
                        }
                    }
                }
            }

            stopWatch.split();
        } while ((recordsFetched < messagesToReceive)
                && (stopWatch.getSplitTime() < consumerDefinition.getTestRunTimeout()));

        stopWatch.stop();
        long runTime = stopWatch.getTime();
        log.info("Done. Consumer received {} msgs in {} ms", messagesToReceive, runTime);

        double averageThroughput = (1000d / runTime) * messagesToReceive;
        log.info("Average throughput: {} msg/s", averageThroughput);

        consumer.close();
        if (latch != null) {
            latch.countDown();
        }
    }
}
