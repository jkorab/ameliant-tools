package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ConsumerDefinition;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.time.StopWatch;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs); // FIXME pausing here

        String topic = consumerDefinition.getTopic();
        log.info("Subscribing to {}", topic);
        consumer.subscribe(topic);

        long recordsFetched = 0;
        long messagesToReceive = consumerDefinition.getMessagesToReceive();
        log.info("Expecting {} messages", messagesToReceive);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        do {
            Map<String, ConsumerRecords<byte[], byte[]>> records = consumer.poll(consumerDefinition.getPollTimeout());
            if (records == null) {
                log.info("null records polled");
            } else {
                recordsFetched += records.entrySet().stream()
                        .map(action -> {
                            List<ConsumerRecord<byte[], byte[]>> records1 = action.getValue().records();
                            return records1.size();
                        }).reduce(0, (total, recordCount) -> total + recordCount);
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
