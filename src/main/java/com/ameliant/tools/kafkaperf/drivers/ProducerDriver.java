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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author jkorab
 */
public class ProducerDriver implements Runnable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ProducerDefinition producerDefinition;

    ProducerDriver(ProducerDefinition producerDefinition) {
        Validate.notNull(producerDefinition, "producerDefinition is null");
        this.producerDefinition = producerDefinition;
    }

    public void run() {
        KafkaProducer producer = new KafkaProducer(producerDefinition.getConfigs());

        String message = generateMessage(producerDefinition.getMessageSize());
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String topic = producerDefinition.getTopic();
        Validate.notEmpty(topic, "topic is empty");
        int messagesToSend = producerDefinition.getMessagesToSend();
        Validate.isTrue(messagesToSend > 0, "messagesToSend must be greater than 0");

        log.info("Producing {} messages to {}", messagesToSend, topic);
        for (int i = 0; i < messagesToSend; i++) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, message.getBytes());

            if (producerDefinition.isSendBlocking()) {
                Future<RecordMetadata> future = producer.send(record);
                try {
                    // all sends are async, you need to get in order to block
                    dumpOffset(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // callbacks for records being sent to the same partition are guaranteed to execute in order
                producer.send(record, (recordMetadata, exception) -> {
                    if (exception == null) {
                        dumpOffset(recordMetadata);
                    } else {
                        log.error("Error sending to Kafka: {}", exception);
                    }
                });
            }
        }

        stopWatch.stop();
        long runTime = stopWatch.getTime();
        log.info("Done. Producer finished sending {} msgs in {} ms", messagesToSend, runTime);

        double averageThroughput = (1000d / runTime) * messagesToSend;
        log.info("Average throughput: {} msg/s", averageThroughput);

        producer.close();
    }

    private String generateMessage(int messageSize) {
        Validate.isTrue(messageSize > 0, "messageSize must be greater than 0");
        return RandomStringUtils.randomAlphanumeric(messageSize);
    }

    private void dumpOffset(RecordMetadata recordMetadata) {
        if (!log.isDebugEnabled()) return;
        Validate.notNull(recordMetadata, "recordMetadata is null");
        log.debug("The partition:offset of the record sent was {}:{}",
               recordMetadata.partition(), recordMetadata.offset());
    }
}
