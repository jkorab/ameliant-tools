package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author jkorab
 */
public class TestProfileRunner {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TestProfileDefinition testProfileDefinition;

    public TestProfileRunner(TestProfileDefinition testProfileDefinition) {
        this.testProfileDefinition = testProfileDefinition;
    }

    public void run() {
        ArrayList<Driver> drivers = new ArrayList<>();

        int driverCount = 0;
        Map<String, Object> globalConfig = testProfileDefinition.getConfig();

        ProducersDefinition producersDefinition = testProfileDefinition.getProducers();
        Map<String, Object> producersConfig = producersDefinition.getConfig();

        List<ProducerDefinition> producerDefinitions = (List<ProducerDefinition>) producersDefinition;
        driverCount += producerDefinitions.size();

        ConsumersDefinition consumersDefinition = testProfileDefinition.getConsumers();
        Map<String, Object> consumersConfig = consumersDefinition.getConfig();

        List<ConsumerDefinition> consumerDefinitions = consumersDefinition.getInstances();
        driverCount += consumerDefinitions.size();

        log.debug("Latching {} drivers", driverCount);
        CountDownLatch latch = new CountDownLatch(driverCount);

        drivers.addAll(producerDefinitions.stream()
                .map(producerDefinition -> new ProducerDriver(
                        producerDefinition, latch))
                .collect(Collectors.toList()));

        drivers.addAll(consumerDefinitions.stream()
                .map(consumerDefinition -> new ConsumerDriver(
                        consumerDefinition, latch))
                .collect(Collectors.toList()));

        if (testProfileDefinition.isConcurrent()) {
            ExecutorService executorService = Executors.newFixedThreadPool(driverCount);

            drivers.forEach(driver -> {
                log.debug("Submitting {}", driver);
                executorService.submit(driver);
            });

            try {
                if (!latch.await(testProfileDefinition.getMaxDuration(), TimeUnit.SECONDS)) {
                    log.info("Shutting down gracefully");
                    drivers.forEach(driver -> driver.flagShutdown());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            executorService.shutdown();
        } else {
            drivers.forEach(driver -> {
                log.debug("Running {}", driver);
                driver.run();
            });
        }
    }
}
