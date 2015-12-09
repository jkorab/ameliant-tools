package com.ameliant.tools.kafkaperf.drivers;

import com.ameliant.tools.kafkaperf.config.ConsumerDefinition;
import com.ameliant.tools.kafkaperf.config.ProducerDefinition;
import com.ameliant.tools.kafkaperf.config.TestProfileDefinition;
import com.ameliant.tools.kafkaperf.drivers.ConsumerDriver;
import com.ameliant.tools.kafkaperf.drivers.Driver;
import com.ameliant.tools.kafkaperf.drivers.ProducerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
        // TODO implement cascading configs override
        ArrayList<Driver> drivers = new ArrayList<>();

        int driverCount = 0;
        List<ProducerDefinition> producerDefinitions = testProfileDefinition.getProducers();
        driverCount += producerDefinitions.size();

        List<ConsumerDefinition> consumerDefinitions = testProfileDefinition.getConsumers();
        driverCount += consumerDefinitions.size();

        log.debug("Latching {} drivers", driverCount);
        CountDownLatch latch = new CountDownLatch(driverCount);

        drivers.addAll(producerDefinitions.stream()
                .map(producerDefinition -> new ProducerDriver(producerDefinition, latch))
                .collect(Collectors.toList()));

        drivers.addAll(consumerDefinitions.stream()
                .map(consumerDefinition -> new ConsumerDriver(consumerDefinition, latch))
                .collect(Collectors.toList()));

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
    }
}
